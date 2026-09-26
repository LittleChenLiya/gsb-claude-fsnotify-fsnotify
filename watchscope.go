package fsnotify

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
)

// ErrScopeGap is sent on a [Scope]'s Errors channel when events for that
// scope were dropped because its Events buffer was full. The underlying
// watcher is never blocked by a slow scope; instead the scope is notified
// that it missed one or more events and should resync (for example by
// re-scanning the watched directory).
var ErrScopeGap = errors.New("fsnotify: scope event buffer full, events dropped")

// ScopeWatcher multiplexes a single [Watcher] over any number of
// independent [Scope] subscribers.
//
// Every scope gets its own Events and Errors channels and its own Close;
// closing one scope never affects any other scope. Paths are normalized
// (made absolute and cleaned) before being deduplicated, so two scopes
// watching the same directory share a single native watch, which is
// reference-counted: the native watch is removed only after the last scope
// using it is closed or removes it.
//
// Each scope has a bounded event buffer. A slow scope drops events instead
// of blocking the shared read loop, and is notified of the loss with
// [ErrScopeGap] on its own Errors channel.
//
// Errors from the underlying watcher are not associated with a single path,
// so they are broadcast to all active scopes. Errors from a scope's own Add
// call are returned to (and only to) that scope.
//
// A ScopeWatcher does not take ownership of the Watcher's lifetime: Close
// closes all scopes but does not close the underlying Watcher, so the
// original Watcher API remains fully usable.
type ScopeWatcher struct {
	w *Watcher

	done   chan struct{}
	fanout chan struct{} // Closed when the fan-in goroutine exits.

	mu     sync.Mutex
	paths  map[string]int // Normalized path → native watch reference count.
	scopes map[*Scope]struct{}
	closed bool
}

// NewScopeWatcher creates a ScopeWatcher on top of w and starts the shared
// fan-in loop that reads w.Events and w.Errors.
//
// While a ScopeWatcher is active the caller must not read from w.Events or
// w.Errors directly; all other Watcher methods (Add, Remove, WatchList,
// Close, ...) keep working as before.
func NewScopeWatcher(w *Watcher) *ScopeWatcher {
	sw := &ScopeWatcher{
		w:      w,
		done:   make(chan struct{}),
		fanout: make(chan struct{}),
		paths:  make(map[string]int),
		scopes: make(map[*Scope]struct{}),
	}
	go sw.run()
	return sw
}

// Watcher returns the underlying shared [Watcher].
func (sw *ScopeWatcher) Watcher() *Watcher { return sw.w }

// WatchList returns all paths currently watched by the underlying watcher.
// It is a thin wrapper around [Watcher.WatchList].
func (sw *ScopeWatcher) WatchList() []string { return sw.w.WatchList() }

// NewScope creates a new subscription scope with an Events channel buffered
// to size events. A size of 0 gives an unbuffered channel, which drops any
// event the scope isn't ready to receive (reported via [ErrScopeGap]).
func (sw *ScopeWatcher) NewScope(size uint) *Scope {
	s := &Scope{
		sw:     sw,
		Events: make(chan Event, size),
		Errors: make(chan error, 1),
		paths:  make(map[string]struct{}),
	}
	sw.mu.Lock()
	if !sw.closed {
		sw.scopes[s] = struct{}{}
	}
	sw.mu.Unlock()
	return s
}

// Close closes all scopes. It does not close the underlying Watcher. It is
// safe to call Close more than once.
func (sw *ScopeWatcher) Close() error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return nil
	}
	sw.closed = true
	scopes := make([]*Scope, 0, len(sw.scopes))
	for s := range sw.scopes {
		scopes = append(scopes, s)
	}
	sw.mu.Unlock()

	for _, s := range scopes {
		s.Close()
	}
	close(sw.done)
	<-sw.fanout
	return nil
}

// run is the shared fan-in loop. It never blocks on a scope: a scope that
// can't keep up drops events and gets a gap notification instead.
func (sw *ScopeWatcher) run() {
	defer close(sw.fanout)
	for {
		select {
		case <-sw.done:
			return
		case e, ok := <-sw.w.Events:
			if !ok {
				sw.closeAll()
				return
			}
			sw.dispatchEvent(e)
		case err, ok := <-sw.w.Errors:
			if !ok {
				sw.closeAll()
				return
			}
			sw.dispatchError(err)
		}
	}
}

// closeAll closes every scope; called when the underlying watcher's channels
// are closed (i.e. Watcher.Close was called).
func (sw *ScopeWatcher) closeAll() {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return
	}
	sw.closed = true
	scopes := make([]*Scope, 0, len(sw.scopes))
	for s := range sw.scopes {
		scopes = append(scopes, s)
	}
	sw.mu.Unlock()

	for _, s := range scopes {
		s.Close()
	}
}

func (sw *ScopeWatcher) dispatchEvent(e Event) {
	sw.mu.Lock()
	scopes := make([]*Scope, 0, len(sw.scopes))
	for s := range sw.scopes {
		scopes = append(scopes, s)
	}
	sw.mu.Unlock()

	for _, s := range scopes {
		s.send(e)
	}
}

// dispatchError broadcasts a watcher-level error to all scopes. Errors from
// the underlying watcher carry no reliable path association, so they are
// treated as global.
func (sw *ScopeWatcher) dispatchError(err error) {
	sw.mu.Lock()
	scopes := make([]*Scope, 0, len(sw.scopes))
	for s := range sw.scopes {
		scopes = append(scopes, s)
	}
	sw.mu.Unlock()

	for _, s := range scopes {
		s.sendErr(err)
	}
}

// add increments the reference count for path, creating the native watch on
// the first reference. If the native Add fails the count is rolled back and
// the error is returned to the calling scope only.
func (sw *ScopeWatcher) add(path string) error {
	sw.mu.Lock()
	if sw.closed {
		sw.mu.Unlock()
		return ErrClosed
	}
	refs := sw.paths[path]
	sw.paths[path] = refs + 1
	sw.mu.Unlock()

	if refs > 0 {
		return nil
	}
	if err := sw.w.Add(path); err != nil {
		sw.mu.Lock()
		delete(sw.paths, path)
		sw.mu.Unlock()
		return err
	}
	return nil
}

// remove decrements the reference count for path, removing the native watch
// when the last reference goes away.
func (sw *ScopeWatcher) remove(path string) error {
	sw.mu.Lock()
	refs := sw.paths[path]
	if refs <= 1 {
		delete(sw.paths, path)
	} else {
		sw.paths[path] = refs - 1
	}
	closed := sw.closed
	sw.mu.Unlock()

	if refs <= 1 && !closed {
		// The underlying watcher returns nil (not ErrNonExistentWatch) when
		// it's already closed, so this is safe during shutdown.
		return sw.w.Remove(path)
	}
	return nil
}

func (sw *ScopeWatcher) dropScope(s *Scope) {
	sw.mu.Lock()
	delete(sw.scopes, s)
	sw.mu.Unlock()
}

// normalizeScopePath cleans path and makes it absolute so that different
// spellings of the same directory ("dir", "dir/", "./dir") share a single
// native watch.
func normalizeScopePath(path string) (string, error) {
	if path == "" {
		return "", errors.New("fsnotify: can't watch empty path")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

// Scope is a single subscription on a [ScopeWatcher]. It has its own Events
// and Errors channels and its own Close; closing a Scope never affects other
// scopes on the same watcher.
//
// Events are delivered for the paths added with [Scope.Add] and everything
// inside them. The Events channel is bounded; when it's full events are
// dropped and [ErrScopeGap] is sent on the Errors channel.
type Scope struct {
	sw *ScopeWatcher

	// Events sends the filesystem change events for this scope's paths.
	// It is closed exactly once, when the scope is closed.
	Events chan Event

	// Errors sends errors relevant to this scope: watcher-level errors
	// (broadcast to all scopes) and [ErrScopeGap] when this scope dropped
	// events. It is closed exactly once, when the scope is closed.
	Errors chan error

	mu     sync.Mutex
	paths  map[string]struct{} // Normalized paths this scope watches.
	closed bool
}

// Add starts watching path within this scope. The path is normalized
// (absolute + cleaned) before being deduplicated, so adding the same
// directory through different spellings shares one native watch. Adding a
// path the scope already watches is a no-op.
//
// An error from the underlying watcher is returned to this scope only.
func (s *Scope) Add(path string) error {
	p, err := normalizeScopePath(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if _, ok := s.paths[p]; ok {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	if err := s.sw.add(p); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		// Closed concurrently with Add; undo.
		s.mu.Unlock()
		s.sw.remove(p)
		return ErrClosed
	}
	s.paths[p] = struct{}{}
	s.mu.Unlock()
	return nil
}

// Remove stops watching path within this scope. The native watch is removed
// only when no other scope watches the same path. Removing a path this scope
// doesn't watch returns [ErrNonExistentWatch].
func (s *Scope) Remove(path string) error {
	p, err := normalizeScopePath(path)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if _, ok := s.paths[p]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrNonExistentWatch, path)
	}
	delete(s.paths, p)
	s.mu.Unlock()

	return s.sw.remove(p)
}

// Paths returns the normalized paths this scope currently watches. The order
// is undefined.
func (s *Scope) Paths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	paths := make([]string, 0, len(s.paths))
	for p := range s.paths {
		paths = append(paths, p)
	}
	return paths
}

// Close removes all of this scope's watches (decrementing the shared
// reference counts) and closes the scope's Events and Errors channels,
// exactly once. It is safe to call Close more than once and concurrently.
func (s *Scope) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	paths := make([]string, 0, len(s.paths))
	for p := range s.paths {
		paths = append(paths, p)
	}
	s.paths = make(map[string]struct{})
	s.mu.Unlock()

	s.sw.dropScope(s)
	for _, p := range paths {
		s.sw.remove(p)
	}

	// Closed exactly once, after the scope can no longer receive events:
	// dropScope ran first, so the fan-in loop won't select this scope any
	// more, and send() re-checks s.closed under s.mu.
	close(s.Events)
	close(s.Errors)
	return nil
}

// send delivers an event to the scope if it watches a matching path. It
// never blocks: a full buffer drops the event and triggers a gap
// notification on the scope's Errors channel.
func (s *Scope) send(e Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if !s.watches(e.Name) {
		return
	}
	select {
	case s.Events <- e:
	default:
		// Buffer full: drop the event and notify the scope of the gap.
		// The send is non-blocking and the lock is held, so the channel
		// can't be closed concurrently.
		select {
		case s.Errors <- fmt.Errorf("%w: %s", ErrScopeGap, e.Name):
		default:
		}
	}
}

// watches reports if name is one of the scope's paths or inside one of them.
// The caller must hold s.mu.
func (s *Scope) watches(name string) bool {
	for p := range s.paths {
		if hasPathPrefix(name, p) {
			return true
		}
	}
	return false
}

// sendErr delivers an error to the scope without blocking. The Errors
// channel is buffered and errors are coalesced: if it's full the error is
// dropped rather than blocking the shared fan-in loop.
func (s *Scope) sendErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.Errors <- err:
	default:
	}
}
