package fsnotify

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// ErrEventGap is sent on a [Subscription]'s Errors channel when one or more
// events for that subscription were dropped because its bounded buffer was
// full. Only the slow subscription receives this notification; other
// subscriptions and the underlying Watcher are unaffected.
//
// It wraps [ErrEventOverflow], so errors.Is(err, ErrEventOverflow) reports
// true for it.
var ErrEventGap = fmt.Errorf("%w: subscription buffer full, events dropped", ErrEventOverflow)

// ScopeManager multiplexes a single [Watcher] over multiple independent
// scopes. Every scope subscribes to paths with its own event and error
// channels and its own Close, without affecting other scopes.
//
// Paths shared by several scopes result in a single native watch on the
// underlying Watcher, which is removed only after the last scope watching it
// is closed (reference counting). Paths are normalized with
// [filepath.Clean] before de-duplication, so "dir" and "dir/" share one
// native watch.
//
// Every subscription has a bounded buffer; a slow subscription drops events
// and is notified with [ErrEventGap] on its own Errors channel, and never
// blocks the underlying Watcher or other subscriptions.
//
// Errors from the underlying Watcher that mention a watched path are
// delivered only to the scopes watching that path; all other errors are
// broadcast to every scope.
//
// A ScopeManager takes ownership of the Watcher's Events and Errors
// channels: after calling [NewScopeManager] do not read from them directly.
// The Watcher itself is not closed by [ScopeManager.Close]; the caller keeps
// owning it, and the regular Watcher API (Add, Remove, Close, WatchList,
// ...) keeps working as before.
type ScopeManager struct {
	w *Watcher

	mu     sync.Mutex // Guards all of the following, and all Subscription state.
	paths  map[string]*scopePath
	closed bool
	done   chan struct{}
	wg     sync.WaitGroup
}

type scopePath struct {
	refs int
	subs map[*Subscription]struct{}
}

// Subscription is a single scope's view on a watched path. Events and Errors
// are closed exactly once, when the subscription is closed – either directly
// with [Subscription.Close], or when the ScopeManager or the underlying
// Watcher is closed.
type Subscription struct {
	// Events sends the filesystem change events for the subscribed path.
	Events chan Event

	// Errors sends errors scoped to this subscription, including
	// [ErrEventGap] when events were dropped.
	Errors chan error

	m    *ScopeManager
	path string // Cleaned path as added to the underlying Watcher.

	closed     bool
	gapPending bool
}

// NewScopeManager creates a ScopeManager on top of w. It takes over reading
// from w.Events and w.Errors; see the ScopeManager documentation.
func NewScopeManager(w *Watcher) *ScopeManager {
	m := &ScopeManager{
		w:     w,
		paths: make(map[string]*scopePath),
		done:  make(chan struct{}),
	}
	m.wg.Add(1)
	go m.run()
	return m
}

// Subscribe watches path for a new scope and returns its Subscription.
//
// buffer is the size of the subscription's bounded event buffer; values
// below 1 are treated as 1. When the buffer is full events for this
// subscription are dropped and [ErrEventGap] is sent on its Errors channel.
//
// If another scope already watches the same (normalized) path the existing
// native watch is reused and its reference count incremented; otherwise a
// new native watch is added with [Watcher.Add], and any error from it is
// returned to the caller of Subscribe only.
func (m *ScopeManager) Subscribe(path string, buffer uint) (*Subscription, error) {
	if buffer < 1 {
		buffer = 1
	}
	p := filepath.Clean(path)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	sp, ok := m.paths[p]
	if !ok {
		if err := m.w.Add(p); err != nil {
			return nil, err
		}
		sp = &scopePath{subs: make(map[*Subscription]struct{})}
		m.paths[p] = sp
	}
	s := &Subscription{
		Events: make(chan Event, buffer),
		Errors: make(chan error, 1),
		m:      m,
		path:   p,
	}
	sp.refs++
	sp.subs[s] = struct{}{}
	return s, nil
}

// Close closes all subscriptions and stops the manager. It is idempotent and
// returns nil on repeated calls. The underlying Watcher is not closed.
func (m *ScopeManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.done)
	m.closeAllLocked()
	m.mu.Unlock()

	m.wg.Wait()
	return nil
}

// Path returns the normalized path this subscription watches.
func (s *Subscription) Path() string { return s.path }

// Close removes this subscription and closes its Events and Errors channels
// exactly once; repeated calls are no-ops returning nil. When it was the last
// subscription for its path the native watch is removed from the underlying
// Watcher. Other scopes watching the same path are unaffected.
func (s *Subscription) Close() error {
	s.m.mu.Lock()
	defer s.m.mu.Unlock()
	s.closeLocked()
	return nil
}

// closeLocked closes the subscription; m.mu must be held.
func (s *Subscription) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	if sp, ok := s.m.paths[s.path]; ok {
		delete(sp.subs, s)
		sp.refs--
		if sp.refs <= 0 {
			delete(s.m.paths, s.path)
			// Ignore the error: the watch may already be gone (e.g. the
			// path was deleted, or the Watcher was closed).
			s.m.w.Remove(s.path)
		}
	}
	close(s.Events)
	close(s.Errors)
}

// closeAllLocked closes every subscription; m.mu must be held.
func (m *ScopeManager) closeAllLocked() {
	for _, sp := range m.paths {
		for s := range sp.subs {
			s.closeLocked()
		}
	}
	m.paths = make(map[string]*scopePath)
}

// run dispatches events and errors from the underlying Watcher to the
// subscriptions until the manager is closed or the Watcher's Events channel
// is closed.
func (m *ScopeManager) run() {
	defer m.wg.Done()
	errs := m.w.Errors
	for {
		select {
		case e, ok := <-m.w.Events:
			if !ok {
				// The underlying Watcher was closed: close all
				// subscriptions so their consumers terminate.
				m.mu.Lock()
				m.closed = true
				m.closeAllLocked()
				m.mu.Unlock()
				return
			}
			m.dispatchEvent(e)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			m.dispatchError(err)
		case <-m.done:
			return
		}
	}
}

func (m *ScopeManager) dispatchEvent(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for p, sp := range m.paths {
		if !hasPathPrefix(e.Name, p) &&
			(e.renamedFrom == "" || !hasPathPrefix(e.renamedFrom, p)) {
			continue
		}
		for s := range sp.subs {
			s.sendLocked(e)
		}
	}
}

func (m *ScopeManager) dispatchError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delivered := false
	for p, sp := range m.paths {
		if !strings.Contains(err.Error(), p) {
			continue
		}
		for s := range sp.subs {
			s.sendErrLocked(err)
		}
		delivered = true
	}
	if !delivered {
		// Not associated with any watched path: broadcast globally.
		for _, sp := range m.paths {
			for s := range sp.subs {
				s.sendErrLocked(err)
			}
		}
	}
}

// sendLocked delivers an event without ever blocking; m.mu must be held. If
// the buffer is full the event is dropped and the subscription is notified
// of the gap on its own Errors channel.
func (s *Subscription) sendLocked(e Event) {
	if s.gapPending {
		select {
		case s.Errors <- ErrEventGap:
			s.gapPending = false
		default:
		}
	}
	select {
	case s.Events <- e:
	default:
		s.gapPending = true
		select {
		case s.Errors <- ErrEventGap:
			s.gapPending = false
		default:
		}
	}
}

// sendErrLocked delivers an error without ever blocking; m.mu must be held.
// Errors are dropped if the subscription's error buffer is full.
func (s *Subscription) sendErrLocked(err error) {
	select {
	case s.Errors <- err:
	default:
	}
}
