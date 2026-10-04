package fsnotify

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrReplaceInProgress is returned by RootSetManager.Replace when a root set
// replacement is already in progress. Wait for the ongoing replacement to
// finish before starting another one.
var ErrReplaceInProgress = errors.New("fsnotify: root set replacement already in progress")

// RetirementError reports roots that could not be removed after a root set
// replacement was published.
//
// The new root set was published successfully; only the retirement of roots
// that are no longer needed failed. The listed roots are therefore no longer
// part of the published set, even though the underlying watcher reported an
// error removing them.
type RetirementError struct {
	// Roots is the ordered list of roots that failed to be removed.
	Roots []string

	// Errs holds the error returned for the matching entry in Roots.
	Errs []error
}

func (e *RetirementError) Error() string {
	if len(e.Roots) == 1 {
		return fmt.Sprintf("fsnotify: retiring root %q: %s", e.Roots[0], e.Errs[0])
	}
	var b strings.Builder
	b.WriteString("fsnotify: retiring roots failed:")
	for i, r := range e.Roots {
		b.WriteString("\n\t")
		b.WriteString(r)
		b.WriteString(": ")
		b.WriteString(e.Errs[i].Error())
	}
	return b.String()
}

// Unwrap returns the errors for the roots that failed to be removed.
func (e *RetirementError) Unwrap() []error { return e.Errs }

// rootSetDedupeWindow is the time window in which identical events (same path
// and operation) are considered duplicates and delivered only once.
//
// We deliberately do not try to identify which OS round an event originated
// from: backends differ wildly in how they batch and re-deliver notifications,
// so attributing events to an "OS round" would be unreliable. Instead,
// deduplication is content- and time-based, which is robust regardless of the
// underlying event source.
const rootSetDedupeWindow = 250 * time.Millisecond

type dedupeKey struct {
	name string
	op   Op
}

// RootSetManager owns a Watcher and manages a single published set of watched
// roots.
//
// Callers submit a complete new set of roots to RootSetManager.Replace. The
// manager then:
//
//  1. Adds every root that is new in the set, one by one. If any Add fails,
//     only the roots added during this attempt are removed again; the
//     previously published set stays in effect and keeps delivering events.
//  2. While preparation is in progress, events belonging to new roots are
//     staged, whereas events belonging to the currently published set are
//     still delivered immediately.
//  3. Once all new roots have been added, the new set is published and the
//     staged events are delivered. From that point on only events matching
//     the new set are delivered.
//  4. Roots that are no longer needed are removed after publication. Any
//     removal errors are reported as a RetirementError; they do not roll back
//     the published set.
//
// Nested roots are attributed to the longest matching root, and an event is
// delivered at most once: watches on nested roots can both report the same
// event, and such duplicates are suppressed.
//
// A RootSetManager should not be copied (e.g. pass it by pointer, rather than
// by value).
type RootSetManager struct {
	w *Watcher

	// Events delivers the filtered file system events.
	Events chan Event

	// Errors reports watcher errors. An ErrEventOverflow arriving while a
	// replacement is being prepared fails that replacement.
	Errors chan error

	mu        sync.Mutex
	published map[string]struct{} // currently published roots
	pending   map[string]struct{} // new roots while preparing
	preparing bool
	overflow  bool
	staged    []Event
	seen      map[dedupeKey]time.Time
	closed    bool

	closing chan struct{} // closed when Close is called
	done    chan struct{} // closed when the dispatcher exits
	flush   chan chan struct{}
}

// NewRootSetManager creates a new RootSetManager with an empty root set.
func NewRootSetManager() (*RootSetManager, error) {
	w, err := NewWatcher()
	if err != nil {
		return nil, err
	}
	return newRootSetManager(w), nil
}

func newRootSetManager(w *Watcher) *RootSetManager {
	m := &RootSetManager{
		w:         w,
		Events:    make(chan Event),
		Errors:    make(chan error),
		published: make(map[string]struct{}),
		seen:      make(map[dedupeKey]time.Time),
		closing:   make(chan struct{}),
		done:      make(chan struct{}),
		flush:     make(chan chan struct{}),
	}
	go m.dispatch()
	return m
}

// Replace atomically replaces the published root set with roots.
//
// All new roots are added first. Events arriving during preparation are
// staged if they belong to a new root, and delivered immediately if they
// belong to the currently published set. When every Add succeeded the new set
// is published, staged events are delivered once, and roots that are no longer
// needed are removed.
//
// If adding a root fails, or an ErrEventOverflow is reported during
// preparation, the replacement is aborted: roots added during this attempt are
// removed and the previously published set remains in effect.
//
// If removing an obsolete root fails after publication, Replace returns a
// RetirementError.
//
// Replace returns ErrClosed after RootSetManager.Close was called and
// ErrReplaceInProgress if called while another replacement is in progress.
func (m *RootSetManager) Replace(roots []string) error {
	pending := make(map[string]struct{})
	for _, r := range cleanRoots(roots) {
		pending[r] = struct{}{}
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if m.preparing {
		m.mu.Unlock()
		return ErrReplaceInProgress
	}
	toAdd := make([]string, 0)
	for r := range pending {
		if _, ok := m.published[r]; !ok {
			toAdd = append(toAdd, r)
		}
	}
	sort.Strings(toAdd)
	toRemove := make([]string, 0)
	for r := range m.published {
		if _, ok := pending[r]; !ok {
			toRemove = append(toRemove, r)
		}
	}
	sort.Strings(toRemove)

	m.preparing = true
	m.pending = pending
	m.overflow = false
	m.staged = nil
	m.mu.Unlock()

	added := make([]string, 0, len(toAdd))
	for _, r := range toAdd {
		if err := m.w.Add(r); err != nil {
			m.abort(added)
			return fmt.Errorf("fsnotify: adding root %q: %w", r, err)
		}
		added = append(added, r)

		m.mu.Lock()
		overflow := m.overflow
		m.mu.Unlock()
		if overflow {
			m.abort(added)
			return ErrEventOverflow
		}
	}

	m.mu.Lock()
	overflow := m.overflow
	m.mu.Unlock()
	if overflow {
		m.abort(added)
		return ErrEventOverflow
	}

	// Wait for the dispatcher to process every event and error that arrived
	// during preparation, so they are attributed to the state they arrived
	// in before we publish the new set.
	flushDone := make(chan struct{})
	select {
	case m.flush <- flushDone:
		<-flushDone
	case <-m.closing:
		m.abort(added)
		return ErrClosed
	}

	// Publish the new set before retiring the obsolete roots.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.abort(added)
		return ErrClosed
	}
	m.published = pending
	m.preparing = false
	m.pending = nil
	staged := m.staged
	m.staged = nil
	m.mu.Unlock()

	for _, ev := range staged {
		select {
		case m.Events <- ev:
		case <-m.closing:
			return ErrClosed
		}
	}

	var retirement *RetirementError
	for _, r := range toRemove {
		if err := m.w.Remove(r); err != nil {
			if retirement == nil {
				retirement = &RetirementError{}
			}
			retirement.Roots = append(retirement.Roots, r)
			retirement.Errs = append(retirement.Errs, err)
		}
	}
	if retirement != nil {
		return retirement
	}
	return nil
}

// Close closes the underlying watcher and both event channels.
func (m *RootSetManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	close(m.closing)
	m.mu.Unlock()

	err := m.w.Close()
	<-m.done
	close(m.Events)
	close(m.Errors)
	return err
}

// Roots returns a sorted copy of the currently published roots.
func (m *RootSetManager) Roots() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.published))
	for r := range m.published {
		out = append(out, r)
	}
	sort.Strings(out)
	return out
}

func (m *RootSetManager) abort(added []string) {
	m.mu.Lock()
	m.preparing = false
	m.pending = nil
	m.staged = nil
	m.mu.Unlock()
	for _, r := range added {
		_ = m.w.Remove(r)
	}
}

func (m *RootSetManager) dispatch() {
	defer close(m.done)
	for {
		select {
		case ev, ok := <-m.w.Events:
			if !ok {
				return
			}
			m.handleEvent(ev)
		case err, ok := <-m.w.Errors:
			if !ok {
				return
			}
			m.handleError(err)
		case done := <-m.flush:
			// Drain every event and error queued during preparation so they
			// are attributed to the state they arrived in, before Replace
			// publishes the new set.
			m.drain()
			close(done)
		}
	}
}

// drain processes all events and errors currently queued by the watcher.
func (m *RootSetManager) drain() {
	for {
		select {
		case ev := <-m.w.Events:
			m.handleEvent(ev)
		case err := <-m.w.Errors:
			m.handleError(err)
		default:
			return
		}
	}
}

func (m *RootSetManager) handleEvent(ev Event) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}

	if m.preparing {
		// Attribute the event to the longest root in the pending set.
		if root, ok := longestRoot(ev.Name, m.pending); ok {
			if _, retained := m.published[root]; !retained {
				// Brand new root: stage until the set is published.
				if !m.dedupeLocked(ev) {
					m.staged = append(m.staged, ev)
				}
				m.mu.Unlock()
				return
			}
		}
		// Event on a root that is still published: deliver immediately even
		// while preparation is in progress.
		if _, ok := longestRoot(ev.Name, m.published); ok {
			if !m.dedupeLocked(ev) {
				m.mu.Unlock()
				m.deliver(ev)
				return
			}
		}
		m.mu.Unlock()
		return
	}

	// Published state: deliver only events matching the current set.
	if _, ok := longestRoot(ev.Name, m.published); !ok {
		m.mu.Unlock()
		return
	}
	if m.dedupeLocked(ev) {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	m.deliver(ev)
}

func (m *RootSetManager) handleError(err error) {
	m.mu.Lock()
	if m.preparing && errors.Is(err, ErrEventOverflow) {
		m.overflow = true
	}
	m.mu.Unlock()
	select {
	case m.Errors <- err:
	case <-m.closing:
	}
}

func (m *RootSetManager) deliver(ev Event) {
	select {
	case m.Events <- ev:
	case <-m.closing:
	}
}

// dedupeLocked reports whether ev is a duplicate of an event delivered
// recently. It must be called with m.mu held.
func (m *RootSetManager) dedupeLocked(ev Event) bool {
	key := dedupeKey{name: filepath.Clean(ev.Name), op: ev.Op}
	if t, ok := m.seen[key]; ok && time.Since(t) < rootSetDedupeWindow {
		return true
	}
	m.seen[key] = time.Now()
	if len(m.seen) > 256 {
		now := time.Now()
		for k, t := range m.seen {
			if now.Sub(t) >= rootSetDedupeWindow {
				delete(m.seen, k)
			}
		}
	}
	return false
}

// longestRoot returns the longest root in roots that contains name. A root
// contains name if name equals the root or is located below it.
func longestRoot(name string, roots map[string]struct{}) (string, bool) {
	name = filepath.Clean(name)
	best := ""
	for r := range roots {
		if name == r || strings.HasPrefix(name, r+string(os.PathSeparator)) {
			if len(r) > len(best) {
				best = r
			}
		}
	}
	return best, best != ""
}

// cleanRoots normalizes root paths and drops duplicates.
func cleanRoots(roots []string) []string {
	out := make([]string, 0, len(roots))
	seen := make(map[string]struct{}, len(roots))
	for _, r := range roots {
		r = filepath.Clean(r)
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}
