package fsnotify

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBackend is a controllable backend implementation used to exercise the
// ScopeManager: it records Add/Remove calls, can be told to fail Add, and
// lets tests inject events and errors directly.
type fakeBackend struct {
	mu      sync.Mutex
	ev      chan Event
	errs    chan error
	adds    map[string]int
	removes map[string]int
	addErr  map[string]error
	closed  bool
}

func newFakeWatcher() (*Watcher, *fakeBackend) {
	ev, errs := make(chan Event, 256), make(chan error, 256)
	fb := &fakeBackend{
		ev:      ev,
		errs:    errs,
		adds:    make(map[string]int),
		removes: make(map[string]int),
		addErr:  make(map[string]error),
	}
	return &Watcher{b: fb, Events: ev, Errors: errs}, fb
}

func (f *fakeBackend) Add(p string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.addErr[p]; err != nil {
		return err
	}
	f.adds[p]++
	return nil
}

func (f *fakeBackend) AddWith(p string, opts ...addOpt) error { return f.Add(p) }

func (f *fakeBackend) Remove(p string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removes[p]++
	return nil
}

func (f *fakeBackend) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var l []string
	for p, n := range f.adds {
		if n-f.removes[p] > 0 {
			l = append(l, p)
		}
	}
	return l
}

func (f *fakeBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.ev)
	}
	return nil
}

func (f *fakeBackend) xSupports(op Op) bool { return true }

func (f *fakeBackend) count(p string) (adds, removes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.adds[p], f.removes[p]
}

func waitScopeEvent(t *testing.T, s *Subscription) Event {
	t.Helper()
	select {
	case e, ok := <-s.Events:
		if !ok {
			t.Fatal("subscription Events channel closed unexpectedly")
		}
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

func waitScopeError(t *testing.T, s *Subscription) error {
	t.Helper()
	select {
	case err, ok := <-s.Errors:
		if !ok {
			t.Fatal("subscription Errors channel closed unexpectedly")
		}
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for error")
		return nil
	}
}

// Two scopes subscribe to the same path on a real Watcher; closing one must
// not affect the other, and the native watch is removed only after the last
// scope closes.
func TestWatchScopeSharedPath(t *testing.T) {
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "dir")
	mkdir(t, dir)

	w := newWatcher(t)
	m := NewScopeManager(w)
	defer m.Close()

	a, err := m.Subscribe(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	// Trailing separator normalizes to the same path: one native watch.
	b, err := m.Subscribe(dir+string(filepath.Separator), 16)
	if err != nil {
		t.Fatal(err)
	}
	if l := w.WatchList(); len(l) != 1 {
		t.Fatalf("WatchList: want 1 path, got %v", l)
	}

	touch(t, filepath.Join(dir, "file-1"))
	if e := waitScopeEvent(t, a); !strings.HasSuffix(e.Name, "file-1") {
		t.Fatalf("scope a: unexpected event %v", e)
	}
	if e := waitScopeEvent(t, b); !strings.HasSuffix(e.Name, "file-1") {
		t.Fatalf("scope b: unexpected event %v", e)
	}

	// Closing scope a must not affect scope b or the native watch.
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if l := w.WatchList(); len(l) != 1 {
		t.Fatalf("WatchList after first close: want 1 path, got %v", l)
	}
	touch(t, filepath.Join(dir, "file-2"))
	if e := waitScopeEvent(t, b); !strings.HasSuffix(e.Name, "file-2") {
		t.Fatalf("scope b: unexpected event %v", e)
	}

	// Last close removes the native watch.
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	waitForEvents()
	if l := w.WatchList(); len(l) != 0 {
		t.Fatalf("WatchList after last close: want empty, got %v", l)
	}
}

// Native Add/Remove calls must be reference counted: one Add for any number
// of scopes on the same path, one Remove when the last scope closes.
func TestWatchScopeRefcount(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)
	defer m.Close()

	p := filepath.Clean("/some/dir")
	var subs []*Subscription
	for i := 0; i < 3; i++ {
		s, err := m.Subscribe(p, 4)
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, s)
	}
	if adds, removes := fb.count(p); adds != 1 || removes != 0 {
		t.Fatalf("after 3 subscribes: adds=%d removes=%d, want 1/0", adds, removes)
	}

	for i, s := range subs {
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		wantRemoves := 0
		if i == len(subs)-1 {
			wantRemoves = 1
		}
		if adds, removes := fb.count(p); adds != 1 || removes != wantRemoves {
			t.Fatalf("after close %d: adds=%d removes=%d, want 1/%d", i, adds, removes, wantRemoves)
		}
	}
}

// A slow subscription with a full buffer drops events and gets its own gap
// notification, without blocking the dispatcher or other subscriptions.
func TestWatchScopeGap(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)
	defer m.Close()

	p := filepath.Clean("/gap/dir")
	slow, err := m.Subscribe(p, 1)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := m.Subscribe(p, 64)
	if err != nil {
		t.Fatal(err)
	}

	const n = 32
	for i := 0; i < n; i++ {
		fb.ev <- Event{Name: filepath.Join(p, fmt.Sprintf("file-%d", i)), Op: Create}
	}

	// The fast subscription receives every event: the slow one never blocks
	// the dispatcher.
	for i := 0; i < n; i++ {
		e := waitScopeEvent(t, fast)
		if want := fmt.Sprintf("file-%d", i); filepath.Base(e.Name) != want {
			t.Fatalf("fast subscription: got %q, want %q", e.Name, want)
		}
	}

	// The slow subscription dropped events and is notified of the gap.
	err = waitScopeError(t, slow)
	if !errors.Is(err, ErrEventGap) {
		t.Fatalf("slow subscription error: want ErrEventGap, got %v", err)
	}
	if !errors.Is(err, ErrEventOverflow) {
		t.Fatalf("ErrEventGap should wrap ErrEventOverflow: %v", err)
	}
}

// A failing Add on the underlying Watcher is reported to the subscribing
// caller only, and no state is kept.
func TestWatchScopeAddError(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)
	defer m.Close()

	good := filepath.Clean("/good")
	bad := filepath.Clean("/bad")
	fb.addErr[bad] = errors.New("no space left on device")

	ok, err := m.Subscribe(good, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Subscribe(bad, 4); err == nil {
		t.Fatal("Subscribe: expected error, got nil")
	}
	if adds, _ := fb.count(bad); adds != 0 {
		t.Fatalf("failed Add counted: adds=%d", adds)
	}
	if l := w.WatchList(); len(l) != 1 || l[0] != good {
		t.Fatalf("WatchList: want [%q], got %v", good, l)
	}

	// The other scope still works.
	fb.ev <- Event{Name: filepath.Join(good, "f"), Op: Write}
	waitScopeEvent(t, ok)
}

// Closing a subscription or the manager more than once is a no-op; channels
// are closed exactly once.
func TestWatchScopeDuplicateClose(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)

	p := filepath.Clean("/dup/dir")
	s, err := m.Subscribe(p, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close %d: %s", i, err)
		}
	}
	if _, ok := <-s.Events; ok {
		t.Fatal("Events channel not closed")
	}
	if _, ok := <-s.Errors; ok {
		t.Fatal("Errors channel not closed")
	}
	if adds, removes := fb.count(p); adds != 1 || removes != 1 {
		t.Fatalf("adds=%d removes=%d, want 1/1", adds, removes)
	}

	s2, err := m.Subscribe(p, 4)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := m.Close(); err != nil {
			t.Fatalf("manager Close %d: %s", i, err)
		}
	}
	if _, ok := <-s2.Events; ok {
		t.Fatal("s2 Events channel not closed")
	}
	if _, err := m.Subscribe(p, 4); !errors.Is(err, ErrClosed) {
		t.Fatalf("Subscribe after Close: want ErrClosed, got %v", err)
	}
}

// Errors mentioning a watched path go to that path's scopes only; anything
// else is broadcast to all scopes.
func TestWatchScopeErrorDispatch(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)
	defer m.Close()

	p1 := filepath.Clean("/one")
	p2 := filepath.Clean("/two")
	s1, err := m.Subscribe(p1, 4)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := m.Subscribe(p2, 4)
	if err != nil {
		t.Fatal(err)
	}

	scoped := fmt.Errorf("watch %q: broken", p1)
	fb.errs <- scoped
	if err := waitScopeError(t, s1); !errors.Is(err, scoped) {
		t.Fatalf("s1: got %v", err)
	}
	select {
	case err := <-s2.Errors:
		t.Fatalf("s2 should not get scoped error, got %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	global := errors.New("fsnotify: queue or buffer overflow")
	fb.errs <- global
	for i, s := range []*Subscription{s1, s2} {
		if err := waitScopeError(t, s); !errors.Is(err, global) {
			t.Fatalf("s%d: got %v", i+1, err)
		}
	}
}

// Closing the underlying Watcher closes all subscription channels.
func TestWatchScopeWatcherClose(t *testing.T) {
	w, _ := newFakeWatcher()
	m := NewScopeManager(w)
	defer m.Close()

	s, err := m.Subscribe(filepath.Clean("/w/dir"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-s.Events:
		if ok {
			t.Fatal("Events channel not closed after Watcher.Close")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Events to close")
	}
}

// Concurrent subscribes and cancels on shared paths; run with -race.
func TestWatchScopeConcurrent(t *testing.T) {
	w, fb := newFakeWatcher()
	m := NewScopeManager(w)

	paths := []string{
		filepath.Clean("/c/a"),
		filepath.Clean("/c/b"),
		filepath.Clean("/c/a") + string(filepath.Separator), // same as /c/a
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				s, err := m.Subscribe(paths[(i+j)%len(paths)], 4)
				if err != nil {
					return // Manager closed.
				}
				select {
				case <-s.Events:
				default:
				}
				s.Close()
			}
		}(i)
	}

	// Stream events while scopes come and go.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			select {
			case fb.ev <- Event{Name: filepath.Join("/c/a", "f"), Op: Write}:
			case <-m.done:
				return
			}
		}
	}()

	wg.Wait()
	<-done
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if l := w.WatchList(); len(l) != 0 {
		t.Fatalf("WatchList: want empty, got %v", l)
	}
}
