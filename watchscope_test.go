package fsnotify

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeBackend is a controllable in-memory backend used to drive the
// ScopeWatcher fan-in loop deterministically and to count native watch
// resources.
type fakeBackend struct {
	events chan Event
	errs   chan error

	mu      sync.Mutex
	adds    int
	removes int
	watched map[string]int
	addErr  error
	closed  bool
}

func newFakeWatcher() (*Watcher, *fakeBackend) {
	ev, errs := make(chan Event, 64), make(chan error, 64)
	fb := &fakeBackend{
		events:  ev,
		errs:    errs,
		watched: make(map[string]int),
	}
	return &Watcher{b: fb, Events: ev, Errors: errs}, fb
}

func (f *fakeBackend) Add(p string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	if f.addErr != nil {
		return f.addErr
	}
	f.adds++
	f.watched[p]++
	return nil
}

func (f *fakeBackend) AddWith(p string, opts ...addOpt) error { return f.Add(p) }

func (f *fakeBackend) Remove(p string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	if f.watched[p] == 0 {
		return ErrNonExistentWatch
	}
	f.removes++
	f.watched[p]--
	if f.watched[p] == 0 {
		delete(f.watched, p)
	}
	return nil
}

func (f *fakeBackend) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := make([]string, 0, len(f.watched))
	for p := range f.watched {
		l = append(l, p)
	}
	return l
}

func (f *fakeBackend) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.mu.Unlock()
	close(f.events)
	close(f.errs)
	return nil
}

func (f *fakeBackend) xSupports(Op) bool { return true }

func (f *fakeBackend) counts() (adds, removes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.adds, f.removes
}

func (f *fakeBackend) watchedN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watched)
}

func waitScopeEvent(t *testing.T, s *Scope, wait time.Duration) Event {
	t.Helper()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case e, ok := <-s.Events:
		if !ok {
			t.Fatal("scope Events channel closed unexpectedly")
		}
		return e
	case err := <-s.Errors:
		t.Fatalf("unexpected error on scope: %v", err)
	case <-timer.C:
		t.Fatal("timed out waiting for scope event")
	}
	return Event{}
}

func waitScopeErr(t *testing.T, s *Scope, wait time.Duration) error {
	t.Helper()
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case err, ok := <-s.Errors:
		if !ok {
			t.Fatal("scope Errors channel closed unexpectedly")
		}
		return err
	case <-timer.C:
		t.Fatal("timed out waiting for scope error")
	}
	return nil
}

func TestWatchScopeSharedPath(t *testing.T) {
	tmp := t.TempDir()

	w, err := NewWatcher()
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	sw := NewScopeWatcher(w)
	defer sw.Close()

	s1, s2 := sw.NewScope(16), sw.NewScope(16)

	// Same directory through two different spellings; they must share a
	// single native watch after normalization.
	if err := s1.Add(tmp); err != nil {
		t.Fatal(err)
	}
	if err := s2.Add(tmp + string(os.PathSeparator) + "."); err != nil {
		t.Fatal(err)
	}

	norm, _ := normalizeScopePath(tmp)
	if l := w.WatchList(); len(l) != 1 || l[0] != norm {
		t.Fatalf("WatchList = %q, want exactly [%q]", l, norm)
	}

	// Closing the first scope must not affect the second.
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if l := w.WatchList(); len(l) != 1 || l[0] != norm {
		t.Fatalf("WatchList after first close = %q, want [%q]", l, norm)
	}

	// The remaining scope still receives real file modifications.
	file := filepath.Join(tmp, "file")
	echoAppend(t, "hello", file)
	waitForEvents()
	e := waitScopeEvent(t, s2, 5*time.Second)
	if e.Name != file {
		t.Fatalf("event Name = %q, want %q", e.Name, file)
	}

	// Last close removes the native watch.
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if l := w.WatchList(); len(l) != 0 {
		t.Fatalf("WatchList after last close = %q, want empty", l)
	}
}

func TestWatchScopeRefcount(t *testing.T) {
	w, fb := newFakeWatcher()
	sw := NewScopeWatcher(w)
	defer sw.Close()

	s1, s2, s3 := sw.NewScope(4), sw.NewScope(4), sw.NewScope(4)
	dir := t.TempDir()
	other := t.TempDir()

	mustAdd := func(s *Scope, p string) {
		t.Helper()
		if err := s.Add(p); err != nil {
			t.Fatal(err)
		}
	}
	mustAdd(s1, dir)
	mustAdd(s2, dir)
	mustAdd(s3, dir)
	mustAdd(s1, other)

	if adds, _ := fb.counts(); adds != 2 {
		t.Fatalf("native Add called %d times, want 2 (shared path counted once)", adds)
	}
	if n := fb.watchedN(); n != 2 {
		t.Fatalf("native watches = %d, want 2", n)
	}

	// Removing from one scope keeps the shared native watch.
	if err := s1.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, removes := fb.counts(); removes != 0 {
		t.Fatalf("native Remove called %d times, want 0 while scopes remain", removes)
	}

	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	if _, removes := fb.counts(); removes != 0 {
		t.Fatalf("native Remove called %d times, want 0 while one scope remains", removes)
	}

	// Last scope for dir closes: native watch removed.
	if err := s3.Close(); err != nil {
		t.Fatal(err)
	}
	if _, removes := fb.counts(); removes != 1 {
		t.Fatalf("native Remove called %d times, want 1", removes)
	}

	// Close the remaining scope; native add/remove counts must balance.
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	adds, removes := fb.counts()
	if adds != removes {
		t.Fatalf("native adds (%d) != native removes (%d)", adds, removes)
	}
	if n := fb.watchedN(); n != 0 {
		t.Fatalf("native watches left = %d, want 0", n)
	}
}

func TestWatchScopeGap(t *testing.T) {
	w, fb := newFakeWatcher()
	sw := NewScopeWatcher(w)
	defer sw.Close()

	dir := t.TempDir()
	norm, _ := normalizeScopePath(dir)

	slow := sw.NewScope(1) // Tiny buffer; never drained.
	fast := sw.NewScope(64)
	if err := slow.Add(dir); err != nil {
		t.Fatal(err)
	}
	if err := fast.Add(dir); err != nil {
		t.Fatal(err)
	}

	// Overflow the slow scope: buffer holds 1, the rest are dropped.
	const n = 10
	for i := 0; i < n; i++ {
		fb.events <- Event{Name: filepath.Join(norm, fmt.Sprintf("f%d", i)), Op: Write}
	}

	// The fast scope must receive every event: a slow scope never blocks
	// the shared fan-in loop.
	for i := 0; i < n; i++ {
		e := waitScopeEvent(t, fast, 5*time.Second)
		if want := fmt.Sprintf("f%d", i); filepath.Base(e.Name) != want {
			t.Fatalf("fast scope event %d = %q, want file %q", i, e.Name, want)
		}
	}

	// The slow scope gets its own gap notification.
	err := waitScopeErr(t, slow, 5*time.Second)
	if !errors.Is(err, ErrScopeGap) {
		t.Fatalf("slow scope error = %v, want ErrScopeGap", err)
	}

	// And it still has the buffered event available.
	select {
	case <-slow.Events:
	default:
		t.Fatal("slow scope buffer should still hold its first event")
	}
}

func TestWatchScopeAddFailure(t *testing.T) {
	w, fb := newFakeWatcher()
	sw := NewScopeWatcher(w)
	defer sw.Close()

	dir := t.TempDir()
	fb.addErr = errors.New("boom")

	s1, s2 := sw.NewScope(4), sw.NewScope(4)
	if err := s1.Add(dir); err == nil {
		t.Fatal("expected Add error")
	} else if err.Error() != "boom" {
		t.Fatalf("Add error = %v, want boom", err)
	}

	// Failed Add must not leak a reference count.
	if adds, _ := fb.counts(); adds != 0 {
		t.Fatalf("native adds = %d, want 0 after failed Add", adds)
	}

	// A retry after the backend recovers must succeed, and other scopes
	// are unaffected.
	fb.addErr = nil
	if err := s1.Add(dir); err != nil {
		t.Fatal(err)
	}
	if err := s2.Add(dir); err != nil {
		t.Fatal(err)
	}
	if adds, _ := fb.counts(); adds != 1 {
		t.Fatalf("native adds = %d, want 1 (shared watch)", adds)
	}
}

func TestWatchScopeErrorBroadcast(t *testing.T) {
	w, fb := newFakeWatcher()
	sw := NewScopeWatcher(w)
	defer sw.Close()

	dir := t.TempDir()
	s1, s2 := sw.NewScope(4), sw.NewScope(4)
	if err := s1.Add(dir); err != nil {
		t.Fatal(err)
	}
	if err := s2.Add(dir); err != nil {
		t.Fatal(err)
	}

	// Watcher-level errors are global: every scope gets a copy.
	fb.errs <- ErrEventOverflow
	for _, s := range []*Scope{s1, s2} {
		if err := waitScopeErr(t, s, 5*time.Second); !errors.Is(err, ErrEventOverflow) {
			t.Fatalf("scope error = %v, want ErrEventOverflow", err)
		}
	}
}

func TestWatchScopeDoubleClose(t *testing.T) {
	w, _ := newFakeWatcher()
	sw := NewScopeWatcher(w)

	s := sw.NewScope(4)
	if err := s.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close %d: %v", i, err)
		}
	}

	// Channels closed exactly once: reads report !ok, and no panic from a
	// second close.
	if _, ok := <-s.Events; ok {
		t.Fatal("Events channel not closed")
	}
	if _, ok := <-s.Errors; ok {
		t.Fatal("Errors channel not closed")
	}

	// Operating on a closed scope fails.
	if err := s.Add(t.TempDir()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Add on closed scope = %v, want ErrClosed", err)
	}

	// ScopeWatcher.Close is idempotent too.
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWatchScopeConcurrent(t *testing.T) {
	w, fb := newFakeWatcher()
	sw := NewScopeWatcher(w)

	dirs := make([]string, 4)
	for i := range dirs {
		dirs[i] = t.TempDir()
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := sw.NewScope(2)
			for j := 0; j < 20; j++ {
				dir := dirs[(i+j)%len(dirs)]
				if err := s.Add(dir); err != nil {
					return // ScopeWatcher closed.
				}
				s.Remove(dir)
				s.Add(dir)
			}
			s.Close()
			s.Close() // Double close is fine.
		}(i)
	}

	// Feed events and errors while scopes come and go.
	stop := make(chan struct{})
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		for {
			select {
			case <-stop:
				return
			default:
			}
			select {
			case fb.events <- Event{Name: dirs[0], Op: Write}:
			case fb.errs <- ErrEventOverflow:
			case <-stop:
				return
			}
		}
	}()

	wg.Wait()
	close(stop)
	<-fed

	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
	adds, removes := fb.counts()
	if adds != removes {
		t.Fatalf("native adds (%d) != native removes (%d)", adds, removes)
	}
	if n := fb.watchedN(); n != 0 {
		t.Fatalf("native watches left = %d, want 0", n)
	}
}

func TestWatchScopeWatcherClose(t *testing.T) {
	w, _ := newFakeWatcher()
	sw := NewScopeWatcher(w)

	s := sw.NewScope(4)
	if err := s.Add(t.TempDir()); err != nil {
		t.Fatal(err)
	}

	// Closing the underlying watcher closes all scopes exactly once.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case _, ok := <-s.Events:
		if ok {
			t.Fatal("expected closed Events channel")
		}
	case <-timer.C:
		t.Fatal("scope Events not closed after Watcher.Close")
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}
}
