package fsnotify

import (
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeBackend is a backend implementation for tests: it records which roots
// are watched, can inject Add/Remove failures, and lets tests feed events and
// errors deterministically.
type fakeBackend struct {
	mu          sync.Mutex
	watches     map[string]bool
	addErr      map[string]error
	removeErr   map[string]error
	events      chan<- Event
	errs        chan<- error
	closed      bool
	addHook     func(path string)
	removeHook  func(path string)
	addCalls    []string
	removeCalls []string
}

func (f *fakeBackend) Add(path string) error {
	f.mu.Lock()
	f.addCalls = append(f.addCalls, path)
	if f.closed {
		f.mu.Unlock()
		return ErrClosed
	}
	if err, ok := f.addErr[path]; ok && err != nil {
		f.mu.Unlock()
		return err
	}
	f.watches[path] = true
	hook := f.addHook
	f.mu.Unlock()
	if hook != nil {
		hook(path)
	}
	return nil
}

func (f *fakeBackend) AddWith(path string, opts ...addOpt) error { return f.Add(path) }

func (f *fakeBackend) Remove(path string) error {
	f.mu.Lock()
	f.removeCalls = append(f.removeCalls, path)
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	if err, ok := f.removeErr[path]; ok && err != nil {
		f.mu.Unlock()
		return err
	}
	if !f.watches[path] {
		f.mu.Unlock()
		return ErrNonExistentWatch
	}
	delete(f.watches, path)
	hook := f.removeHook
	f.mu.Unlock()
	if hook != nil {
		hook(path)
	}
	return nil
}

func (f *fakeBackend) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.watches))
	for w := range f.watches {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

func (f *fakeBackend) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	f.watches = nil
	f.mu.Unlock()
	close(f.events)
	return nil
}

func (f *fakeBackend) xSupports(op Op) bool { return true }

func (f *fakeBackend) sortedWatches() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.watches))
	for w := range f.watches {
		out = append(out, w)
	}
	sort.Strings(out)
	return out
}

func newFakeRootSetManager(t *testing.T, fb *fakeBackend) *RootSetManager {
	t.Helper()
	evCh := make(chan Event, 64)
	errCh := make(chan error, 64)
	fb.events = evCh
	fb.errs = errCh
	w := &Watcher{b: fb, Events: evCh, Errors: errCh}
	return newRootSetManager(w)
}

func TestRootSetManager_AddFailureRollback(t *testing.T) {
	fb := &fakeBackend{
		watches: map[string]bool{"a": true},
		addErr:  map[string]error{"c": errors.New("boom")},
	}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}

	// The old root keeps delivering events before the replacement.
	fb.events <- Event{Name: "a/x", Op: Create}
	select {
	case ev := <-m.Events:
		if ev.Name != "a/x" {
			t.Fatalf("got event %q, want a/x", ev.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("old root event not delivered")
	}

	err := m.Replace([]string{"b", "c"})
	if err == nil {
		t.Fatal("Replace succeeded, want error")
	}
	if !strings.Contains(err.Error(), `adding root "c"`) {
		t.Fatalf("error %q does not mention root c", err)
	}

	// The previously published set is still in effect ...
	if got := m.Roots(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("published roots = %v, want [a]", got)
	}
	// ... and only the old root is still watched; b was recycled.
	if got := fb.sortedWatches(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("watched roots = %v, want [a]", got)
	}

	// The old root still works ...
	fb.events <- Event{Name: "a/y", Op: Write}
	select {
	case ev := <-m.Events:
		if ev.Name != "a/y" {
			t.Fatalf("got event %q, want a/y", ev.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("old root event not delivered after failed replacement")
	}

	// ... but the recycled root b delivers nothing.
	fb.events <- Event{Name: "b/z", Op: Create}
	select {
	case ev := <-m.Events:
		t.Fatalf("recycled root delivered event: %v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRootSetManager_StagesNewRootEventsUntilPublish(t *testing.T) {
	fb := &fakeBackend{watches: map[string]bool{"a": true}}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}

	// Replace delivers staged events before returning, so collect events in
	// a concurrent reader.
	events := make(chan Event, 64)
	go func() {
		for ev := range m.Events {
			events <- ev
		}
	}()

	// While the set is being prepared, events on new roots are staged and
	// events on the old root are still delivered.
	fb.addHook = func(path string) {
		if path == "b" {
			fb.events <- Event{Name: "a/mid", Op: Chmod}
			fb.events <- Event{Name: "b/1", Op: Create}
		}
	}
	if err := m.Replace([]string{"b", "c"}); err != nil {
		t.Fatal(err)
	}
	if got := m.Roots(); !reflect.DeepEqual(got, []string{"b", "c"}) {
		t.Fatalf("published roots = %v, want [b c]", got)
	}

	// Collect everything that is delivered: the live old-root event and the
	// staged new-root event, each exactly once.
	got := map[string]int{}
	deadline := time.After(500 * time.Millisecond)
loop:
	for {
		select {
		case ev := <-events:
			got[ev.Name]++
		case <-deadline:
			break loop
		}
	}
	if got["a/mid"] != 1 {
		t.Fatalf("a/mid delivered %d times, want 1 (old root, live)", got["a/mid"])
	}
	if got["b/1"] != 1 {
		t.Fatalf("b/1 delivered %d times, want 1 (staged new root)", got["b/1"])
	}

	// After publication the retired root a is filtered out.
	fb.events <- Event{Name: "a/late", Op: Remove}
	select {
	case ev := <-events:
		t.Fatalf("event on retired root delivered: %v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRootSetManager_NestedRootsLongestMatch(t *testing.T) {
	a := filepath.Join(t.TempDir(), "a")
	ab := filepath.Join(a, "b")
	fb := &fakeBackend{watches: map[string]bool{}}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()

	events := make(chan Event, 64)
	go func() {
		for ev := range m.Events {
			events <- ev
		}
	}()

	fb.addHook = func(path string) {
		switch path {
		case a:
			fb.events <- Event{Name: filepath.Join(a, "y"), Op: Write}
		case ab:
			// Both watches (a and a/b) report the same event.
			fb.events <- Event{Name: filepath.Join(ab, "x"), Op: Create}
			fb.events <- Event{Name: filepath.Join(ab, "x"), Op: Create}
		}
	}
	if err := m.Replace([]string{a, ab}); err != nil {
		t.Fatal(err)
	}

	got := map[string]int{}
	for i := 0; i < 2; i++ {
		select {
		case ev := <-events:
			got[ev.Name]++
		case <-time.After(time.Second):
			t.Fatalf("missing event %d", i)
		}
	}
	if got[filepath.Join(ab, "x")] != 1 {
		t.Fatalf("a/b/x delivered %d times, want 1", got[filepath.Join(ab, "x")])
	}
	if got[filepath.Join(a, "y")] != 1 {
		t.Fatalf("a/y delivered %d times, want 1", got[filepath.Join(a, "y")])
	}
	select {
	case ev := <-events:
		t.Fatalf("unexpected duplicate event: %v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	// Each event is attributed to exactly one root: the longest match.
	roots := map[string]struct{}{a: {}, ab: {}}
	for _, c := range []struct {
		name string
		root string
		ok   bool
	}{
		{filepath.Join(ab, "x"), ab, true},
		{filepath.Join(a, "y"), a, true},
		{ab, ab, true},
		{filepath.Join(t.TempDir(), "z"), "", false},
	} {
		root, ok := longestRoot(c.name, roots)
		if ok != c.ok || (ok && root != c.root) {
			t.Errorf("longestRoot(%q) = %q, %v; want %q, %v", c.name, root, ok, c.root, c.ok)
		}
	}
}

func TestRootSetManager_RemoveErrorRetirement(t *testing.T) {
	removeErr := errors.New("stuck")
	fb := &fakeBackend{
		watches:   map[string]bool{"a": true},
		removeErr: map[string]error{"a": removeErr},
	}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}

	err := m.Replace([]string{"b"})
	var retirement *RetirementError
	if !errors.As(err, &retirement) {
		t.Fatalf("Replace error = %v, want *RetirementError", err)
	}
	if !errors.Is(err, removeErr) {
		t.Fatalf("error %v does not wrap the remove error", err)
	}
	if !reflect.DeepEqual(retirement.Roots, []string{"a"}) {
		t.Fatalf("retired roots = %v, want [a]", retirement.Roots)
	}

	// The new set is published regardless of the retirement failure.
	if got := m.Roots(); !reflect.DeepEqual(got, []string{"b"}) {
		t.Fatalf("published roots = %v, want [b]", got)
	}

	// The remaining watcher keeps working.
	fb.events <- Event{Name: "b/x", Op: Create}
	select {
	case ev := <-m.Events:
		if ev.Name != "b/x" {
			t.Fatalf("got %q, want b/x", ev.Name)
		}
	case <-time.After(time.Second):
		t.Fatal("new root event not delivered")
	}
}

func TestRootSetManager_Close(t *testing.T) {
	fb := &fakeBackend{watches: map[string]bool{"a": true}}
	m := newFakeRootSetManager(t, fb)

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := m.Replace([]string{"b"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Replace after Close = %v, want ErrClosed", err)
	}
	select {
	case _, ok := <-m.Events:
		if ok {
			t.Fatal("Events channel not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("Events channel not closed after 1s")
	}
	select {
	case _, ok := <-m.Errors:
		if ok {
			t.Fatal("Errors channel not closed")
		}
	case <-time.After(time.Second):
		t.Fatal("Errors channel not closed after 1s")
	}
}

func TestRootSetManager_CloseDuringReplace(t *testing.T) {
	fb := &fakeBackend{watches: map[string]bool{"a": true}}
	hookDone := make(chan struct{})
	fb.addHook = func(path string) {
		if path == "b" {
			close(hookDone)
			time.Sleep(200 * time.Millisecond)
		}
	}
	m := newFakeRootSetManager(t, fb)
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- m.Replace([]string{"b", "c"}) }()

	<-hookDone
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Replace after Close = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Replace did not return after Close")
	}
}

func TestRootSetManager_OverflowFailsReplace(t *testing.T) {
	fb := &fakeBackend{watches: map[string]bool{"a": true}}
	proceed := make(chan struct{})
	fb.addHook = func(path string) {
		if path == "b" {
			fb.errs <- ErrEventOverflow
			<-proceed
		}
	}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- m.Replace([]string{"b", "c"}) }()

	// The overflow is reported on the Errors channel ...
	select {
	case err := <-m.Errors:
		if !errors.Is(err, ErrEventOverflow) {
			t.Fatalf("Errors got %v, want ErrEventOverflow", err)
		}
	case <-time.After(time.Second):
		t.Fatal("overflow not reported")
	}

	// ... and fails the replacement: the old set stays in effect and the new
	// roots are recycled.
	close(proceed)
	select {
	case err := <-done:
		if !errors.Is(err, ErrEventOverflow) {
			t.Fatalf("Replace = %v, want ErrEventOverflow", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Replace did not return")
	}
	if got := m.Roots(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("published roots = %v, want [a]", got)
	}
	if got := fb.sortedWatches(); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("watched roots = %v, want [a]", got)
	}
}

func TestRootSetManager_RealWatcherSmoke(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a")
	b := filepath.Join(dir, "b")
	mkdir(t, a)
	mkdir(t, b)

	m, err := NewRootSetManager()
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()

	if err := m.Replace([]string{a}); err != nil {
		t.Fatalf("Replace([a]): %v", err)
	}
	if err := m.Replace([]string{b}); err != nil {
		t.Fatalf("Replace([b]): %v", err)
	}
	if got := m.Roots(); !reflect.DeepEqual(got, []string{b}) {
		t.Fatalf("Roots() = %v, want [b]", got)
	}
}

// TestRootSetManager_DemoPublicationTiming demonstrates when a root set is
// published and how a short burst of events is delivered:
//
//	phase 1: the published set is {a}; events on a are delivered immediately.
//	phase 2: Replace({b,c}) is preparing. Events on the new roots b/c are
//	         staged, while events on the old root a are still delivered live.
//	phase 3: the set {b,c} is published. The staged events are delivered once,
//	         and further events on the retired root a are filtered out.
func TestRootSetManager_DemoPublicationTiming(t *testing.T) {
	fb := &fakeBackend{watches: map[string]bool{"a": true}}
	m := newFakeRootSetManager(t, fb)
	defer m.Close()
	if err := m.Replace([]string{"a"}); err != nil {
		t.Fatal(err)
	}

	// Phase 1: the published set is {a}; an event on a is delivered.
	fb.events <- Event{Name: "a/old", Op: Write}
	ev := <-m.Events
	t.Logf("phase 1 published=%v: %s", m.Roots(), ev)

	// Replace delivers staged events before returning, so collect events in
	// a concurrent reader.
	events := make(chan Event, 64)
	go func() {
		for ev := range m.Events {
			events <- ev
		}
	}()

	// Phase 2: start replacing {a} with {b,c}. The short burst of events
	// arrives while the set is being prepared.
	var once sync.Once
	fb.addHook = func(path string) {
		once.Do(func() {
			fb.events <- Event{Name: "a/mid", Op: Chmod}
			fb.events <- Event{Name: "b/1", Op: Create}
			fb.events <- Event{Name: "b/2", Op: Write}
		})
	}
	replaceDone := make(chan error, 1)
	go func() { replaceDone <- m.Replace([]string{"b", "c"}) }()

	// The old root keeps delivering while the set is being prepared.
	select {
	case ev := <-events:
		t.Logf("phase 2 preparing, old root live: %s", ev)
	case <-time.After(time.Second):
		t.Fatal("old root event not delivered during preparation")
	}
	if err := <-replaceDone; err != nil {
		t.Fatal(err)
	}

	// Phase 3: the set is published; the staged events are delivered once.
	t.Logf("phase 3 published=%v: staged events delivered once:", m.Roots())
	for i := 0; i < 2; i++ {
		select {
		case ev := <-events:
			t.Logf("  %s", ev)
		case <-time.After(time.Second):
			t.Fatalf("staged event %d not delivered", i)
		}
	}

	// Phase 3 continued: the retired root a is filtered out.
	fb.events <- Event{Name: "a/late", Op: Remove}
	select {
	case ev := <-events:
		t.Fatalf("event on retired root delivered after publication: %v", ev)
	case <-time.After(100 * time.Millisecond):
		t.Logf("phase 3: event on retired root dropped")
	}
}
