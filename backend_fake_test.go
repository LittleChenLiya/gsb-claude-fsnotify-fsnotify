package fsnotify

import (
	"path/filepath"
	"sync"
	"testing"
)

// fakeBackend is a deterministic, controllable backend used to test the
// generation/drain machinery without depending on OS notification timing.
type fakeBackend struct {
	*shared

	mu      sync.Mutex
	watches map[string]Generation
}

func newFakeBackend(ev chan Event, errs chan error) *fakeBackend {
	return &fakeBackend{
		shared:  newShared(ev, errs),
		watches: make(map[string]Generation),
	}
}

func (f *fakeBackend) Add(name string) error { return f.AddWith(name) }

func (f *fakeBackend) AddWith(name string, _ ...addOpt) error {
	if f.isClosed() {
		return ErrClosed
	}
	name = cleanPath(name)
	f.mu.Lock()
	_, existed := f.watches[name]
	f.mu.Unlock()
	if existed {
		return nil
	}
	gen := f.shared.added(name)
	f.mu.Lock()
	f.watches[name] = gen
	f.mu.Unlock()
	return nil
}

func (f *fakeBackend) Remove(name string) error {
	if f.isClosed() {
		return nil
	}
	name = cleanPath(name)
	f.mu.Lock()
	if _, ok := f.watches[name]; !ok {
		f.mu.Unlock()
		return ErrNonExistentWatch
	}
	delete(f.watches, name)
	f.mu.Unlock()
	f.shared.removed(name)
	return nil
}

func (f *fakeBackend) RemoveAndDrain(name string) (Generation, <-chan struct{}, error) {
	if f.isClosed() {
		return 0, closedChan(), nil
	}
	name = cleanPath(name)
	gen, done := f.shared.beginDrain(name)
	if err := f.Remove(name); err != nil {
		return 0, closedChan(), err
	}
	return gen, done, nil
}

func (f *fakeBackend) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	l := make([]string, 0, len(f.watches))
	for p := range f.watches {
		l = append(l, p)
	}
	return l
}

func (f *fakeBackend) Close() error {
	if f.shared.close() {
		return nil
	}
	f.waitDispatch()
	close(f.Errors)
	close(f.Events)
	return nil
}

func (f *fakeBackend) xSupports(Op) bool { return true }

// genFor reports the generation the backend currently associates with a path.
func (f *fakeBackend) genFor(name string) Generation {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watches[cleanPath(name)]
}

// deliver mimics a backend reader accepting an event.
func (f *fakeBackend) deliver(e Event) bool {
	return f.shared.sendEvent(e)
}

func (f *fakeBackend) deliverErr(err error) bool { return f.shared.sendError(err) }

func cleanPath(p string) string { return filepath.Clean(p) }

func newFakeWatcher(t *testing.T, buffer uint) *Watcher {
	t.Helper()
	var ev chan Event
	if buffer > 0 {
		ev = make(chan Event, buffer)
	} else {
		ev = make(chan Event)
	}
	errs := make(chan error, 8)
	b := newFakeBackend(ev, errs)
	w := &Watcher{b: b, Events: ev, Errors: errs}
	t.Cleanup(func() { w.Close() })
	return w
}
