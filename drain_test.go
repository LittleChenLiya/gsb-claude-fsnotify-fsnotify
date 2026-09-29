package fsnotify

import (
	"errors"
	"sync"
	"testing"
	"time"
)

const drainWait = 5 * time.Second

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(drainWait):
		t.Fatalf("%s: completion signal did not close", what)
	}
}

func waitOpen(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s: completion signal closed too early", what)
	case <-time.After(50 * time.Millisecond):
	}
}

func fakeOf(w *Watcher) *fakeBackend {
	b, ok := w.b.(*fakeBackend)
	if !ok {
		panic("watcher does not use fakeBackend")
	}
	return b
}

// Core scenario: an old event is stuck in the backend, the watch is removed and
// immediately re-added. The delayed event must be attributed to the old
// generation, and events after re-add to the new one.
func TestRemoveAndDrainOldEventSeparated(t *testing.T) {
	w := newFakeWatcher(t, 0)
	const path = "/watch"
	if err := w.Add(path); err != nil {
		t.Fatal(err)
	}
	oldGen := fakeOf(w).genFor(path)
	if oldGen == 0 {
		t.Fatal("no generation assigned to the first Add")
	}

	// Accept an old event. With an unbuffered Events channel and no reader it
	// is accepted by the dispatcher but stuck during delivery, exactly like an
	// event the backend took in just before the watch was removed.
	accepted := make(chan struct{})
	go func() {
		close(accepted)
		fakeOf(w).deliver(Event{Name: path, Op: Write})
	}()
	<-accepted
	time.Sleep(50 * time.Millisecond) // Let the dispatcher accept it.

	// Remove and immediately re-add while the old event is stuck.
	gen, done, err := w.RemoveAndDrain(path)
	if err != nil {
		t.Fatal(err)
	}
	if gen != oldGen {
		t.Fatalf("drained generation = %d, want %d", gen, oldGen)
	}
	if err := w.Add(path); err != nil {
		t.Fatal(err)
	}
	newGen := fakeOf(w).genFor(path)
	if newGen == 0 || newGen == oldGen {
		t.Fatalf("new generation = %d, want a generation distinct from %d", newGen, oldGen)
	}

	// The stuck event keeps the drain pending.
	waitOpen(t, done, "while old event is stuck")

	// Reading releases the old event first; ordering is preserved.
	e1 := recvEvent(t, w)
	if e1.Generation != oldGen || !e1.Has(Write) {
		t.Fatalf("first event = %+v, want write with old gen %d", e1, oldGen)
	}
	waitClosed(t, done, "after draining old events")

	// A later event belongs to the new generation.
	if !fakeOf(w).deliver(Event{Name: path, Op: Chmod}) {
		t.Fatal("new event not delivered")
	}
	e2 := recvEvent(t, w)
	if e2.Generation != newGen || !e2.Has(Chmod) {
		t.Fatalf("second event = %+v, want chmod with new gen %d", e2, newGen)
	}
}

// The completion signal must only close after events reach the consumer, even
// if the dispatcher accepted them earlier.
func TestRemoveAndDrainCompletesAfterDelivery(t *testing.T) {
	w := newFakeWatcher(t, 16)
	const path = "/watch"
	mustOK(t, w.Add(path))
	oldGen := fakeOf(w).genFor(path)

	mustBool(t, fakeOf(w).deliver(Event{Name: path, Op: Write}))
	mustBool(t, fakeOf(w).deliver(Event{Name: path, Op: Write}))

	gen, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	if gen != oldGen {
		t.Fatalf("gen = %d, want %d", gen, oldGen)
	}

	for range 2 {
		e := recvEvent(t, w)
		if e.Generation != oldGen {
			t.Fatalf("event gen = %d, want %d", e.Generation, oldGen)
		}
	}
	waitClosed(t, done, "after both events consumed")
}

// Removing the same path twice: second call reports ErrNonExistentWatch and
// still returns a safe, already-closed signal.
func TestRemoveAndDrainRepeated(t *testing.T) {
	w := newFakeWatcher(t, 16)
	const path = "/watch"
	mustOK(t, w.Add(path))
	gen, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	if gen == 0 {
		t.Fatal("first drain returned zero generation")
	}
	waitClosed(t, done, "first drain")

	gen2, done2, err2 := w.RemoveAndDrain(path)
	if !errors.Is(err2, ErrNonExistentWatch) {
		t.Fatalf("second drain error = %v, want ErrNonExistentWatch", err2)
	}
	if gen2 != 0 {
		t.Fatalf("second drain gen = %d, want 0", gen2)
	}
	select {
	case <-done2:
	default:
		t.Fatal("second drain signal should already be closed")
	}
}

// A consumer that never reads must not make RemoveAndDrain or Close deadlock;
// Close unblocks the pending drain immediately.
func TestRemoveAndDrainSlowConsumerClose(t *testing.T) {
	w := newFakeWatcher(t, 0)
	const path = "/watch"
	mustOK(t, w.Add(path))

	// Accept an old event; nobody reads it, so delivery stays blocked.
	go fakeOf(w).deliver(Event{Name: path, Op: Write})
	time.Sleep(50 * time.Millisecond)

	_, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	waitOpen(t, done, "with no consumer")

	closed := make(chan struct{})
	go func() {
		if err := w.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(drainWait):
		t.Fatal("Close deadlocked with a consumer that never reads")
	}
	waitClosed(t, done, "after Close")
}

// Calling Close with a pending drain (events still accepted) completes the
// drain rather than leaving the waiter stuck.
func TestRemoveAndDrainCloseWithPending(t *testing.T) {
	w := newFakeWatcher(t, 0)
	const path = "/watch"
	mustOK(t, w.Add(path))

	// An accepted but undelivered old event keeps the drain pending.
	go fakeOf(w).deliver(Event{Name: path, Op: Write})
	time.Sleep(50 * time.Millisecond)
	_, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	waitOpen(t, done, "pending event")

	mustOK(t, w.Close())
	waitClosed(t, done, "after close with pending")
}

// Backend errors must be deliverable and must not stall drain completion.
func TestRemoveAndDrainWithError(t *testing.T) {
	w := newFakeWatcher(t, 16)
	const path = "/watch"
	mustOK(t, w.Add(path))
	myErr := errors.New("backend exploded")
	if !fakeOf(w).deliverErr(myErr) {
		t.Fatal("error not delivered")
	}
	select {
	case got := <-w.Errors:
		if !errors.Is(got, myErr) {
			t.Fatalf("error = %v, want %v", got, myErr)
		}
	case <-time.After(drainWait):
		t.Fatal("error not received")
	}

	_, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	waitClosed(t, done, "after error and drain")
}

// Events are never reordered: a large burst from the old generation, then the
// drain, then new-generation events must arrive in acceptance order.
func TestRemoveAndDrainOrdering(t *testing.T) {
	w := newFakeWatcher(t, 256)
	const path = "/watch"
	mustOK(t, w.Add(path))
	oldGen := fakeOf(w).genFor(path)

	const n = 50
	for i := range n {
		mustBool(t, fakeOf(w).deliver(Event{Name: path, Op: Write}))
		_ = i
	}
	_, done, err := w.RemoveAndDrain(path)
	mustOK(t, err)
	mustOK(t, w.Add(path))
	newGen := fakeOf(w).genFor(path)
	for range n {
		mustBool(t, fakeOf(w).deliver(Event{Name: path, Op: Chmod}))
	}

	for i := range n {
		e := recvEvent(t, w)
		if !e.Has(Write) || e.Generation != oldGen {
			t.Fatalf("event %d = %+v, want write/old gen %d", i, e, oldGen)
		}
	}
	waitClosed(t, done, "after ordered drain")
	for i := range n {
		e := recvEvent(t, w)
		if !e.Has(Chmod) || e.Generation != newGen {
			t.Fatalf("event %d = %+v, want chmod/new gen %d", i, e, newGen)
		}
	}
}

// Two paths drained concurrently have independent signals and generations.
func TestRemoveAndDrainIndependentPaths(t *testing.T) {
	w := newFakeWatcher(t, 0)
	const (
		a = "/a"
		b = "/b"
	)
	mustOK(t, w.Add(a))
	mustOK(t, w.Add(b))
	genA := fakeOf(w).genFor(a)
	genB := fakeOf(w).genFor(b)
	if genA == genB {
		t.Fatal("two paths share a generation")
	}

	go fakeOf(w).deliver(Event{Name: a, Op: Write})
	time.Sleep(50 * time.Millisecond)

	g1, d1, err := w.RemoveAndDrain(a)
	mustOK(t, err)
	g2, d2, err := w.RemoveAndDrain(b)
	mustOK(t, err)
	if g1 != genA || g2 != genB {
		t.Fatalf("drained gens = %d,%d want %d,%d", g1, g2, genA, genB)
	}
	waitClosed(t, d2, "drain b")
	waitOpen(t, d1, "drain a still pending")

	e := recvEvent(t, w)
	if e.Name != a || e.Generation != genA {
		t.Fatalf("event = %+v, want a/gen %d", e, genA)
	}
	waitClosed(t, d1, "drain a")
}

func TestRemoveAndDrainClosedWatcher(t *testing.T) {
	w := newFakeWatcher(t, 16)
	mustOK(t, w.Close())
	gen, done, err := w.RemoveAndDrain("/anything")
	if err != nil {
		t.Fatalf("RemoveAndDrain after Close error = %v, want nil", err)
	}
	if gen != 0 {
		t.Fatalf("gen = %d, want 0", gen)
	}
	select {
	case <-done:
	default:
		t.Fatal("done should be closed for a closed watcher")
	}
}

func TestGenerationMonotonicAcrossReadds(t *testing.T) {
	w := newFakeWatcher(t, 4)
	const path = "/watch"
	var prev Generation
	for i := 0; i < 4; i++ {
		mustOK(t, w.Add(path))
		gen := fakeOf(w).genFor(path)
		if gen <= prev {
			t.Fatalf("iteration %d: gen %d not greater than %d", i, gen, prev)
		}
		_, done, err := w.RemoveAndDrain(path)
		mustOK(t, err)
		waitClosed(t, done, "drain")
		prev = gen
	}
}

// Repeatedly adding the same path without removing it is a no-op and must not
// allocate a new generation.
func TestRepeatedAddKeepsGeneration(t *testing.T) {
	w := newFakeWatcher(t, 4)
	const path = "/watch"
	mustOK(t, w.Add(path))
	first := fakeOf(w).genFor(path)
	mustOK(t, w.Add(path))
	mustOK(t, w.Add(path))
	if got := fakeOf(w).genFor(path); got != first {
		t.Fatalf("generation changed from %d to %d on a repeated Add", first, got)
	}
}

// Concurrent Add/RemoveAndDrain/Close must not trigger the race detector or
// deadlock.
func TestRemoveAndDrainConcurrent(t *testing.T) {
	for range 16 {
		w := newFakeWatcher(t, 64)
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 20 {
					p := "/watch"
					if err := w.Add(p); err == nil {
						_, done, err := w.RemoveAndDrain(p)
						if err == nil {
							<-done
						}
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			fakeOf(w).deliver(Event{Name: "/watch", Op: Write})
		}()
		wg.Wait()
		mustOK(t, w.Close())
	}
}

func recvEvent(t *testing.T, w *Watcher) Event {
	t.Helper()
	select {
	case e := <-w.Events:
		return e
	case err := <-w.Errors:
		t.Fatalf("unexpected error: %v", err)
		return Event{}
	case <-time.After(drainWait):
		t.Fatal("timed out waiting for event")
		return Event{}
	}
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustBool(t *testing.T, ok bool) {
	t.Helper()
	if !ok {
		t.Fatal("expected true")
	}
}
