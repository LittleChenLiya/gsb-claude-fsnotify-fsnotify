package fsnotify

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// fakeBackend is a test backend that delivers events from a channel. It lets
// tests drive the cursor deterministically without touching the OS.
type fakeBackend struct {
	events chan Event
	errors chan error
	done   chan struct{}
	once   sync.Once
}

func (b *fakeBackend) Add(string) error                    { return nil }
func (b *fakeBackend) AddWith(string, ...addOpt) error     { return nil }
func (b *fakeBackend) Remove(string) error                 { return nil }
func (b *fakeBackend) WatchList() []string                { return nil }
func (b *fakeBackend) xSupports(Op) bool                   { return false }
func (b *fakeBackend) Close() error {
	b.once.Do(func() {
		close(b.done)
		close(b.events)
	})
	return nil
}

// newFakeWatcher builds a Watcher backed by a fake backend. The returned
// channel is where tests send events to feed the cursor.
func newFakeWatcher(t *testing.T, buf int) (*Watcher, chan Event) {
	t.Helper()
	ev := make(chan Event, buf)
	errs := make(chan error)
	fb := &fakeBackend{events: ev, errors: errs, done: make(chan struct{})}
	w := &Watcher{b: fb, Events: ev, Errors: errs}
	return w, ev
}

func sendEvents(ev chan Event, seqs ...int64) {
	for _, s := range seqs {
		ev <- Event{Name: fmt.Sprintf("/file-%d", s), Op: Create}
	}
}

// recvOne reads one event from the consumer, failing the test on error.
func recvOne(t *testing.T, c *Consumer) (Event, int64) {
	t.Helper()
	ev, seq, err := c.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return ev, seq
}

// recvGap reads one gap from the consumer, failing the test if the error is
// not an ErrGap.
func recvGap(t *testing.T, c *Consumer) *ErrGap {
	t.Helper()
	_, _, err := c.Next()
	if err == nil {
		t.Fatalf("Next: expected gap, got event")
	}
	var g *ErrGap
	if !errors.As(err, &g) {
		t.Fatalf("Next: expected *ErrGap, got %T: %v", err, err)
	}
	return g
}

// waitFor blocks until fn returns true or the timeout expires.
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("waitFor: timed out")
}

func TestCursorBasic(t *testing.T) {
	w, ev := newFakeWatcher(t, 16)
	cur := NewCursor(w, 8)
	defer cur.Close()

	cons := cur.NewConsumer()

	sendEvents(ev, 1, 2, 3)

	for i := int64(1); i <= 3; i++ {
		got, seq := recvOne(t, cons)
		if seq != i {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
		if got.Name != fmt.Sprintf("/file-%d", i) {
			t.Fatalf("name = %q, want /file-%d", got.Name, i)
		}
	}
}

func TestCursorAck(t *testing.T) {
	w, ev := newFakeWatcher(t, 16)
	cur := NewCursor(w, 8)
	defer cur.Close()

	cons := cur.NewConsumer()

	sendEvents(ev, 1, 2, 3)

	// Acking before delivery is illegal.
	if err := cons.Ack(1); err == nil {
		t.Fatal("Ack(1) before delivery should be rejected")
	}

	recvOne(t, cons) // 1
	recvOne(t, cons) // 2

	// Ack 1 and 2.
	if err := cons.Ack(1); err != nil {
		t.Fatalf("Ack(1): %v", err)
	}
	if err := cons.Ack(2); err != nil {
		t.Fatalf("Ack(2): %v", err)
	}

	// Non-monotonic ack is illegal.
	if err := cons.Ack(1); err == nil {
		t.Fatal("Ack(1) after Ack(2) should be rejected")
	}
	// Acking an undelivered sequence is illegal.
	if err := cons.Ack(3); err == nil {
		t.Fatal("Ack(3) before delivery should be rejected")
	}

	recvOne(t, cons) // 3
	if err := cons.Ack(3); err != nil {
		t.Fatalf("Ack(3): %v", err)
	}
}

// TestCursorRetention: two consumers with ack positions 1 and 3 retain the
// events not yet jointly confirmed (i.e. from the slow consumer position).
func TestCursorRetention(t *testing.T) {
	w, ev := newFakeWatcher(t, 16)
	cur := NewCursor(w, 8)
	defer cur.Close()

	fast := cur.NewConsumer()
	slow := cur.NewConsumer()

	sendEvents(ev, 1, 2, 3, 4, 5)

	// Both receive all 5 events.
	for i := 0; i < 5; i++ {
		recvOne(t, fast)
		recvOne(t, slow)
	}

	// Fast acks 5, slow acks 1.
	if err := fast.Ack(5); err != nil {
		t.Fatalf("fast.Ack(5): %v", err)
	}
	if err := slow.Ack(1); err != nil {
		t.Fatalf("slow.Ack(1): %v", err)
	}

	// The cursor must retain events 2..5 (not jointly confirmed).
	// Send more events; the slow consumer should get them without a gap
	// because events 2..5 are retained.
	sendEvents(ev, 6, 7)

	// Slow receives 6 and 7 (no gap).
	_, seq6 := recvOne(t, slow)
	if seq6 != 6 {
		t.Fatalf("slow seq = %d, want 6 (no gap expected)", seq6)
	}
	_, seq7 := recvOne(t, slow)
	if seq7 != 7 {
		t.Fatalf("slow seq = %d, want 7", seq7)
	}

	// Fast receives 6 and 7.
	_, fseq6 := recvOne(t, fast)
	if fseq6 != 6 {
		t.Fatalf("fast seq = %d, want 6", fseq6)
	}
	_, fseq7 := recvOne(t, fast)
	if fseq7 != 7 {
		t.Fatalf("fast seq = %d, want 7", fseq7)
	}
}

// TestCursorGap: when the bounded buffer overflows, only the lagging consumer
// receives a gap; the fast consumer is unaffected.
func TestCursorGap(t *testing.T) {
	w, ev := newFakeWatcher(t, 16)
	cur := NewCursor(w, 5)
	defer cur.Close()

	fast := cur.NewConsumer()
	slow := cur.NewConsumer()

	// Fill the buffer with 5 events.
	sendEvents(ev, 1, 2, 3, 4, 5)

	// Both receive all 5.
	for i := 0; i < 5; i++ {
		recvOne(t, fast)
		recvOne(t, slow)
	}

	// Fast acks 5, slow acks 1.
	if err := fast.Ack(5); err != nil {
		t.Fatalf("fast.Ack(5): %v", err)
	}
	if err := slow.Ack(1); err != nil {
		t.Fatalf("slow.Ack(1): %v", err)
	}

	// Overflow: send 5 more events. The buffer drops 1..5 one by one.
	// The slow consumer (pos=2) will get a gap; the fast consumer
	// (pos=6) will not.
	sendEvents(ev, 6, 7, 8, 9, 10)

	// Fast receives 6..10 without a gap.
	for i := int64(6); i <= 10; i++ {
		_, seq := recvOne(t, fast)
		if seq != i {
			t.Fatalf("fast seq = %d, want %d", seq, i)
		}
	}

	// Slow receives a gap.
	g := recvGap(t, slow)
	if g.FromSeq != 2 {
		t.Fatalf("gap FromSeq = %d, want 2", g.FromSeq)
	}
	if g.ToSeq != 6 {
		t.Fatalf("gap ToSeq = %d, want 6", g.ToSeq)
	}

	// After the gap, slow restarts at 6 and receives 6..10.
	for i := int64(6); i <= 10; i++ {
		_, seq := recvOne(t, slow)
		if seq != i {
			t.Fatalf("slow seq = %d, want %d (after gap restart)", seq, i)
		}
	}
}

// TestCursorCloseWakesWaiters: closing the cursor wakes consumers blocked in
// Next with ErrClosed.
func TestCursorCloseWakesWaiters(t *testing.T) {
	w, _ := newFakeWatcher(t, 16)
	cur := NewCursor(w, 8)

	cons := cur.NewConsumer()

	done := make(chan error, 1)
	go func() {
		_, _, err := cons.Next()
		done <- err
	}()

	// Wait until the consumer is blocked in Next.
	waitFor(t, func() bool {
		cur.mu.Lock()
		defer cur.mu.Unlock()
		return cons.pos == 1 && !cur.closed
	})

	if err := cur.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Next err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next was not woken by Close")
	}
}

// TestCursorConsumerCloseReleases: closing a consumer releases its retention
// reference, allowing events to be released.
func TestCursorConsumerCloseReleases(t *testing.T) {
	w, ev := newFakeWatcher(t, 16)
	cur := NewCursor(w, 5)
	defer cur.Close()

	fast := cur.NewConsumer()
	slow := cur.NewConsumer()

	sendEvents(ev, 1, 2, 3, 4, 5)

	for i := 0; i < 5; i++ {
		recvOne(t, fast)
		recvOne(t, slow)
	}

	if err := fast.Ack(5); err != nil {
		t.Fatalf("fast.Ack(5): %v", err)
	}
	if err := slow.Ack(1); err != nil {
		t.Fatalf("slow.Ack(1): %v", err)
	}

	// Close the slow consumer, releasing its reference.
	if err := slow.Close(); err != nil {
		t.Fatalf("slow.Close: %v", err)
	}

	// Now only the fast consumer remains. Send more events; the fast
	// consumer should receive them without a gap (its reference is the
	// only one, and it has acked 5).
	sendEvents(ev, 6, 7)

	for i := int64(6); i <= 7; i++ {
		_, seq := recvOne(t, fast)
		if seq != i {
			t.Fatalf("fast seq = %d, want %d", seq, i)
		}
	}
}

// TestCursorWatcherClose: closing the watcher (and thus its Events channel)
// closes the cursor and wakes waiters.
func TestCursorWatcherClose(t *testing.T) {
	w, _ := newFakeWatcher(t, 16)
	cur := NewCursor(w, 8)

	cons := cur.NewConsumer()

	done := make(chan error, 1)
	go func() {
		_, _, err := cons.Next()
		done <- err
	}()

	// Wait until the consumer is blocked in Next.
	waitFor(t, func() bool {
		cur.mu.Lock()
		defer cur.mu.Unlock()
		return cons.pos == 1 && !cur.closed
	})

	if err := w.Close(); err != nil {
		t.Fatalf("w.Close: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Next err = %v, want ErrClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next was not woken by watcher close")
	}
}