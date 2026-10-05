package fsnotify

import (
	"errors"
	"testing"
	"time"
)

// newTestHub creates a Hub fed by a channel, so tests do not depend on any OS
// watcher.
func newTestHub(t *testing.T, capacity int) (*Hub, chan Event) {
	t.Helper()
	src := make(chan Event, 128)
	h := NewHub(&Watcher{Events: src}, capacity)
	t.Cleanup(h.Close)
	return h, src
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timeout waiting for condition")
}

func (h *Hub) state() (base, last uint64, buffered int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.base, h.last, len(h.log)
}

// feed sends events until the hub has assigned sequence upto (cumulative).
func feed(t *testing.T, h *Hub, src chan Event, upto uint64) {
	t.Helper()
	_, last, _ := h.state()
	for i := last + 1; i <= upto; i++ {
		src <- Event{Name: "event"}
	}
	waitFor(t, func() bool {
		_, last, _ := h.state()
		return last == upto
	})
}

func next(t *testing.T, c *Cursor) Event {
	t.Helper()
	_, e, err := c.Next()
	if err != nil {
		t.Fatalf("Next: %s", err)
	}
	return e
}

// Two consumers at positions 1 and 3: events not yet confirmed by both must
// be retained, and are released only once the slower consumer confirms
// them.
func TestCursorRetention(t *testing.T) {
	h, src := newTestHub(t, 100)
	c1 := h.ConsumerFromStart()
	c2 := h.ConsumerFromStart()

	feed(t, h, src, 4)

	next(t, c1)
	if err := c1.Ack(1); err != nil {
		t.Fatal(err)
	}
	next(t, c2)
	next(t, c2)
	next(t, c2)
	if err := c2.Ack(3); err != nil {
		t.Fatal(err)
	}

	// c1 is at 1, c2 at 3: events 2..4 are not confirmed by both and must
	// be retained.
	if base, _, n := h.state(); base != 1 || n != 3 {
		t.Fatalf("base=%d buffered=%d, want base=1 buffered=3", base, n)
	}

	// The slow consumer still gets the retained events.
	next(t, c1)
	next(t, c1)
	next(t, c1)
	if err := c1.Ack(4); err != nil {
		t.Fatal(err)
	}
	// c2 confirmed only 3, so event 4 is still retained for it.
	if base, _, n := h.state(); base != 3 || n != 1 {
		t.Fatalf("base=%d buffered=%d, want base=3 buffered=1", base, n)
	}
	next(t, c2)
	if err := c2.Ack(4); err != nil {
		t.Fatal(err)
	}
	if base, _, n := h.state(); base != 4 || n != 0 {
		t.Fatalf("base=%d buffered=%d, want base=4 buffered=0", base, n)
	}
}

// When the buffer overflows only the consumer that fell behind sees a gap;
// consumers that kept up are unaffected, and nothing is dropped silently.
func TestCursorGapOnlyForLaggard(t *testing.T) {
	h, src := newTestHub(t, 3)
	fast := h.ConsumerFromStart()
	slow := h.ConsumerFromStart()

	feed(t, h, src, 3)
	next(t, fast)
	next(t, fast)
	next(t, fast)
	if err := fast.Ack(3); err != nil {
		t.Fatal(err)
	}

	// Overflow the buffer; events 1..3 are evicted while slow still needs
	// them.
	feed(t, h, src, 6)

	if _, _, err := slow.Next(); !errors.Is(err, ErrGap) {
		t.Fatalf("slow Next: %v, want ErrGap", err)
	}
	// After the gap the slow consumer restarts at the oldest retained event.
	if _, _, err := slow.Next(); err != nil {
		t.Fatalf("slow Next after gap: %s", err)
	}
	if base, _, n := h.state(); base != 3 || n != 3 {
		t.Fatalf("base=%d buffered=%d, want base=3 buffered=3", base, n)
	}

	// The fast consumer never saw a gap and continues in sequence.
	if _, _, err := fast.Next(); err != nil {
		t.Fatalf("fast Next: %s", err)
	}
}

// Acks that move backwards or advance beyond the delivered position are
// rejected.
func TestCursorIllegalAck(t *testing.T) {
	h, src := newTestHub(t, 10)
	c := h.ConsumerFromStart()

	feed(t, h, src, 2)
	next(t, c)

	if err := c.Ack(2); !errors.Is(err, ErrIllegalAck) {
		t.Fatalf("Ack beyond delivered: %v, want ErrIllegalAck", err)
	}
	if err := c.Ack(1); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(1); err != nil { // idempotent
		t.Fatal(err)
	}
	next(t, c)
	if err := c.Ack(2); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(1); !errors.Is(err, ErrIllegalAck) {
		t.Fatalf("Ack moving backwards: %v, want ErrIllegalAck", err)
	}

	c.Close()
	if err := c.Ack(2); !errors.Is(err, ErrIllegalAck) {
		t.Fatalf("Ack on closed cursor: %v, want ErrIllegalAck", err)
	}
}

// A consumer that exits releases its hold on the buffer; its unconfirmed
// events no longer pin the log for the remaining consumers.
func TestCursorExitReleasesHold(t *testing.T) {
	h, src := newTestHub(t, 4)
	c1 := h.ConsumerFromStart()
	c2 := h.ConsumerFromStart()

	feed(t, h, src, 4)
	next(t, c1)
	if err := c1.Ack(1); err != nil {
		t.Fatal(err)
	}
	next(t, c2)
	next(t, c2)
	next(t, c2)
	if err := c2.Ack(3); err != nil {
		t.Fatal(err)
	}
	if base, _, _ := h.state(); base != 1 {
		t.Fatalf("base=%d, want 1", base)
	}

	// c2 exits: only c1's position pins the log now.
	c2.Close()
	if base, _, n := h.state(); base != 1 || n != 3 {
		t.Fatalf("base=%d buffered=%d, want base=1 buffered=3", base, n)
	}

	// Overflow now that c2 is gone: only c1 gets a gap.
	feed(t, h, src, 8)
	if _, _, err := c1.Next(); !errors.Is(err, ErrGap) {
		t.Fatalf("c1 Next: %v, want ErrGap", err)
	}
	if base, _, _ := h.state(); base != 4 {
		t.Fatalf("base=%d, want 4", base)
	}
}

// Closing the Hub wakes consumers blocked in Next with ErrClosed, and closing
// the underlying Watcher (closing its Events channel) closes the Hub.
func TestCursorCloseWakesWaiters(t *testing.T) {
	h, _ := newTestHub(t, 4)
	c := h.Consumer()

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Next()
		done <- err
	}()
	h.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Next: %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Next not woken by Close")
	}

	// Closing the watcher closes the hub.
	src := make(chan Event)
	w := &Watcher{Events: src}
	h2 := NewHub(w, 4)
	c2 := h2.Consumer()
	go func() { _, _, _ = c2.Next() }()
	close(src)
	waitFor(t, func() bool {
		h2.mu.Lock()
		defer h2.mu.Unlock()
		return h2.closed
	})
	if _, _, err := c2.Next(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after watcher close: %v, want ErrClosed", err)
	}
}

// A consumer registered with Consumer only sees events arriving after
// registration (live tail).
func TestCursorLiveTail(t *testing.T) {
	h, src := newTestHub(t, 10)
	feed(t, h, src, 2)
	c := h.Consumer()
	feed(t, h, src, 5)
	// Only the events after registration (3..5) are delivered.
	next(t, c)
	next(t, c)
	next(t, c)
	if err := c.Ack(5); err != nil {
		t.Fatal(err)
	}
}
