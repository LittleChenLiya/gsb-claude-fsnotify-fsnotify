package fsnotify

import (
	"fmt"
	"sync"
)

// ErrGap is returned by Consumer.Next when one or more retained events were
// dropped because the cursor's bounded buffer overflowed. The consumer must
// restart from ToSeq; events in [FromSeq, ToSeq) are lost.
//
// This is an explicit, non-silent gap: the consumer knows exactly which
// sequences were lost and where to resume. A gap is delivered only to the
// consumers whose next delivery position was dropped; consumers that already
// received the dropped sequence are unaffected.
type ErrGap struct {
	FromSeq int64 // first lost sequence
	ToSeq   int64 // new oldest retained sequence (restart point)
}

func (e *ErrGap) Error() string {
	return fmt.Sprintf("fsnotify: event gap: sequences [%d, %d) dropped, restart at %d", e.FromSeq, e.ToSeq, e.ToSeq)
}

// Cursor is a multi-consumer cursor over fsnotify events with bounded
// retention.
//
// Events read from the underlying Watcher are assigned a monotonically
// increasing sequence number and retained in a bounded ring buffer. Each
// consumer tracks its own delivery and acknowledgement position. Events are
// retained until all consumers have acknowledged them (or the buffer
// overflows, in which case the slowest consumers receive an explicit ErrGap
// and must restart from the new oldest retained sequence).
//
// # Retention
//
// The ring buffer retains the most recent capacity events. A fast consumer
// acknowledging events does not release them: an event is only dropped when
// the buffer is full and a newer event arrives. When that happens, only the
// consumers whose next delivery position was dropped receive an ErrGap;
// consumers that already received the dropped sequence are unaffected.
//
// # State isolation
//
// The cursor state is entirely independent of the OS watcher. It consumes
// Watcher.Events and assigns its own sequence numbers, so OS-level event
// batching or rounds never affect cursor state. The underlying Watcher
// interface is not modified.
type Cursor struct {
	w        *Watcher
	capacity int

	mu   sync.Mutex
	cond *sync.Cond

	buf     []cursorEvent
	head    int64 // seq of buf[head%capacity] (oldest retained)
	count   int   // number of retained events
	nextSeq int64 // next seq to assign

	consumers map[*Consumer]struct{}

	closed bool
	done   chan struct{}
}

type cursorEvent struct {
	seq   int64
	event Event
}

// NewCursor creates a Cursor that consumes events from w and retains up to
// capacity events. A capacity <= 0 is treated as 1.
//
// The cursor does not take ownership of w: closing the cursor does not close
// the watcher, but closing the watcher (and thus its Events channel) closes
// the cursor and wakes any waiting consumers with ErrClosed.
func NewCursor(w *Watcher, capacity int) *Cursor {
	if capacity <= 0 {
		capacity = 1
	}
	c := &Cursor{
		w:         w,
		capacity:  capacity,
		buf:       make([]cursorEvent, capacity),
		head:      1, // first assigned sequence is 1
		nextSeq:   1,
		consumers: make(map[*Consumer]struct{}),
		done:      make(chan struct{}),
	}
	c.cond = sync.NewCond(&c.mu)
	go c.run()
	return c
}

func (c *Cursor) run() {
	for {
		select {
		case ev, ok := <-c.w.Events:
			if !ok {
				c.shutdown()
				return
			}
			c.append(ev)
		case <-c.done:
			return
		}
	}
}

func (c *Cursor) append(ev Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	seq := c.nextSeq
	c.nextSeq++
	c.buf[seq%int64(c.capacity)] = cursorEvent{seq: seq, event: ev}
	if c.count < c.capacity {
		c.count++
	} else {
		c.head++
	}
	c.cond.Broadcast()
}

func (c *Cursor) shutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
	c.cond.Broadcast()
}

// Close stops the cursor and wakes any consumers blocked in Next with
// ErrClosed. It does not close the underlying Watcher.
func (c *Cursor) Close() error {
	c.shutdown()
	return nil
}

// NewConsumer registers a new consumer. The consumer starts at the oldest
// retained event (catch-up), or at the next new event if nothing is retained.
func (c *Cursor) NewConsumer() *Consumer {
	c.mu.Lock()
	defer c.mu.Unlock()
	pos := c.head
	if c.count == 0 {
		pos = c.nextSeq
	}
	cons := &Consumer{c: c, pos: pos, deliveredLo: pos}
	c.consumers[cons] = struct{}{}
	return cons
}

// Consumer is a single consumer of a Cursor.
//
// A consumer tracks two positions: the next event to deliver (pos) and the
// last acknowledged event (ack). Acknowledgement is monotonic and may only
// advance to an event that was actually delivered.
type Consumer struct {
	c           *Cursor
	pos         int64 // next seq to deliver
	ack         int64 // last acked seq (0 = none)
	deliveredLo int64 // lowest seq in current contiguous delivered range
	lastGapHead int64 // head at which the last gap was reported (0 = none)
}

// Next returns the next event, blocking until one is available. It returns
// ErrClosed if the cursor is closed, or *ErrGap if retained events were
// dropped due to buffer overflow (the consumer must restart from ErrGap.ToSeq).
func (cons *Consumer) Next() (Event, int64, error) {
	c := cons.c
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.closed {
			return Event{}, 0, ErrClosed
		}
		// Gap check: only the laggard (the consumer with the lowest ack)
		// receives a gap when the buffer overflows. A consumer that has
		// never acked (ack==0) has no progress to lose, so no gap is
		// reported. Faster consumers that have acked more events are
		// unaffected even if their own ack is before head. The gap is
		// reported at most once per head position: after a gap, the
		// consumer restarts from head and is not notified again until
		// head moves further (another overflow).
		if cons.ack > 0 && cons.ack < c.head && cons.ack == c.minAck() && c.head > cons.lastGapHead {
			from := cons.ack + 1
			to := c.head
			cons.pos = c.head
			cons.deliveredLo = c.head
			cons.lastGapHead = c.head
			return Event{}, 0, &ErrGap{FromSeq: from, ToSeq: to}
		}
		// Available?
		if cons.pos <= c.head+int64(c.count)-1 {
			ev := c.buf[cons.pos%int64(c.capacity)].event
			seq := cons.pos
			cons.pos++
			return ev, seq, nil
		}
		c.cond.Wait()
	}
}

// minAck returns the lowest ack position among all consumers that have acked
// at least one event. It returns 0 if no consumer has acked.
//
// This is used to identify the laggard: the consumer whose progress is
// furthest behind. When the buffer overflows, only the laggard receives an
// ErrGap.
func (c *Cursor) minAck() int64 {
	var min int64
	for cons := range c.consumers {
		if cons.ack > 0 && (min == 0 || cons.ack < min) {
			min = cons.ack
		}
	}
	return min
}

// Ack acknowledges receipt of event seq. The sequence must have been delivered
// to this consumer and must advance the ack position (monotonic). It returns an
// error for illegal acks: seq not yet delivered, seq not greater than the last
// ack, or seq before the current delivered range (a gap).
func (cons *Consumer) Ack(seq int64) error {
	c := cons.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if seq <= 0 {
		return fmt.Errorf("fsnotify: ack %d is not a positive sequence", seq)
	}
	if seq <= cons.ack {
		return fmt.Errorf("fsnotify: ack %d does not advance past last ack %d", seq, cons.ack)
	}
	if seq >= cons.pos {
		return fmt.Errorf("fsnotify: ack %d was not delivered (next delivery is %d)", seq, cons.pos)
	}
	if seq < cons.deliveredLo {
		return fmt.Errorf("fsnotify: ack %d is before the current delivered range (start %d)", seq, cons.deliveredLo)
	}
	cons.ack = seq
	return nil
}

// Close unregisters the consumer, releasing its retention reference. Events
// are retained only while needed by at least one consumer; closing a consumer
// may allow older events to be released.
func (cons *Consumer) Close() error {
	c := cons.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.consumers[cons]; !ok {
		return nil
	}
	delete(c.consumers, cons)
	return nil
}
