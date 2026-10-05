package fsnotify

import (
	"errors"
	"sync"
)

var (
	// ErrGap is delivered by Cursor.Next when the consumer fell too far
	// behind: the bounded buffer overflowed and events the consumer had not
	// yet confirmed were discarded to make room for new ones. This is always
	// reported explicitly; events are never dropped silently. After a gap the
	// cursor restarts at the oldest retained event.
	ErrGap = errors.New("fsnotify: consumer fell behind, gap in event stream")

	// ErrIllegalAck is returned by Cursor.Ack when the acknowledgement is
	// not valid: it either moves backwards or advances beyond the position
	// that was actually delivered to the consumer.
	ErrIllegalAck = errors.New("fsnotify: illegal acknowledgement")
)

// Hub fans the events of a single Watcher out to any number of consumers,
// each with its own independent cursor.
//
// Every event received from the underlying Watcher is assigned a monotonically
// increasing sequence number, starting at 1, and stored in a bounded buffer.
// Consumers read events in sequence order and confirm them with Ack; an
// event is released from the buffer only once every active consumer has
// confirmed it, so a fast consumer can never free events a slower consumer
// still needs.
//
// When the buffer is full and a new event arrives, the oldest event is
// evicted. Consumers that had not yet confirmed the evicted event receive
// ErrGap from their next Cursor.Next call and restart at the oldest retained
// event; consumers that did confirm it are unaffected. Data is never dropped
// silently.
//
// All cursor state lives in the Hub and its Cursors; the underlying Watcher
// is only read from and is otherwise untouched, so cursor bookkeeping never
// interferes with the OS watch loop.
//
// A Hub must be created with NewHub; do not create one directly.
type Hub struct {
	mu     sync.Mutex
	notify chan struct{} // closed and replaced on every state change, waking waiters
	done   chan struct{} // closed by Close to stop the pump
	source chan Event

	capacity  int
	log       []seqEvent // retained events; log[i].seq == base+1+i
	base      uint64     // all events with seq <= base have been released
	last      uint64     // last sequence number assigned
	consumers map[*Cursor]struct{}
	closed    bool
}

type seqEvent struct {
	seq uint64
	Event
}

// NewHub creates a Hub reading events from w, keeping at most capacity
// events in its buffer. capacity must be at least 1.
//
// Closing the Hub does not close the underlying Watcher; closing the
// Watcher (which closes its Events channel) closes the Hub.
func NewHub(w *Watcher, capacity int) *Hub {
	if capacity < 1 {
		panic("fsnotify: NewHub capacity must be at least 1")
	}
	h := &Hub{
		notify:    make(chan struct{}),
		done:      make(chan struct{}),
		source:    w.Events,
		capacity:  capacity,
		consumers: make(map[*Cursor]struct{}),
	}
	go h.pump()
	return h
}

// pump moves events from the source channel into the buffer. It is the only
// writer of the event log and keeps no state of its own beyond the Hub.
func (h *Hub) pump() {
	for {
		select {
		case e, ok := <-h.source:
			if !ok {
				h.Close()
				return
			}
			h.append(e)
		case <-h.done:
			return
		}
	}
}

func (h *Hub) append(e Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.last++
	for len(h.log) == h.capacity {
		h.evictOldest()
	}
	h.log = append(h.log, seqEvent{seq: h.last, Event: e})
	h.broadcast()
}

// evictOldest drops the oldest retained event to make room for a new one.
// Consumers that have not confirmed it are sent a gap and restart at the new
// oldest retained event; everyone else is unaffected.
func (h *Hub) evictOldest() {
	evict := h.log[0].seq
	for c := range h.consumers {
		if c.acked < evict {
			c.gap = true
			c.acked = evict
			c.next = 0 // rebase on the next Next call
		}
	}
	h.log = h.log[1:]
	h.base = evict
}

// recomputeBase releases events confirmed by every active consumer.
func (h *Hub) recomputeBase() {
	min := h.last
	for c := range h.consumers {
		if c.acked < min {
			min = c.acked
		}
	}
	for h.base < min {
		h.base++
		h.log = h.log[1:]
	}
}

func (h *Hub) broadcast() {
	close(h.notify)
	h.notify = make(chan struct{})
}

// Consumer registers a new consumer that only receives events arriving from
// now on (a "live tail"). It returns nil if the Hub is closed.
func (h *Hub) Consumer() *Cursor {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.consumerAt(h.last + 1)
}

// ConsumerFromStart registers a new consumer starting at the oldest event
// still retained in the buffer. It returns nil if the Hub is closed.
func (h *Hub) ConsumerFromStart() *Cursor {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.consumerAt(h.base + 1)
}

// consumerAt registers a consumer starting at sequence at; the caller must
// hold h.mu.
func (h *Hub) consumerAt(at uint64) *Cursor {
	if h.closed {
		return nil
	}
	c := &Cursor{hub: h, next: at, acked: at - 1}
	h.consumers[c] = struct{}{}
	return c
}

// Close closes the Hub, waking all consumers blocked in Cursor.Next with
// ErrClosed. It does not close the underlying Watcher.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.close()
}

func (h *Hub) close() {
	if h.closed {
		return
	}
	h.closed = true
	close(h.done)
	h.broadcast()
}

// Cursor is a single consumer's position in a Hub's event log.
//
// A cursor is confirmed with Ack; confirmations are monotonic and can only
// advance to positions already delivered by Next. Closing a cursor releases
// its hold on the buffer so its unconfirmed events can be freed for the
// remaining consumers.
type Cursor struct {
	hub *Hub

	next   uint64 // next sequence to deliver; 0 means rebase to base+1
	acked  uint64 // highest confirmed sequence
	gap    bool   // pending gap: next Next call returns ErrGap
	closed bool
}

// Next returns the next event and its sequence number, blocking until one
// is available. The sequence number is what Ack confirms.
//
// It returns ErrGap if the consumer fell behind and unconfirmed events were
// evicted; the cursor then restarts at the oldest retained event. It
// returns ErrClosed if the cursor or the Hub was closed. On error the
// returned sequence is 0.
func (c *Cursor) Next() (uint64, Event, error) {
	h := c.hub
	h.mu.Lock()
	for {
		if c.closed || h.closed {
			h.mu.Unlock()
			return 0, Event{}, ErrClosed
		}
		if c.gap {
			c.gap = false
			c.next = h.base + 1
			h.mu.Unlock()
			return 0, Event{}, ErrGap
		}
		if c.next == 0 || c.next <= h.base {
			// Events this cursor still needed were released before
			// delivery; report an explicit gap rather than skipping
			// them silently.
			c.next = h.base + 1
			h.mu.Unlock()
			return 0, Event{}, ErrGap
		}
		if c.next <= h.last {
			e := h.log[c.next-h.base-1]
			c.next++
			h.mu.Unlock()
			return e.seq, e.Event, nil
		}
		wait := h.notify
		h.mu.Unlock()
		<-wait
		h.mu.Lock()
	}
}

// Ack confirms all events up to and including sequence n.
//
// A confirmation can only move forward and only up to the position actually
// delivered by Next; anything else is rejected with ErrIllegalAck. Ack is
// idempotent for the current position.
func (c *Cursor) Ack(n uint64) error {
	h := c.hub
	h.mu.Lock()
	defer h.mu.Unlock()
	if c.closed {
		return ErrIllegalAck
	}
	delivered := c.acked
	if c.next > 0 {
		delivered = c.next - 1
	}
	if n > delivered || n < c.acked {
		return ErrIllegalAck
	}
	if n == c.acked {
		return nil
	}
	c.acked = n
	h.recomputeBase()
	h.broadcast()
	return nil
}

// Close removes the consumer from the Hub, releasing its hold on retained
// events. Pending and future Next calls return ErrClosed.
func (c *Cursor) Close() {
	h := c.hub
	h.mu.Lock()
	if !c.closed {
		c.closed = true
		delete(h.consumers, c)
		h.recomputeBase()
		h.broadcast()
	}
	h.mu.Unlock()
}
