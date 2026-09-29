package fsnotify

import (
	"path/filepath"
	"sync"
)

// shared sits between every backend and the user-facing channels. It owns the
// watch generations. Two goroutines cooperate:
//
//   - The dispatcher accepts events from the backend reader, tags them with
//     the generation active when they were accepted, and answers Add / Remove
//     bookkeeping. Acceptance never blocks on a slow consumer: accepted events
//     go onto an unbounded queue.
//   - The deliverer hands queued events to the Events channel in acceptance
//     order and reports each completed delivery back to the dispatcher, which
//     is when a pending RemoveAndDrain signal can complete.
//
// Splitting acceptance from delivery means RemoveAndDrain and Close stay
// responsive even while the deliverer is blocked because the consumer never
// reads.
type shared struct {
	Events chan Event
	Errors chan error

	closed chan struct{} // Closed when the watcher is shutting down.

	in     chan queuedEvent // Events accepted from the backend reader.
	add    chan addReq      // A successful Add allocates a new generation.
	drain  chan drainReq    // A RemoveAndDrain retires a generation.
	forget chan string      // A plain Remove clears path bookkeeping.
	finish chan queuedEvent // A delivered event, reported by the deliverer.
	notify chan struct{}    // Kicks the deliverer (capacity 1).
	next   Generation       // Next generation to allocate.

	closeOnce sync.Once

	queueMu sync.Mutex
	queue   []queuedEvent

	dispatchDone chan struct{}
	deliverDone  chan struct{}
}

type queuedEvent struct {
	event     Event
	gen       Generation
	delivered func()
}

type addReq struct {
	path string
	resp chan Generation
}

type drainReq struct {
	path string
	resp chan drainResp
}

type drainResp struct {
	gen  Generation
	done <-chan struct{}
}

type drainState struct {
	path string
	done chan struct{}
}

func newShared(ev chan Event, errs chan error) *shared {
	w := &shared{
		Events:       ev,
		Errors:       errs,
		closed:       make(chan struct{}),
		in:           make(chan queuedEvent),
		add:          make(chan addReq),
		drain:        make(chan drainReq),
		forget:       make(chan string),
		finish:       make(chan queuedEvent),
		notify:       make(chan struct{}, 1),
		next:         1,
		dispatchDone: make(chan struct{}),
		deliverDone:  make(chan struct{}),
	}
	go w.deliver()
	go w.dispatch()
	return w
}

// Returns true if the event was accepted, or false if watcher is closed. The
// generation is resolved from the event path at acceptance, so events that
// were already in flight when RemoveAndDrain ran keep the old generation.
func (w *shared) sendEvent(e Event) bool {
	return w.sendEventGen(e, 0, nil)
}

// sendEventGen accepts an event the backend already tagged with a generation.
// A generation of 0 resolves the generation from the event path. The optional
// delivered callback runs once the event has been handed to the consumer.
func (w *shared) sendEventGen(e Event, gen Generation, delivered func()) bool {
	if e.Op == 0 {
		return true
	}
	select {
	case w.in <- queuedEvent{event: e, gen: gen, delivered: delivered}:
		return true
	case <-w.closed:
		return false
	}
}

// Returns true if the error was sent, or false if watcher is closed.
func (w *shared) sendError(err error) bool {
	if err == nil {
		return true
	}
	select {
	case <-w.closed:
		return false
	case w.Errors <- err:
		return true
	}
}

func (w *shared) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}

// added records a successful Add and returns the freshly allocated generation
// for the path.
func (w *shared) added(path string) Generation {
	req := addReq{path: filepath.Clean(path), resp: make(chan Generation)}
	select {
	case w.add <- req:
	case <-w.closed:
		return 0
	}
	select {
	case gen := <-req.resp:
		return gen
	case <-w.closed:
		return 0
	}
}

// beginDrain retires the generation currently watching path: no events
// accepted afterwards resolve to it, and the returned channel closes once all
// events accepted for the generation have been delivered.
func (w *shared) beginDrain(path string) (Generation, <-chan struct{}) {
	req := drainReq{path: filepath.Clean(path), resp: make(chan drainResp)}
	select {
	case w.drain <- req:
	case <-w.closed:
		return 0, w.closed
	}
	select {
	case r := <-req.resp:
		return r.gen, r.done
	case <-w.closed:
		return 0, w.closed
	}
}

// removed notifies the dispatcher that the path was removed with a plain
// Remove (or removed implicitly), so a drain entry with no events in flight
// can be cleaned up.
func (w *shared) removed(path string) {
	select {
	case w.forget <- filepath.Clean(path):
	case <-w.closed:
	}
}

// close marks the watcher as closed; returns true if it was already closed.
// Closing unblocks the dispatcher and deliverer (even if the deliverer was
// stuck sending to a consumer that never reads) and completes any pending
// drain so callers never deadlock on it.
func (w *shared) close() bool {
	already := w.isClosed()
	w.closeOnce.Do(func() { close(w.closed) })
	return already
}

// waitDispatch blocks until neither goroutine touches the user-facing
// channels any more; backend readers call it before closing the channels.
func (w *shared) waitDispatch() {
	<-w.dispatchDone
	<-w.deliverDone
}

func closedChan() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// kick wakes the deliverer if it is idle.
func (w *shared) kick() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

func (w *shared) enqueue(q queuedEvent) {
	w.queueMu.Lock()
	w.queue = append(w.queue, q)
	w.queueMu.Unlock()
	w.kick()
}

func (w *shared) pop() (queuedEvent, bool) {
	w.queueMu.Lock()
	defer w.queueMu.Unlock()
	if len(w.queue) == 0 {
		return queuedEvent{}, false
	}
	q := w.queue[0]
	w.queue = w.queue[1:]
	return q, true
}

// deliver sends queued events to the consumer in order. It may block on the
// Events channel without holding up the dispatcher; Close unblocks it.
func (w *shared) deliver() {
	defer close(w.deliverDone)
	for {
		select {
		case <-w.closed:
			return
		case <-w.notify:
		}

		for {
			q, ok := w.pop()
			if !ok {
				break
			}
			q.event.Generation = q.gen
			select {
			case w.Events <- q.event:
			case <-w.closed:
				return
			}
			if q.delivered != nil {
				q.delivered()
			}
			// Hand the bookkeeping back to the dispatcher so drain
			// completion stays serialised with acceptance.
			select {
			case w.finish <- q:
			case <-w.closed:
				return
			}
		}
	}
}

// dispatch serialises all generation bookkeeping on one goroutine.
func (w *shared) dispatch() {
	defer close(w.dispatchDone)

	var (
		// Active generations per watched path; entries are moved to draining on
		// RemoveAndDrain.
		active = make(map[string]Generation)

		// Generations retired with RemoveAndDrain, keyed by generation.
		draining = make(map[Generation]drainState)

		// Events accepted but not yet delivered, per generation.
		inflight = make(map[Generation]int)
	)

	resolve := func(path string) Generation {
		path = filepath.Clean(path)
		if g, ok := active[path]; ok {
			return g
		}
		if g, ok := matchGeneration(active, path); ok {
			return g
		}
		if g, ok := drainingPath(draining, path); ok {
			return g
		}
		return 0
	}

	complete := func(gen Generation) {
		d, ok := draining[gen]
		if !ok {
			return
		}
		close(d.done)
		delete(draining, gen)
		delete(inflight, gen)
	}

	for {
		select {
		case <-w.closed:
			for _, d := range draining {
				close(d.done)
			}
			return

		case q := <-w.in:
			if q.gen == 0 {
				q.gen = resolve(q.event.Name)
			}
			if q.gen != 0 {
				inflight[q.gen]++
			}
			w.enqueue(q)

		case q := <-w.finish:
			if q.gen != 0 {
				inflight[q.gen]--
				if inflight[q.gen] == 0 {
					complete(q.gen)
				}
			}

		case req := <-w.add:
			gen := w.next
			w.next++
			active[req.path] = gen
			req.resp <- gen

		case req := <-w.drain:
			gen := active[req.path]
			delete(active, req.path)
			var done chan struct{}
			if gen != 0 {
				done = make(chan struct{})
				draining[gen] = drainState{path: req.path, done: done}
				if inflight[gen] == 0 {
					close(done)
					delete(draining, gen)
				}
			}
			// A path that isn't active may still have a pending drain (for
			// example RemoveAndDrain called twice); reuse that.
			if done == nil {
				if g, d, ok := findDrain(draining, req.path); ok {
					gen, done = g, d
				}
			}
			if done == nil {
				done = make(chan struct{})
				close(done)
			}
			req.resp <- drainResp{gen: gen, done: done}

		case path := <-w.forget:
			delete(active, path)
			if g, _, ok := findDrain(draining, path); ok && inflight[g] == 0 {
				complete(g)
			}
		}
	}
}

// matchGeneration finds the active generation of the longest matching watched
// directory prefix, so events for files inside a watched directory inherit the
// directory watch's generation.
func matchGeneration(active map[string]Generation, path string) (Generation, bool) {
	var (
		match string
		gen   Generation
	)
	for dir, g := range active {
		if (path == dir || hasPathPrefix(path, dir)) && len(dir) > len(match) {
			match, gen = dir, g
		}
	}
	return gen, match != ""
}

// drainingPath matches retired generations the same way matchGeneration does
// for active ones; an active match always takes precedence.
func drainingPath(draining map[Generation]drainState, path string) (Generation, bool) {
	var (
		match string
		gen   Generation
	)
	for g, d := range draining {
		if (path == d.path || hasPathPrefix(path, d.path)) && len(d.path) > len(match) {
			match, gen = d.path, g
		}
	}
	return gen, match != ""
}

// findDrain looks up the pending drain registered for an exact path.
func findDrain(draining map[Generation]drainState, path string) (Generation, chan struct{}, bool) {
	for g, d := range draining {
		if d.path == path {
			return g, d.done, true
		}
	}
	return 0, nil, false
}
