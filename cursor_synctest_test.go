//go:build go1.25

package fsnotify

import (
	"testing"
	"testing/synctest"
)

// A consumer blocked in Next is woken when an event arrives, and Close wakes
// all waiters with ErrClosed. Uses the synctest bubble as a test barrier so
// no sleeps are needed.
func TestCursorNextBarrier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := make(chan Event)
		h := NewHub(&Watcher{Events: src}, 4)
		defer h.Close()
		c := h.Consumer()

		got := make(chan Event, 1)
		go func() {
			_, e, err := c.Next()
			if err != nil {
				t.Errorf("Next: %s", err)
				return
			}
			got <- e
		}()
		synctest.Wait() // blocked in Next

		src <- Event{Name: "file", Op: Create}
		synctest.Wait() // delivered to the waiter

		select {
		case e := <-got:
			if e.Name != "file" {
				t.Errorf("got %q, want %q", e.Name, "file")
			}
		default:
			t.Fatal("Next was not woken by the new event")
		}

		// Close wakes a blocked Next with ErrClosed.
		errc := make(chan error, 1)
		go func() {
			_, _, err := c.Next()
			errc <- err
		}()
		synctest.Wait() // blocked in Next again

		h.Close()
		synctest.Wait()

		select {
		case err := <-errc:
			if err != ErrClosed {
				t.Errorf("Next after Close: %v, want ErrClosed", err)
			}
		default:
			t.Fatal("Next was not woken by Close")
		}
	})
}

// Closing the underlying Watcher closes its Events channel, which closes the
// Hub and wakes all waiters.
func TestCursorWatcherCloseWakesWaiters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := make(chan Event)
		h := NewHub(&Watcher{Events: src}, 4)
		c := h.Consumer()

		errc := make(chan error, 1)
		go func() {
			_, _, err := c.Next()
			errc <- err
		}()
		synctest.Wait() // blocked in Next

		close(src) // watcher closed: Events channel closed
		synctest.Wait()

		select {
		case err := <-errc:
			if err != ErrClosed {
				t.Errorf("Next after watcher close: %v, want ErrClosed", err)
			}
		default:
			t.Fatal("Next was not woken by watcher close")
		}
	})
}
