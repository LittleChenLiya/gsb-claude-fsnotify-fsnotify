// Command watchscope-demo is a local demonstration of the fsnotify
// WatchScope layer (ScopeManager). It finishes in a few seconds and never
// runs longer than 90 seconds.
//
// It shows:
//
//  1. Two scopes sharing one native watch on the same directory.
//  2. Closing one scope while the other keeps receiving real events.
//  3. The native watch removed only after the last scope closes.
//  4. A slow subscription with a tiny buffer dropping events and getting
//     its own ErrEventGap notification, without blocking the others.
package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

func main() {
	// Hard cap: the whole demo must stay well below 90 seconds.
	done := time.AfterFunc(80*time.Second, func() {
		fmt.Println("demo: 80s time budget exceeded, aborting")
		os.Exit(1)
	})
	defer done.Stop()
	start := time.Now()

	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nDemo finished in %s (limit: 90s)\n", time.Since(start).Round(time.Millisecond))
}

func run() error {
	tmp, err := os.MkdirTemp("", "watchscope-demo")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	dir := filepath.Join(tmp, "watched")
	if err := os.Mkdir(dir, 0o755); err != nil {
		return err
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer w.Close()
	m := fsnotify.NewScopeManager(w)
	defer m.Close()

	// Two scopes on the same path (one written with a trailing separator to
	// show normalization), plus a slow scope with a tiny buffer.
	scopeA, err := m.Subscribe(dir, 64)
	if err != nil {
		return err
	}
	scopeB, err := m.Subscribe(dir+string(filepath.Separator), 64)
	if err != nil {
		return err
	}
	slow, err := m.Subscribe(dir, 1) // never read from: will lag behind
	if err != nil {
		return err
	}

	fmt.Printf("Watching %s\n", dir)
	fmt.Printf("Native watches with 3 scopes subscribed: %d\n", len(w.WatchList()))

	a := drain(scopeA)
	b := drain(scopeB)

	// Burst of real file modifications; too fast for the slow scope's
	// buffer of 1.
	fmt.Println("Creating 50 files ...")
	for i := 0; i < 50; i++ {
		name := filepath.Join(dir, fmt.Sprintf("file-%02d", i))
		if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
			return err
		}
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("scope A received %d events, scope B received %d events\n", a.count(), b.count())

	select {
	case err := <-slow.Errors:
		if errors.Is(err, fsnotify.ErrEventGap) {
			fmt.Println("slow scope: buffer full, got gap notification:", err)
		}
	case <-time.After(2 * time.Second):
		fmt.Println("slow scope: no gap notification (all events fit in the buffers)")
	}

	// Close scope A: scope B keeps working on the same native watch.
	fmt.Println("\nClosing scope A ...")
	if err := scopeA.Close(); err != nil {
		return err
	}
	fmt.Printf("Native watches after closing scope A: %d\n", len(w.WatchList()))
	name := filepath.Join(dir, "after-close")
	if err := os.WriteFile(name, []byte("x"), 0o644); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("scope B received %d more events after scope A closed\n", b.count())

	// Close the remaining scopes: the native watch goes away with the last
	// one.
	slow.Close()
	if err := scopeB.Close(); err != nil {
		return err
	}
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("Native watches after closing all scopes: %d\n", len(w.WatchList()))
	return nil
}

// drainer counts events from a subscription in the background.
type drainer struct{ n chan int }

func drain(s *fsnotify.Subscription) *drainer {
	d := &drainer{n: make(chan int, 1)}
	go func() {
		n := 0
		for range s.Events {
			n++
			select {
			case <-d.n:
			default:
			}
			d.n <- n
		}
	}()
	return d
}

func (d *drainer) count() int {
	select {
	case n := <-d.n:
		d.n <- n
		return n
	default:
		return 0
	}
}
