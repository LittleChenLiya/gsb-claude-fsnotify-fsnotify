// Command watchscope-demo demonstrates the WatchScope layer: multiple
// independent scopes sharing a single fsnotify.Watcher.
//
// It creates a temporary directory and runs three scopes:
//
//   - scope A and scope B both watch the same directory (one shared native
//     watch, reference-counted);
//   - scope C has a tiny buffer and never reads, so it drops events and
//     reports ErrScopeGap without blocking anyone else.
//
// A writer goroutine modifies a file once per second. Halfway through, scope
// A is closed: the native watch stays (scope B still receives events) and is
// removed only when scope B closes at the end.
//
// By default the demo runs for 90 seconds:
//
//	go run ./cmd/watchscope-demo            # 90 seconds
//	go run ./cmd/watchscope-demo -duration 20s
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

var duration = flag.Duration("duration", 90*time.Second, "how long to run the demo")

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	tmp, err := os.MkdirTemp("", "watchscope-demo")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmp)
	file := filepath.Join(tmp, "watched-file")
	if err := os.WriteFile(file, []byte("start"), 0o644); err != nil {
		log.Fatal(err)
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()
	sw := fsnotify.NewScopeWatcher(w)

	scopeA := sw.NewScope(16)
	scopeB := sw.NewScope(16)
	scopeC := sw.NewScope(1) // Slow scope: tiny buffer, never drained.

	// Both scopes watch the same directory via different spellings; after
	// normalization this is a single native watch.
	if err := scopeA.Add(tmp); err != nil {
		log.Fatal(err)
	}
	if err := scopeB.Add(tmp + string(os.PathSeparator) + "."); err != nil {
		log.Fatal(err)
	}
	if err := scopeC.Add(tmp); err != nil {
		log.Fatal(err)
	}
	log.Printf("watching %s; WatchList=%q (one shared native watch)", tmp, sw.WatchList())

	// Scope A: print events until closed halfway through the demo.
	go func() {
		for e := range scopeA.Events {
			log.Printf("scope A event: %s", e)
		}
		log.Print("scope A: Events channel closed")
	}()
	// Scope B: print events for the whole run.
	go func() {
		for e := range scopeB.Events {
			log.Printf("scope B event: %s", e)
		}
		log.Print("scope B: Events channel closed")
	}()
	// Scope C: never read Events; only report gaps.
	go func() {
		for err := range scopeC.Errors {
			log.Printf("scope C error: %v", err)
		}
		log.Print("scope C: Errors channel closed")
	}()

	// Writer: modify the file once per second.
	stop := time.After(*duration)
	half := time.After(*duration / 2)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	n := 0
loop:
	for {
		select {
		case <-stop:
			break loop
		case <-half:
			log.Print("closing scope A; scope B keeps the shared native watch")
			scopeA.Close()
			log.Printf("WatchList after scope A close: %q", sw.WatchList())
		case <-tick.C:
			n++
			f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				log.Printf("write: %v", err)
				continue
			}
			fmt.Fprintf(f, "tick %d\n", n)
			f.Close()
		}
	}

	log.Print("closing remaining scopes; last close removes the native watch")
	scopeB.Close()
	scopeC.Close()
	sw.Close()
	log.Printf("WatchList at end: %q (empty)", sw.WatchList())
	fmt.Println("demo done")
}
