package fsnotify_test

import (
	"fmt"

	"github.com/fsnotify/fsnotify"
)

// ExamplePathIndex shows how the explicit path identity correlates the
// three events of a file's short life – create, write, remove – while the
// presentation path is left untouched. A Windows-style case policy is used
// so differently cased spellings of the same root merge, and two roots with
// the same suffix ("a" and "ab") stay separate.
func ExamplePathIndex() {
	// Pretend the watcher was created with Add("C:/Watch/a") and
	// Add("C:/Watch/ab"); no real watcher or filesystem is needed to
	// classify events.
	idx := fsnotify.NewPathIndexForPlatform(fsnotify.CaseFolded,
		`C:\Watch\a`, `C:\Watch\ab`)

	// The three events of one file, reported with varying casing and slash
	// direction – as can happen on Windows. Note the Remove arrives after
	// the file is gone.
	events := []fsnotify.Event{
		{Op: fsnotify.Create, Name: `C:\WATCH\a\report.log`},
		{Op: fsnotify.Write, Name: `c:/watch/a/report.log`},
		{Op: fsnotify.Remove, Name: `C:\Watch\A\REPORT.LOG`},
	}

	// Correlate by identity key, but always print the original path.
	keys := map[fsnotify.IdentityKey]int{}
	for _, e := range events {
		id, err := idx.Event(e)
		if err != nil {
			fmt.Printf("rejected: %v\n", err)
			continue
		}
		keys[id.Key()]++
		fmt.Printf("%-6s display=%-24q root=%q relative=%q\n",
			e.Op, id.Display(), id.Root(), id.Relative())
	}

	// All three are the same object; the adjacent root "ab" is unrelated.
	var key fsnotify.IdentityKey
	count := 0
	for k, n := range keys {
		key, count = k, n
	}
	fmt.Printf("distinct identities: %d; events for the file: %d\n", len(keys), count)
	if other, err := idx.Identity(`C:\Watch\ab\report.log`); err == nil {
		fmt.Printf("sibling root merges with a: %v\n", other.Key() == key)
	}

	// Output:
	// CREATE display="C:\\WATCH\\a\\report.log" root="C:\\Watch\\a" relative="report.log"
	// WRITE  display="c:/watch/a/report.log"  root="C:\\Watch\\a" relative="report.log"
	// REMOVE display="C:\\Watch\\A\\REPORT.LOG" root="C:\\Watch\\a" relative="report.log"
	// distinct identities: 1; events for the file: 3
	// sibling root merges with a: false
}
