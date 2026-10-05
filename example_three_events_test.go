package fsnotify

import (
	"fmt"
	"path/filepath"
)

// ExampleWatcher_threeEvents demonstrates the path identity layer with a fake
// watcher injecting three events: Create, Write, Remove. The display path is
// preserved while the identity is normalized.
func ExampleWatcher_threeEvents() {
	root := filepath.Join("/tmp", "example")
	fake := newFakeWatcher()
	if err := fake.Add(root); err != nil {
		fmt.Println("error:", err)
		return
	}

	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		fmt.Println("error:", err)
		return
	}

	// Inject three events through the fake watcher.
	go func() {
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Create}
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Write}
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Remove}
		fake.Close()
	}()

	// Collect and normalize events.
	for ev := range fake.Events {
		ne, err := resolver.normalizeEvent(ev)
		if err != nil {
			fmt.Println("reject:", err)
			continue
		}
		fmt.Printf("%s %s (identity: %s)\n", ne.Op, filepath.ToSlash(ne.Name), ne.identity.rel)
	}

	// Output:
	// CREATE /tmp/example/file.txt (identity: file.txt)
	// WRITE /tmp/example/file.txt (identity: file.txt)
	// REMOVE /tmp/example/file.txt (identity: file.txt)
}
