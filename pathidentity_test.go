package fsnotify

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// fakeWatcher is a test double for the backend interface. It allows injecting
// events directly into the Events channel without relying on the OS. It tracks
// its own watcher state so tests can verify that the path identity layer does
// not modify it.
type fakeWatcher struct {
	Events chan Event
	Errors chan error

	mu      sync.Mutex
	watches map[string]bool
	closed  bool
}

func newFakeWatcher() *fakeWatcher {
	return &fakeWatcher{
		Events:  make(chan Event, 64),
		Errors:  make(chan error, 16),
		watches: make(map[string]bool),
	}
}

func (f *fakeWatcher) Add(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrClosed
	}
	f.watches[filepath.Clean(path)] = true
	return nil
}

func (f *fakeWatcher) AddWith(path string, opts ...addOpt) error {
	return f.Add(path)
}

func (f *fakeWatcher) Remove(path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	path = filepath.Clean(path)
	if !f.watches[path] {
		return ErrNonExistentWatch
	}
	delete(f.watches, path)
	return nil
}

func (f *fakeWatcher) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.closed = true
	close(f.Events)
	close(f.Errors)
	return nil
}

func (f *fakeWatcher) WatchList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	list := make([]string, 0, len(f.watches))
	for p := range f.watches {
		list = append(list, p)
	}
	return list
}

func (f *fakeWatcher) xSupports(op Op) bool { return true }

// watcherState returns a snapshot of the fake watcher state.
func (f *fakeWatcher) watcherState() map[string]bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]bool, len(f.watches))
	for k, v := range f.watches {
		out[k] = v
	}
	return out
}

// TestPathIdentityRootBoundary verifies that roots a and ab do not mix
// identities. A path under root /a must not resolve to root /ab and vice
// versa.
func TestPathIdentityRootBoundary(t *testing.T) {
	rootA := filepath.Join(t.TempDir(), "a")
	rootAB := filepath.Join(t.TempDir(), "ab")

	idA, err := newPathIdentity(rootA, filepath.Join(rootA, "file.txt"))
	if err != nil {
		t.Fatalf("newPathIdentity(%q, %q): %s", rootA, filepath.Join(rootA, "file.txt"), err)
	}
	if idA.root != rootA {
		t.Errorf("idA.root = %q, want %q", idA.root, rootA)
	}
	if idA.rel != "file.txt" {
		t.Errorf("idA.rel = %q, want %q", idA.rel, "file.txt")
	}

	idAB, err := newPathIdentity(rootAB, filepath.Join(rootAB, "file.txt"))
	if err != nil {
		t.Fatalf("newPathIdentity(%q, %q): %s", rootAB, filepath.Join(rootAB, "file.txt"), err)
	}
	if idAB.root != rootAB {
		t.Errorf("idAB.root = %q, want %q", idAB.root, rootAB)
	}
	if idAB.rel != "file.txt" {
		t.Errorf("idAB.rel = %q, want %q", idAB.rel, "file.txt")
	}

	if idA.key() == idAB.key() {
		t.Errorf("idA.key() == idAB.key(): %q and %q should be distinct", idA.key(), idAB.key())
	}

	_, err = newPathIdentity(rootAB, filepath.Join(rootA, "file.txt"))
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Errorf("newPathIdentity(%q, %q) error = %v, want ErrPathOutsideRoot", rootAB, filepath.Join(rootA, "file.txt"), err)
	}

	_, err = newPathIdentity(rootA, filepath.Join(rootAB, "file.txt"))
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Errorf("newPathIdentity(%q, %q) error = %v, want ErrPathOutsideRoot", rootA, filepath.Join(rootAB, "file.txt"), err)
	}

	idRoot, err := newPathIdentity(rootA, rootA)
	if err != nil {
		t.Fatalf("newPathIdentity(%q, %q): %s", rootA, rootA, err)
	}
	if idRoot.rel != "" {
		t.Errorf("idRoot.rel = %q, want empty", idRoot.rel)
	}
	if idRoot.identity() != filepath.ToSlash(rootA) {
		t.Errorf("idRoot.identity() = %q, want %q", idRoot.identity(), filepath.ToSlash(rootA))
	}
}

// TestPathIdentityCasePolicy verifies the platform case policy. On Windows,
// paths that differ only in case share the same identity. On other platforms
// they are distinct.
func TestPathIdentityCasePolicy(t *testing.T) {
	root := t.TempDir()

	id1, err := newPathIdentity(root, filepath.Join(root, "File.TXT"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := newPathIdentity(root, filepath.Join(root, "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	id3, err := newPathIdentity(root, filepath.Join(root, "FILE.TXT"))
	if err != nil {
		t.Fatal(err)
	}

	if runtime.GOOS == "windows" {
		if id1.key() != id2.key() || id2.key() != id3.key() {
			t.Errorf("Windows case policy: keys should all be equal, got %q, %q, %q", id1.key(), id2.key(), id3.key())
		}
	} else {
		if id1.key() == id2.key() {
			t.Errorf("non-Windows case policy: keys should differ, got %q and %q", id1.key(), id2.key())
		}
	}

	if id1.display != filepath.Join(root, "File.TXT") {
		t.Errorf("id1.display = %q, want %q", id1.display, filepath.Join(root, "File.TXT"))
	}
}

// TestPathIdentityDeletedPath verifies that deleted paths can still be
// normalized. The identity is computed from path segments, not from the
// filesystem, so a path that no longer exists is handled correctly.
func TestPathIdentityDeletedPath(t *testing.T) {
	root := t.TempDir()
	deleted := filepath.Join(root, "deleted_file.txt")

	if _, err := filepath.Glob(deleted); err != nil {
		t.Fatal(err)
	}

	id, err := newPathIdentity(root, deleted)
	if err != nil {
		t.Fatalf("newPathIdentity(%q, %q) for deleted path: %s", root, deleted, err)
	}
	if id.rel != "deleted_file.txt" {
		t.Errorf("id.rel = %q, want %q", id.rel, "deleted_file.txt")
	}
	if id.display != deleted {
		t.Errorf("id.display = %q, want %q", id.display, deleted)
	}
	if id.identity() != filepath.ToSlash(filepath.Join(root, "deleted_file.txt")) {
		t.Errorf("id.identity() = %q, want %q", id.identity(), filepath.ToSlash(filepath.Join(root, "deleted_file.txt")))
	}
}

// TestPathIdentityNoSymlinkResolve verifies that the identity layer does not
// follow symlinks. A symlink path keeps its own identity rather than being
// resolved to the target.
func TestPathIdentityNoSymlinkResolve(t *testing.T) {
	root := t.TempDir()
	link := filepath.Join(root, "link")
	target := filepath.Join(root, "target")

	if err := writeFile(target, "data"); err != nil {
		t.Fatal(err)
	}
	if err := symlinkFile(target, link); err != nil {
		t.Skipf("symlinks not supported: %s", err)
	}

	idLink, err := newPathIdentity(root, link)
	if err != nil {
		t.Fatal(err)
	}
	idTarget, err := newPathIdentity(root, target)
	if err != nil {
		t.Fatal(err)
	}

	if idLink.key() == idTarget.key() {
		t.Errorf("symlink identity should not resolve to target: %q == %q", idLink.key(), idTarget.key())
	}
	if idLink.rel != "link" {
		t.Errorf("idLink.rel = %q, want %q", idLink.rel, "link")
	}
}

// TestPathIdentitySameNameDifferentRoot verifies that the same name under
// different roots produces independent identities.
func TestPathIdentitySameNameDifferentRoot(t *testing.T) {
	root1 := filepath.Join(t.TempDir(), "root1")
	root2 := filepath.Join(t.TempDir(), "root2")

	id1, err := newPathIdentity(root1, filepath.Join(root1, "same.txt"))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := newPathIdentity(root2, filepath.Join(root2, "same.txt"))
	if err != nil {
		t.Fatal(err)
	}

	if id1.key() == id2.key() {
		t.Errorf("same name under different roots should be independent: %q == %q", id1.key(), id2.key())
	}
	if id1.root != root1 || id2.root != root2 {
		t.Errorf("roots not preserved: %q, %q", id1.root, id2.root)
	}
}

// TestPathResolverNormalize verifies that the path resolver normalizes events
// and preserves the display path.
func TestPathResolverNormalize(t *testing.T) {
	root := t.TempDir()
	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}

	ev := Event{Name: filepath.Join(root, "file.txt"), Op: Create}
	norm, err := resolver.normalizeEvent(ev)
	if err != nil {
		t.Fatalf("normalizeEvent: %s", err)
	}
	if norm.Name != ev.Name {
		t.Errorf("display path overwritten: got %q, want %q", norm.Name, ev.Name)
	}
	if norm.identity.rel != "file.txt" {
		t.Errorf("identity.rel = %q, want %q", norm.identity.rel, "file.txt")
	}
	if norm.identity.display != ev.Name {
		t.Errorf("identity.display = %q, want %q", norm.identity.display, ev.Name)
	}

	outside := Event{Name: filepath.Join(t.TempDir(), "outside.txt"), Op: Create}
	_, err = resolver.normalizeEvent(outside)
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Errorf("normalizeEvent outside root error = %v, want ErrPathOutsideRoot", err)
	}
}

// TestPathResolverMostSpecificRoot verifies that when a path is within
// multiple roots, the most specific root is used.
func TestPathResolverMostSpecificRoot(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "sub")
	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}
	if err := resolver.addRoot(sub); err != nil {
		t.Fatal(err)
	}

	ev := Event{Name: filepath.Join(sub, "file.txt"), Op: Create}
	norm, err := resolver.normalizeEvent(ev)
	if err != nil {
		t.Fatal(err)
	}
	if norm.identity.root != sub {
		t.Errorf("identity.root = %q, want %q (most specific)", norm.identity.root, sub)
	}
	if norm.identity.rel != "file.txt" {
		t.Errorf("identity.rel = %q, want %q", norm.identity.rel, "file.txt")
	}
}

// TestPathResolverDoesNotModifyWatcher verifies that processing events through
// the path identity layer does not modify the watcher internal state.
func TestPathResolverDoesNotModifyWatcher(t *testing.T) {
	root := t.TempDir()
	fake := newFakeWatcher()
	if err := fake.Add(root); err != nil {
		t.Fatal(err)
	}

	before := fake.watcherState()

	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}

	events := []Event{
		{Name: filepath.Join(root, "a.txt"), Op: Create},
		{Name: filepath.Join(root, "b.txt"), Op: Write},
		{Name: filepath.Join(root, "c.txt"), Op: Remove},
		{Name: filepath.Join(t.TempDir(), "outside.txt"), Op: Create},
	}
	normalized := resolver.normalizeEvents(events)

	after := fake.watcherState()

	if len(before) != len(after) {
		t.Errorf("watcher state changed: before %d entries, after %d", len(before), len(after))
	}
	for k, v := range before {
		if after[k] != v {
			t.Errorf("watcher state changed for %q: before %v, after %v", k, v, after[k])
		}
	}

	if len(normalized) != 3 {
		t.Errorf("normalized %d events, want 3 (outside event rejected)", len(normalized))
	}
}

// TestFakeWatcherThreeEvents demonstrates the path identity layer with a fake
// watcher injecting three events: Create, Write, Remove. This is the
// three-event example test.
func TestFakeWatcherThreeEvents(t *testing.T) {
	root := t.TempDir()
	fake := newFakeWatcher()
	if err := fake.Add(root); err != nil {
		t.Fatal(err)
	}

	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}

	go func() {
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Create}
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Write}
		fake.Events <- Event{Name: filepath.Join(root, "file.txt"), Op: Remove}
		fake.Close()
	}()

	var normalized []Event
	for ev := range fake.Events {
		ne, err := resolver.normalizeEvent(ev)
		if err != nil {
			t.Errorf("normalizeEvent(%q): %s", ev.Name, err)
			continue
		}
		normalized = append(normalized, ne)
	}

	if len(normalized) != 3 {
		t.Fatalf("got %d events, want 3", len(normalized))
	}

	wantOps := []Op{Create, Write, Remove}
	for i, want := range wantOps {
		if normalized[i].Op != want {
			t.Errorf("event %d: Op = %v, want %v", i, normalized[i].Op, want)
		}
	}

	wantDisplay := filepath.Join(root, "file.txt")
	for i, ev := range normalized {
		if ev.Name != wantDisplay {
			t.Errorf("event %d: display path = %q, want %q", i, ev.Name, wantDisplay)
		}
		if ev.identity.rel != "file.txt" {
			t.Errorf("event %d: identity.rel = %q, want %q", i, ev.identity.rel, "file.txt")
		}
	}

	idKey := normalized[0].identity.key()
	for i := 1; i < len(normalized); i++ {
		if normalized[i].identity.key() != idKey {
			t.Errorf("event %d: identity key = %q, want %q (same file)", i, normalized[i].identity.key(), idKey)
		}
	}
}

// TestPathIdentityRootBoundaryBySegments verifies that the root boundary is
// determined by complete path segments, not string prefix. Root /a must not
// match /ab as a string prefix.
func TestPathIdentityRootBoundaryBySegments(t *testing.T) {
	root := filepath.Join(t.TempDir(), "a")

	cases := []struct {
		path string
		want bool
	}{
		{filepath.Join(root, "file.txt"), true},
		{filepath.Join(root, "sub", "file.txt"), true},
		{filepath.Join(root, "..", "ab", "file.txt"), false},
		{filepath.Join(root, "..", "other"), false},
	}

	for _, tc := range cases {
		_, err := newPathIdentity(root, tc.path)
		got := err == nil
		if got != tc.want {
			t.Errorf("newPathIdentity(%q, %q) success = %v, want %v (err: %v)", root, tc.path, got, tc.want, err)
		}
	}

	ab := root + "b"
	_, err := newPathIdentity(root, ab)
	if !errors.Is(err, ErrPathOutsideRoot) {
		t.Errorf("root %q must not match %q by string prefix: err = %v, want ErrPathOutsideRoot", root, ab, err)
	}
}

// TestPathIdentityDisplayPathNotOverwritten verifies that the display path is
// never overwritten by the identity layer, even when the path is normalized.
func TestPathIdentityDisplayPathNotOverwritten(t *testing.T) {
	root := t.TempDir()
	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}

	display := filepath.Join(root, "SubDir", "File.TXT")
	ev := Event{Name: display, Op: Create}
	norm, err := resolver.normalizeEvent(ev)
	if err != nil {
		t.Fatal(err)
	}

	if norm.Name != display {
		t.Errorf("display path overwritten: got %q, want %q", norm.Name, display)
	}
	if norm.identity.display != display {
		t.Errorf("identity.display = %q, want %q", norm.identity.display, display)
	}

	if runtime.GOOS == "windows" {
		if strings.Contains(norm.identity.rel, "\\") {
			t.Errorf("identity.rel = %q, want forward slashes", norm.identity.rel)
		}
	}
}

// TestPathResolverRemoveRoot verifies that removing a root from the resolver
// causes subsequent events under that root to be rejected.
func TestPathResolverRemoveRoot(t *testing.T) {
	root := t.TempDir()
	resolver := newPathResolver()
	if err := resolver.addRoot(root); err != nil {
		t.Fatal(err)
	}

	// Event within root is accepted.
	ev := Event{Name: filepath.Join(root, "file.txt"), Op: Create}
	if _, err := resolver.normalizeEvent(ev); err != nil {
		t.Fatalf("normalizeEvent before remove: %s", err)
	}

	// Remove the root.
	resolver.removeRoot(root)

	// Event within root is now rejected.
	if _, err := resolver.normalizeEvent(ev); !errors.Is(err, ErrPathOutsideRoot) {
		t.Errorf("normalizeEvent after remove error = %v, want ErrPathOutsideRoot", err)
	}
}
