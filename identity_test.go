package fsnotify

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// fakeIdentityWatcher is a controllable, filesystem-free stand-in for a real
// Watcher. It only records the roots the caller pretends to have added and
// counts calls; it never delivers events or opens handles, so identity logic
// can be exercised deterministically on every platform.
type fakeIdentityWatcher struct {
	roots   []string
	added   int
	removed int
	listCnt int
}

func (f *fakeIdentityWatcher) WatchList() []string {
	f.listCnt++
	out := make([]string, len(f.roots))
	copy(out, f.roots)
	return out
}

func (f *fakeIdentityWatcher) add(root string) {
	f.roots = append(f.roots, root)
	f.added++
}

func (f *fakeIdentityWatcher) remove(root string) {
	for i, r := range f.roots {
		if r == root {
			f.roots = append(f.roots[:i], f.roots[i+1:]...)
			f.removed++
			return
		}
	}
}

func sortedRoots(roots []string) []string {
	out := append([]string(nil), roots...)
	sort.Strings(out)
	return out
}

// Root boundaries are whole segments: a watch on "a" must not share identity
// with anything under the sibling-looking "ab", while nested roots remain
// independent.
func TestPathIdentityRootBoundaries(t *testing.T) {
	idx := NewPathIndex("a", "ab")

	cases := []struct {
		path     string
		wantRoot string
		wantRel  string
	}{
		{"a", "a", ""},
		{"a/file", "a", "file"},
		{"a/b/file", "a", "b/file"},
		{"ab", "ab", ""},
		{"ab/file", "ab", "file"},
	}
	for _, c := range cases {
		id, err := idx.Identity(c.path)
		if err != nil {
			t.Fatalf("Identity(%q): %v", c.path, err)
		}
		if id.Root() != c.wantRoot || id.Relative() != c.wantRel {
			t.Errorf("Identity(%q) = root %q rel %q; want root %q rel %q",
				c.path, id.Root(), id.Relative(), c.wantRoot, c.wantRel)
		}
		if id.Display() != c.path {
			t.Errorf("Identity(%q) overwrote display path to %q", c.path, id.Display())
		}
	}

	a, _ := idx.Identity("a/file")
	ab, _ := idx.Identity("ab/file")
	if a.Key() == ab.Key() {
		t.Errorf("roots a and ab must not mix identity: %+v == %+v", a.Key(), ab.Key())
	}
	if a.Same(ab) {
		t.Errorf("a/file and ab/file reported as same")
	}

	// A nested, more specific root wins over its parent.
	nested := NewPathIndex("root", "root/sub")
	id, err := nested.Identity("root/sub/f")
	if err != nil {
		t.Fatal(err)
	}
	if id.Root() != "root/sub" || id.Relative() != "f" {
		t.Errorf("longest root not selected: %q %q", id.Root(), id.Relative())
	}
}

// Same relative path under distinct roots stays independent, even with
// trailing separators and "." components.
func TestPathIdentitySameNameDifferentRoots(t *testing.T) {
	idx := NewPathIndex("/w/one", "/w/two")
	one, err := idx.Identity("/w/one/x/y")
	if err != nil {
		t.Fatal(err)
	}
	two, err := idx.Identity("/w/two/./x/y")
	if err != nil {
		t.Fatal(err)
	}
	if one.Key() == two.Key() {
		t.Errorf("same relative path under different roots merged: %+v", one.Key())
	}
	if one.Relative() != "x/y" || two.Relative() != "x/y" {
		t.Errorf("relative segments not unified: %q %q", one.Relative(), two.Relative())
	}
}

// Windows per-component case policy folds differing spellings of the same
// root and segments, but only in CaseFolded mode.
func TestPathIdentityWindowsCasePolicy(t *testing.T) {
	idx := NewPathIndexForPlatform(CaseFolded, `C:\Watch`)

	upper, err := idx.Identity(`C:\WATCH\Sub\FILE.TXT`)
	if err != nil {
		t.Fatal(err)
	}
	lower, err := idx.Identity(`c:\watch\sub\file.txt`)
	if err != nil {
		t.Fatal(err)
	}
	mixed, err := idx.Identity(`C:/Watch/Sub/FILE.TXT`) // forward slashes too
	if err != nil {
		t.Fatal(err)
	}
	if upper.Key() != lower.Key() || lower.Key() != mixed.Key() {
		t.Errorf("case-folded spellings not merged: %+v %+v %+v",
			upper.Key(), lower.Key(), mixed.Key())
	}
	if !upper.Same(lower) {
		t.Errorf("Same() did not merge case-folded items")
	}
	if upper.Relative() != "sub/file.txt" {
		t.Errorf("relative not canonicalized/folded: %q", upper.Relative())
	}
	// The presentation spelling is preserved verbatim.
	if upper.Display() != `C:\WATCH\Sub\FILE.TXT` {
		t.Errorf("display path rewritten: %q", upper.Display())
	}

	// The same index under the sensitive policy keeps them apart.
	sens := NewPathIndexForPlatform(CaseSensitive, `C:\Watch`)
	if id, err := sens.Identity(`C:\WATCH\Sub`); !errors.Is(err, ErrOutsideRoot) {
		t.Errorf("sensitive policy should reject differing case, got %+v, %v", id, err)
	}

	// Duplicate root registered under a different spelling collapses to one
	// root and reports the first spelling.
	dup := NewPathIndexForPlatform(CaseFolded, `C:\Watch`, `c:\watch`)
	id, err := dup.Identity(`C:\watch\f`)
	if err != nil {
		t.Fatal(err)
	}
	if id.Root() != `C:\Watch` {
		t.Errorf("duplicate root did not keep first spelling: %q", id.Root())
	}
	if len(dup.roots) != 1 {
		t.Errorf("duplicate root registered twice: %d entries", len(dup.roots))
	}
}

// A removed path no longer resolves on disk, yet its event is normalized from
// the string alone. This exercises create/write/remove for a file that is
// never created.
func TestPathIdentityDeletedPath(t *testing.T) {
	root := filepath.Join(t.TempDir(), "gone-root") // deliberately not created
	idx := NewPathIndex(root)

	for _, op := range []Op{Create, Write, Remove} {
		e := Event{Name: filepath.Join(root, "deleted-file.tmp"), Op: op}
		id, err := idx.Event(e)
		if err != nil {
			t.Fatalf("%s event on deleted path: %v", op, err)
		}
		if id.Root() != root || id.Relative() != "deleted-file.tmp" {
			t.Errorf("%s: root=%q rel=%q", op, id.Root(), id.Relative())
		}
		if id.Display() != e.Name {
			t.Errorf("%s: display path overwritten", op)
		}
		if e.Name != filepath.Join(root, "deleted-file.tmp") {
			t.Errorf("%s: original event mutated", op)
		}
	}

	// Remove on the root itself, after it vanished: still classified.
	id, err := idx.Identity(root)
	if err != nil {
		t.Fatalf("remove of deleted root: %v", err)
	}
	if id.Relative() != "" {
		t.Errorf("root event should have empty relative path: %q", id.Relative())
	}
}

// Events that lexically escape or do not belong to any root are rejected with
// ErrOutsideRoot, and the index returns no partial identity.
func TestPathIdentityRejectsOutside(t *testing.T) {
	idx := NewPathIndex("root/sub")

	for _, bad := range []string{
		"",
		"root/other",
		"root/sub/../../etc/passwd",
		"../root/sub/f",
		"/elsewhere/f",
	} {
		id, err := idx.Identity(bad)
		if !errors.Is(err, ErrOutsideRoot) {
			t.Errorf("Identity(%q): err=%v, want ErrOutsideRoot", bad, err)
		}
		if id != (PathIdentity{}) {
			t.Errorf("Identity(%q) returned a partial identity: %+v", bad, id)
		}
	}

	// ".." that stays inside the root is cleaned lexically.
	idx2 := NewPathIndex("root")
	id, err := idx2.Identity("root/sub/../file")
	if err != nil {
		t.Fatalf("in-root .. rejected: %v", err)
	}
	if id.Relative() != "file" {
		t.Errorf("in-root .. not cleaned: %q", id.Relative())
	}
}

// The identity layer never resolves symlinks and never guesses the real
// target object: a symlink inside a root keeps the symlink's own identity.
func TestPathIdentityDoesNotFollowSymlinks(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "link")
	if err := os.WriteFile(target, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unsupported: %v", err)
	}

	idx := NewPathIndex(dir)
	idLink, err := idx.Identity(link)
	if err != nil {
		t.Fatal(err)
	}
	idTarget, err := idx.Identity(target)
	if err != nil {
		t.Fatal(err)
	}
	if idLink.Key() == idTarget.Key() {
		t.Errorf("symlink resolved to its target: %+v", idLink.Key())
	}
	if idLink.Relative() != "link" {
		t.Errorf("symlink relative path wrong: %q", idLink.Relative())
	}
}

// Fake watcher: building the index and classifying events snapshots the roots
// without mutating watcher state, and the classic create/write/remove triplet
// collapses to one identity (so a deleted file's Remove still correlates).
func TestPathIdentityWithFakeWatcher(t *testing.T) {
	fw := &fakeIdentityWatcher{}
	fw.add("root/a")
	fw.add("root/ab")

	before := sortedRoots(fw.WatchList())
	idx := NewPathIndexFromWatcher(fw)
	afterBuild := sortedRoots(fw.WatchList())

	file := "root/a/file.log"
	var wantKey IdentityKey
	for i, op := range []Op{Create, Write, Remove} {
		e := Event{Name: file, Op: op}
		id, err := idx.Event(e)
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if id.Root() != "root/a" || id.Relative() != "file.log" {
			t.Errorf("%s: root=%q rel=%q", op, id.Root(), id.Relative())
		}
		if i == 0 {
			wantKey = id.Key()
		} else if id.Key() != wantKey {
			t.Errorf("%s identity drifted from create: %+v != %+v", op, id.Key(), wantKey)
		}
		if id.Display() != file {
			t.Errorf("%s display path overwritten", op)
		}
	}

	// "ab" must not be mistaken for a child of "a".
	if id, err := idx.Identity("root/ab/other"); err != nil || id.Root() != "root/ab" {
		t.Errorf("root ab not distinguished: id=%+v err=%v", id, err)
	}

	after := sortedRoots(fw.WatchList())
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(before, afterBuild) {
		t.Errorf("watcher roots mutated: before=%v build=%v after=%v", before, afterBuild, after)
	}
	if fw.added != 2 || fw.removed != 0 {
		t.Errorf("watcher add/remove state changed: added=%d removed=%d", fw.added, fw.removed)
	}
	// Four snapshot reads so far: before, NewPathIndexFromWatcher, after
	// build, after classification – all read-only.
	if fw.listCnt != 4 {
		t.Errorf("unexpected WatchList call count: %d", fw.listCnt)
	}

	// Removing a root on the fake does not retroactively change the index:
	// it is a detached snapshot.
	fw.remove("root/a")
	if _, err := idx.Identity(file); err != nil {
		t.Errorf("snapshot changed when fake watcher mutated: %v", err)
	}
	if fw.removed != 1 {
		t.Errorf("expected one removal on fake, got %d", fw.removed)
	}
}

// A real watcher satisfies the same snapshot contract: Identity reads the
// live watch list once, classifies a not-yet-existing and an already-gone
// path, and never alters the watcher.
func TestWatcherIdentitySnapshot(t *testing.T) {
	dir := t.TempDir()
	w := newWatcher(t, dir)
	defer w.Close()

	before := sortedRoots(w.WatchList())
	for _, p := range []string{
		filepath.Join(dir, "never-existed.tmp"),
		filepath.Join(dir, "deeply", "nested", "gone.tmp"),
	} {
		id, err := w.Identity(p)
		if err != nil {
			t.Fatalf("Identity(%q): %v", p, err)
		}
		if id.Root() != dir || id.Display() != p {
			t.Errorf("Identity(%q) = root %q display %q", p, id.Root(), id.Display())
		}
	}
	if _, err := w.Identity(filepath.Join(t.TempDir(), "other")); !errors.Is(err, ErrOutsideRoot) {
		t.Fatalf("path in another directory not rejected")
	}
	after := sortedRoots(w.WatchList())
	if !reflect.DeepEqual(before, after) {
		t.Errorf("watcher state changed by Identity: before=%v after=%v", before, after)
	}
}
