// Path identity layer for fsnotify events.
//
// Every event delivered by a backend already carries a display string in
// Event.Name; that string is exactly what users see and it must never be
// rewritten. Some consumers (deduplication, state keyed by path, correlation
// of create/write/remove sequences) additionally need a stable *identity* for
// the thing an event refers to.
//
// The identity is derived purely lexically from:
//
//   - the set of watch roots (the paths added with Watcher.Add), and
//   - the event path as reported by the kernel.
//
// It never touches the filesystem: it does not resolve symlinks, evaluate
// ".." against real directories, or stat anything. Consequently an event for
// a path that has just been deleted – and no longer resolves – is normalized
// just as well as an event for an existing path.
//
// Root membership is checked on whole path segments: a watch on "a" is not a
// prefix of "ab", and events that lexically escape every root ("../x" or an
// absolute path belonging to no root) are rejected rather than guessed about.
//
// On case-insensitive platforms the same root may report a path under
// differently cased spellings; [CaseFolded] collapses those while the
// original spelling is preserved in [PathIdentity.Display]. The same
// relative path under two distinct roots is always a distinct identity.
package fsnotify

import (
	"errors"
	"runtime"
	"sort"
	"strings"
)

// ErrOutsideRoot is returned by [PathIndex.Identity] when an event path does
// not belong to any watch root: it lexically escapes the root with ".." or no
// root matches it on whole-segment boundaries.
var ErrOutsideRoot = errors.New("fsnotify: event path is outside of every watch root")

// PathCase selects how path segments are compared when deriving identities.
type PathCase uint8

const (
	// CaseSensitive compares segments byte for byte, as on Linux and the
	// BSDs. This is the zero value.
	CaseSensitive PathCase = iota

	// CaseFolded compares segments case-insensitively, matching the
	// per-component policy of Windows volumes.
	CaseFolded
)

// defaultPathCase reports the comparison policy of the running platform.
func defaultPathCase() PathCase {
	if runtime.GOOS == "windows" {
		return CaseFolded
	}
	return CaseSensitive
}

// PathIdentity is the explicit identity of an event path.
//
// Root is the watch root the event belongs to, Relative holds the path
// relative to that root as canonical segments ("" for the root itself), and
// Display is the event path exactly as it arrived, untouched.
type PathIdentity struct {
	root     string
	relative string
	display  string
	pathCase PathCase
}

// Root returns the watch root the event belongs to, using the spelling under
// which the root was registered.
func (p PathIdentity) Root() string { return p.root }

// Relative returns the canonical in-root path. Segments are joined with "/"
// (forward slashes on every platform); it is "" for an event on the root
// itself. Under [CaseFolded] the segments are lower-cased.
func (p PathIdentity) Relative() string { return p.relative }

// Display returns the original event path, exactly as presented by the
// backend. It is never modified.
func (p PathIdentity) Display() string { return p.display }

// IdentityKey is the comparable identity of an event path. Two events refer
// to the same watched object iff their keys compare equal.
//
// The fields are intentionally unexported: obtain keys with
// [PathIdentity.Key] and compare them with == (or use them as map keys).
type IdentityKey struct {
	root string // canonicalized root (folded when CaseFolded)
	rel  string // canonicalized in-root segments (folded when CaseFolded)
}

// Key returns the comparable identity. The same relative path under distinct
// roots yields distinct keys; under [CaseFolded] differing spellings of the
// same root and segments collapse to one key.
func (p PathIdentity) Key() IdentityKey {
	return IdentityKey{root: p.canonical(p.root), rel: p.relative}
}

// Same reports whether two identities refer to the same watched object.
func (p PathIdentity) Same(other PathIdentity) bool { return p.Key() == other.Key() }

func (p PathIdentity) canonical(segmented string) string {
	if p.pathCase == CaseFolded {
		return strings.ToLower(segmented)
	}
	return segmented
}

// PathIndex derives event identities from a fixed set of watch roots.
//
// It is a pure, snapshot value: building it from a watcher copies the watch
// list once, and neither [PathIndex] nor its methods inspect or modify any
// watcher state afterwards. No filesystem access is performed, so deleted or
// symlinked event paths are classified from their spelling alone.
type PathIndex struct {
	// roots sorted by descending segment count so the longest (most
	// specific) root wins for overlapping watches.
	roots    []identityRoot
	pathCase PathCase
}

type identityRoot struct {
	display  string // original spelling of the root, for PathIdentity.Root
	segments []string
}

// NewPathIndex builds a [PathIndex] from watch roots, using the comparison
// policy of the running platform. A root listed more than once (under
// different spellings in [CaseFolded]) is registered once.
func NewPathIndex(roots ...string) *PathIndex {
	return newPathIndex(defaultPathCase(), roots)
}

// NewPathIndexForPlatform builds a [PathIndex] with an explicit comparison
// policy. This is mainly useful for portable tests that need to exercise the
// Windows policy on other platforms.
func NewPathIndexForPlatform(pathCase PathCase, roots ...string) *PathIndex {
	return newPathIndex(pathCase, roots)
}

func newPathIndex(pathCase PathCase, roots []string) *PathIndex {
	idx := &PathIndex{pathCase: pathCase}
	seen := make(map[string]bool)
	for _, root := range roots {
		segs, ok := splitLexical(root)
		if !ok {
			continue
		}
		key := strings.Join(idx.fold(segs), "/")
		if seen[key] {
			continue
		}
		seen[key] = true
		idx.roots = append(idx.roots, identityRoot{display: root, segments: segs})
	}
	sort.SliceStable(idx.roots, func(i, j int) bool {
		return len(idx.roots[i].segments) > len(idx.roots[j].segments)
	})
	return idx
}

func (idx *PathIndex) fold(segs []string) []string {
	if idx.pathCase != CaseFolded {
		return segs
	}
	out := make([]string, len(segs))
	for i, s := range segs {
		out[i] = strings.ToLower(s)
	}
	return out
}

// watchLister is implemented by [Watcher]; any type exposing the currently
// watched roots can be snapshotted.
type watchLister interface {
	WatchList() []string
}

// NewPathIndexFromWatcher snapshots the watcher's current roots and builds an
// index from them. The watcher is only read; its state is never changed, and
// no filesystem resolution takes place.
func NewPathIndexFromWatcher(w watchLister) *PathIndex {
	return newPathIndex(defaultPathCase(), w.WatchList())
}

// Identity normalizes an event path against the watch roots.
//
// It works identically whether or not the path still exists on disk, so a
// Remove event for an already-deleted file is normalized without resolving
// anything. [ErrOutsideRoot] is returned, without side effects, for a path
// that is not contained in any root on whole-segment boundaries or that
// lexically escapes all roots.
func (idx *PathIndex) Identity(eventPath string) (PathIdentity, error) {
	segs, ok := splitLexical(eventPath)
	if !ok {
		return PathIdentity{}, ErrOutsideRoot
	}
	folded := idx.fold(segs)

	for _, root := range idx.roots {
		rf := idx.fold(root.segments)
		if !equalSegmentPrefix(folded, rf) {
			continue
		}
		rel := strings.Join(folded[len(rf):], "/")
		return PathIdentity{
			root:     root.display,
			relative: rel,
			display:  eventPath,
			pathCase: idx.pathCase,
		}, nil
	}
	return PathIdentity{}, ErrOutsideRoot
}

// Event normalizes an event's path, returning its identity. The event's Name
// (and any renamed-from path) is left untouched; the identity is supplied
// separately.
func (idx *PathIndex) Event(e Event) (PathIdentity, error) {
	return idx.Identity(e.Name)
}

// equalSegmentPrefix reports whether path starts with root on whole
// segments.
func equalSegmentPrefix(path, root []string) bool {
	if len(path) < len(root) {
		return false
	}
	for i := range root {
		if path[i] != root[i] {
			return false
		}
	}
	return true
}

// splitLexical breaks a path into cleaned lexical segments without
// consulting the filesystem. Both "/" and "\" are separators on every
// platform so that Windows-styled events can be classified anywhere.
//
// A Windows drive ("C:") or UNC volume ("\\server\share") is kept as the
// first, lower-cased segment. It returns ok=false if ".." escapes above the
// path's start or its volume – such a path has no lexical location inside
// any watch root.
func splitLexical(path string) (segs []string, ok bool) {
	if path == "" {
		return nil, false
	}

	var volume string
	rest := path
	if d := driveIndex(path); d > 0 {
		volume = strings.ToLower(strings.TrimRight(path[:d], `/\`))
		rest = path[d:]
	}

	cleaned := make([]string, 0, strings.Count(rest, "/")+strings.Count(rest, `\`)+2)
	if volume != "" {
		cleaned = append(cleaned, volume)
	}
	for _, seg := range strings.FieldsFunc(rest, isPathSeparator) {
		switch seg {
		case "", ".":
			continue
		case "..":
			// Ascend within the path, but never pop a volume and never
			// escape above the beginning of a relative path.
			n := len(cleaned)
			if n == 0 || (n == 1 && volume != "") {
				return nil, false
			}
			cleaned = cleaned[:n-1]
		default:
			cleaned = append(cleaned, seg)
		}
	}
	if len(cleaned) == 0 {
		return nil, false
	}
	return cleaned, true
}

func isPathSeparator(r rune) bool { return r == '/' || r == '\\' }

// driveIndex returns the byte index just past a Windows drive ("C:") or UNC
// ("\\server\share", "//server/share") prefix, or 0.
func driveIndex(path string) int {
	if len(path) >= 2 && path[1] == ':' &&
		((path[0] >= 'a' && path[0] <= 'z') || (path[0] >= 'A' && path[0] <= 'Z')) {
		return 2
	}
	if len(path) >= 2 && isPathSeparator(rune(path[0])) && isPathSeparator(rune(path[1])) {
		rest := path[2:]
		// Skip the server component.
		i := strings.IndexAny(rest, `/\`)
		if i < 0 {
			return 0
		}
		rest = rest[i+1:]
		// Skip the share component.
		j := strings.IndexAny(rest, `/\`)
		if j < 0 {
			return len(path)
		}
		return 2 + i + 1 + j
	}
	return 0
}
