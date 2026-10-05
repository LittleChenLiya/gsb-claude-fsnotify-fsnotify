package fsnotify

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// ErrPathOutsideRoot is returned when an event path is not within any watched
// root. The event is rejected and no watcher state is modified.
var ErrPathOutsideRoot = errors.New("fsnotify: event path is outside the watched root")

// pathIdentity is a normalized representation of a path relative to a watched
// root. It provides a stable identity for paths within a root while preserving
// the original display path.
//
// The identity is computed purely from path segments – it does not resolve
// symlinks or require the path to exist. This means deleted paths and paths
// behind symlinks are handled without guessing the "real" object.
type pathIdentity struct {
	root    string // canonical root path (cleaned)
	rel     string // canonical relative path from root ("" for the root itself)
	display string // original path as given (preserved for display)
}

// identity returns the full canonical path (root + rel), normalized to
// forward slashes.
func (id pathIdentity) identity() string {
	root := filepath.ToSlash(id.root)
	if id.rel == "" {
		return root
	}
	return root + "/" + id.rel
}

// key returns the identity key for map lookups, applying the platform case
// policy. On Windows paths are case-insensitive, so "Foo" and "foo" share the
// same key.
func (id pathIdentity) key() string {
	return caseKey(id.identity())
}

// newPathIdentity creates a pathIdentity for the given path within root.
// It returns ErrPathOutsideRoot if path is not within root by complete
// segments (e.g. root "/a" does not match path "/ab").
//
// This function does NOT resolve symlinks or check if the path exists, so it
// works for deleted paths and paths behind symlinks.
func newPathIdentity(root, path string) (pathIdentity, error) {
	root = filepath.Clean(root)
	path = filepath.Clean(path)

	rel, err := filepath.Rel(root, path)
	if err != nil {
		return pathIdentity{}, err
	}

	// Reject paths that escape the root. filepath.Rel returns a path starting
	// with ".." when the target is outside the root.
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return pathIdentity{}, ErrPathOutsideRoot
	}

	// Normalize to forward slashes for a canonical, platform-independent form.
	rel = filepath.ToSlash(rel)
	if rel == "." {
		rel = ""
	}

	return pathIdentity{
		root:    root,
		rel:     rel,
		display: path,
	}, nil
}

// caseKey applies the platform case policy to a path string for identity
// comparison. On Windows paths are case-insensitive; on other platforms they
// are case-sensitive.
func caseKey(s string) string {
	if runtime.GOOS == "windows" {
		return strings.ToLower(s)
	}
	return s
}

// pathResolver resolves event paths to path identities within watched roots.
// It maintains a set of roots and resolves events to identities, rejecting
// paths that are outside all roots.
//
// The resolver is safe for concurrent use. It does not modify the watcher's
// internal state.
type pathResolver struct {
	mu    sync.RWMutex
	roots map[string]pathIdentity // keyed by caseKey(root)
}

// newPathResolver creates a new pathResolver.
func newPathResolver() *pathResolver {
	return &pathResolver{
		roots: make(map[string]pathIdentity),
	}
}

// addRoot adds a root to the resolver. If the root is already present, this is
// a no-op.
func (r *pathResolver) addRoot(root string) error {
	id, err := newPathIdentity(root, root)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roots[id.key()] = id
	return nil
}

// removeRoot removes a root from the resolver. If the root is not present,
// this is a no-op.
func (r *pathResolver) removeRoot(root string) {
	id, err := newPathIdentity(root, root)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.roots, id.key())
}

// resolve resolves a path to a path identity within one of the roots. If the
// path is within multiple roots, the most specific (longest) root is used.
// It returns ErrPathOutsideRoot if the path is not within any root.
func (r *pathResolver) resolve(path string) (pathIdentity, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var best pathIdentity
	found := false
	for _, root := range r.roots {
		id, err := newPathIdentity(root.root, path)
		if err != nil {
			continue
		}
		// The most specific root has the shortest relative path.
		if !found || len(id.rel) < len(best.rel) {
			best = id
			found = true
		}
	}
	if !found {
		return pathIdentity{}, ErrPathOutsideRoot
	}
	return best, nil
}

// normalizeEvent processes an event through the path identity layer. It
// resolves the event path to a path identity and stores it on the event,
// while preserving the original display path (Event.Name is not overwritten).
//
// It returns ErrPathOutsideRoot if the event path is not within any root.
func (r *pathResolver) normalizeEvent(e Event) (Event, error) {
	id, err := r.resolve(e.Name)
	if err != nil {
		return Event{}, err
	}
	e.identity = id
	return e, nil
}

// normalizeEvents processes multiple events through the path identity layer.
// Events outside all roots are rejected and skipped.
func (r *pathResolver) normalizeEvents(events []Event) []Event {
	out := make([]Event, 0, len(events))
	for _, e := range events {
		ne, err := r.normalizeEvent(e)
		if err != nil {
			continue
		}
		out = append(out, ne)
	}
	return out
}
