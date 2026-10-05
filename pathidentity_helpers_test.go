package fsnotify

import (
	"os"
	"path/filepath"
)

// writeFile writes data to a file, creating it if it does not exist.
func writeFile(path string, data string) error {
	return os.WriteFile(path, []byte(data), 0o644)
}

// symlinkFile creates a symbolic link at link pointing to target.
func symlinkFile(target, link string) error {
	// Ensure the parent directory exists.
	if err := os.MkdirAll(filepath.Dir(link), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, link)
}
