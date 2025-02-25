package sqlfs

import (
	"path/filepath"
	"testing"
)

func TestFilePathDirAndBase(t *testing.T) {
	tests := []struct {
		path         string
		expectedDir  string
		expectedBase string
	}{
		{"/home", "/", "home"},
		{"/", "/", "/"},
		{filepath.FromSlash("c:/"), filepath.FromSlash("."), filepath.FromSlash("c:")},
		{filepath.FromSlash("c:/home"), filepath.FromSlash("c:"), "home"},
		{"/home/user", "/home", "user"},
		{"/home/user/", "/home", "user"},
		{filepath.FromSlash("c:/home/user"), filepath.FromSlash("c:/home"), "user"},
		{filepath.FromSlash("c:/home/user/"), filepath.FromSlash("c:/home"), "user"},
	}

	for _, tt := range tests {
		path := filepath.Clean(filepath.FromSlash(tt.path))
		dir := filepath.Dir(path)
		base := filepath.Base(path)

		if dir != tt.expectedDir {
			t.Errorf("filepath.Dir(%q) = %q, want %q", path, dir, tt.expectedDir)
		}
		if base != tt.expectedBase {
			t.Errorf("filepath.Base(%q) = %q, want %q", path, base, tt.expectedBase)
		}
	}
}
