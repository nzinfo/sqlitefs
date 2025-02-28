package sqlfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMkdirAll(t *testing.T) {
	// Create a new SQLiteFS instance with an in-memory database
	sqlfs, fs, err := NewSQLiteFS(":memory:")
	require.NoError(t, err)
	defer sqlfs.Close()

	tests := []struct {
		name    string
		path    string
		perm    os.FileMode
		wantErr bool
	}{
		{
			name:    "create single directory",
			path:    "/testdir",
			perm:    0755,
			wantErr: false,
		},
		{
			name:    "create nested directory",
			path:    "/nested/dir/structure",
			perm:    0755,
			wantErr: false,
		},
		{
			name:    "create directory with existing parent",
			path:    "/nested/dir/another",
			perm:    0755,
			wantErr: false,
		},
		{
			name:    "create directory with file in path",
			path:    "/testfile/dir",
			perm:    0755,
			wantErr: true,
		},
	}

	// Create a test file to test the error case
	_, err = fs.Create("/testfile")
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := fs.MkdirAll(tt.path, tt.perm)
			
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			
			assert.NoError(t, err)
			if err != nil {
				return // Skip further checks if there was an error
			}

			// Verify the directory exists
			info, err := fs.Stat(tt.path)
			assert.NoError(t, err)
			if err != nil {
				return // Skip further checks if there was an error
			}
			assert.True(t, info.IsDir())

			// If it's a nested directory, verify all parent directories exist
			if filepath.Dir(tt.path) != "/" {
				parent := filepath.Dir(tt.path)
				for parent != "/" {
					info, err := fs.Stat(parent)
					assert.NoError(t, err)
					if err != nil {
						break // Stop checking parents if one doesn't exist
					}
					assert.True(t, info.IsDir())
					parent = filepath.Dir(parent)
				}
			}
		})
	}
}

func TestMkdirAllWithExistingDirectories(t *testing.T) {
	// Create a new SQLiteFS instance with an in-memory database
	sqlfs, fs, err := NewSQLiteFS(":memory:")
	require.NoError(t, err)
	defer sqlfs.Close()

	// First create a directory structure
	err = fs.MkdirAll("/a/b/c", 0755)
	require.NoError(t, err)

	// Now try to create a directory that shares part of the path
	err = fs.MkdirAll("/a/b/d", 0755)
	assert.NoError(t, err)

	// Verify both directories exist
	info, err := fs.Stat("/a/b/c")
	assert.NoError(t, err)
	assert.True(t, info.IsDir())

	info, err = fs.Stat("/a/b/d")
	assert.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestMkdirAllIdempotent(t *testing.T) {
	// Create a new SQLiteFS instance with an in-memory database
	sqlfs, fs, err := NewSQLiteFS(":memory:")
	require.NoError(t, err)
	defer sqlfs.Close()

	// Create a directory
	err = fs.MkdirAll("/test/dir", 0755)
	require.NoError(t, err)

	// Try to create it again
	err = fs.MkdirAll("/test/dir", 0755)
	assert.NoError(t, err, "MkdirAll should be idempotent")

	// Verify the directory exists
	info, err := fs.Stat("/test/dir")
	assert.NoError(t, err)
	assert.True(t, info.IsDir())
}
