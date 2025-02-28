package sqlfs

import (
	"bytes"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTempFile(t *testing.T) {
	sqlfs, fs, err := NewSQLiteFS(":memory:")
	require.NoError(t, err)
	defer sqlfs.Close()

	t.Run("create temp file", func(t *testing.T) {
		tempFile, err := fs.TempFile("", "test")
		require.NoError(t, err)
		defer tempFile.Close()

		// Verify temp file exists
		_, err = fs.Stat(tempFile.Name())
		require.NoError(t, err)

		// Write some data
		n, err := tempFile.Write([]byte("test data"))
		require.NoError(t, err)
		assert.Equal(t, 9, n)
	})

	t.Run("rename temp file", func(t *testing.T) {
		tempFile, err := fs.TempFile("", "test")
		require.NoError(t, err)
		defer tempFile.Close()

		// Write some data
		_, err = tempFile.Write([]byte("test data"))
		require.NoError(t, err)

		// Rename temp file using filesystem's Rename
		newName := filepath.Join(filepath.Dir(tempFile.Name()), "newfile")
		err = fs.Rename(tempFile.Name(), newName)
		require.NoError(t, err)

		// Verify old name doesn't exist
		_, err = fs.Stat(tempFile.Name())
		require.Error(t, err)

		// Verify new name exists
		info, err := fs.Stat(newName)
		require.NoError(t, err)
		assert.False(t, info.IsDir())

		// Verify content
		f, err := fs.Open(newName)
		require.NoError(t, err)
		defer f.Close()

		var buf bytes.Buffer
		_, err = io.Copy(&buf, f)
		require.NoError(t, err)
		assert.Equal(t, "test data", buf.String())
	})

	t.Run("temp file cleanup", func(t *testing.T) {
		tempFile, err := fs.TempFile("", "test")
		require.NoError(t, err)

		name := tempFile.Name()

		// Close and verify file still exists
		err = tempFile.Close()
		require.NoError(t, err)
		_, err = fs.Stat(name)
		require.NoError(t, err)

		// Remove using filesystem's Remove
		err = fs.Remove(name)
		require.NoError(t, err)
		_, err = fs.Stat(name)
		require.Error(t, err)
	})

	t.Run("temp file in directory", func(t *testing.T) {
		// Create a directory
		dir := "/testdir"
		err := fs.MkdirAll(dir, 0755)
		require.NoError(t, err)

		// Create temp file in directory
		tempFile, err := fs.TempFile(dir, "test")
		require.NoError(t, err)
		defer tempFile.Close()

		// Verify file is in the correct directory
		assert.Contains(t, tempFile.Name(), "testdir")
	})
}
