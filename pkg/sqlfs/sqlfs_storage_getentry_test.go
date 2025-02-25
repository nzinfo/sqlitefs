package sqlfs

import (
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

func TestGetEntry_Root(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Patch LoadEntriesByParent to return a predefined entry for root
	patches := gomonkey.ApplyFunc((*storage).LoadEntriesByParent,
		func(_ *storage, parentID int64) *AsyncResult[[]fileInfo] {
			rs := NewAsyncResult[[]fileInfo]()
			rs.Complete([]fileInfo{
				{name: "file.txt", entryID: 2},
				{name: "dir", entryID: 3}}, nil)
			return rs
		})
	defer patches.Reset()

	entry, err := s.getEntry("/")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "/", entry.name)
}

func TestGetEntry_Success(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Patch LoadEntriesByParent to return the correct structure
	patches := gomonkey.ApplyFunc((*storage).LoadEntriesByParent,
		func(_ *storage, parentID int64) *AsyncResult[[]fileInfo] {
			var entries []fileInfo
			switch parentID {
			case 1: // Root directory
				entries = []fileInfo{
					{name: "path", entryID: 2, mode: os.ModeDir},
				}
			case 2: // path directory
				entries = []fileInfo{
					{name: "to", entryID: 3, mode: os.ModeDir},
				}
			case 3: // to directory
				entries = []fileInfo{
					{name: "file.txt", entryID: 4, mode: 0}, // 0 表示文件模式
				}
			default:
				entries = []fileInfo{}
			}

			rs := NewAsyncResult[[]fileInfo]()
			rs.Complete(entries, nil)
			return rs
		})
	defer patches.Reset()

	entry, err := s.getEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.name)
}

func TestGetEntry_NotFound(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	patches := gomonkey.ApplyFunc((*storage).LoadEntriesByParent,
		func(_ *storage, parentID int64) *AsyncResult[[]fileInfo] {
			rs := NewAsyncResult[[]fileInfo]()
			rs.Complete(nil, &ErrFileNotFound{Path: "/path/to/nonexistent.txt"})
			return rs
		})
	defer patches.Reset()

	entry, err := s.getEntry("/path/to/nonexistent.txt")
	assert.Error(t, err)
	var notFoundErr *ErrFileNotFound
	assert.ErrorAs(t, err, &notFoundErr)
	assert.Nil(t, entry)
}

func TestGetEntry_CacheHit(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-fill the cache
	s.entriesCache.Add("/path/to/file.txt", &fileInfo{name: "file.txt", entryID: 1})

	entry, err := s.getEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.name)
}
