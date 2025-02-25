package sqlfs

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
)

func TestGetEntry_Success(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	patches := gomonkey.ApplyFunc((*storage).LoadEntriesByParent,
		func(_ *storage, parentID int64) *AsyncResult[[]fileInfo] {
			return &AsyncResult[[]fileInfo]{
				Result: []fileInfo{{name: "file.txt", entryID: 1}},
				Err:    nil,
			}
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
			return &AsyncResult[[]fileInfo]{
				Result: []fileInfo{},
				Err:    nil,
			}
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
