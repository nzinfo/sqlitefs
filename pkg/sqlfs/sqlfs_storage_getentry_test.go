package sqlfs

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/nzinfo/go-sqlfs/pkg/sqlfs/mock"
	"github.com/stretchr/testify/assert"
)

func TestGetEntry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock.NewMockStorageOps(ctrl)
	mockStorage.EXPECT().LoadEntriesByParent(gomock.Any()).Return(&AsyncResult[[]fileInfo]{Result: []fileInfo{{name: "file.txt", entryID: 1}}, Err: nil})

	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	s.loadEntriesByParent = mockStorage.LoadEntriesByParent

	entry, err := s.getEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.name)
}

func TestGetEntry_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock.NewMockStorageOps(ctrl)
	mockStorage.EXPECT().LoadEntriesByParent(gomock.Any()).Return(&AsyncResult[[]fileInfo]{Result: []fileInfo{}, Err: nil})

	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	s.loadEntriesByParent = mockStorage.LoadEntriesByParent

	entry, err := s.getEntry("/path/to/nonexistent.txt")
	assert.Error(t, err)
	assert.Nil(t, entry)
	assert.IsType(t, &ErrFileNotFound{}, err)
}

func TestGetEntry_CacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
