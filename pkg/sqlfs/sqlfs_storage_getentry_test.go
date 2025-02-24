package sqlfs_test

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
	"github.com/nzinfo/go-sqlfs/pkg/sqlfs/mock"
	"github.com/stretchr/testify/assert"
)

func TestGetEntry_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock.NewMockStorageOps(ctrl)
	mockStorage.EXPECT().LoadEntriesByParent(gomock.Any()).Return(&sqlfs.AsyncResult[[]sqlfs.FileInfo]{Result: []sqlfs.FileInfo{{Name: "file.txt", EntryID: 1}}, Err: nil})

	s, err := sqlfs.NewStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	s.LoadEntriesByParent = mockStorage.LoadEntriesByParent

	entry, err := s.GetEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.Name)
}

func TestGetEntry_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStorage := mock.NewMockStorageOps(ctrl)
	mockStorage.EXPECT().LoadEntriesByParent(gomock.Any()).Return(&sqlfs.AsyncResult[[]sqlfs.FileInfo]{Result: []sqlfs.FileInfo{}, Err: nil})

	s, err := sqlfs.NewStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	s.LoadEntriesByParent = mockStorage.LoadEntriesByParent

	entry, err := s.GetEntry("/path/to/nonexistent.txt")
	assert.Error(t, err)
	assert.Nil(t, entry)
	assert.IsType(t, &sqlfs.ErrFileNotFound{}, err)
}

func TestGetEntry_CacheHit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	s, err := sqlfs.NewStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Pre-fill the cache
	s.EntriesCache.Add("/path/to/file.txt", &sqlfs.FileInfo{Name: "file.txt", EntryID: 1})

	entry, err := s.GetEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.Name)
}
