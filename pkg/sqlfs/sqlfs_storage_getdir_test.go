package sqlfs

import (
	"os"
	"testing"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/stretchr/testify/assert"
)

// mockStorage implements a mock filesystem for testing
type mockStorage struct {
	storage
	mockEntries map[int64]fileInfo // entryID -> fileInfo mapping
	mockDirs    map[int64][]int64  // parentID -> []entryID mapping
}

func newMockStorage() *mockStorage {
	// Create mock filesystem structure:
	// /
	// ├── dir1
	// │   ├── file1.txt
	// │   └── subdir1
	// │       └── file2.txt
	// └── dir2
	//     └── file3.txt

	now := time.Now()
	ms := &mockStorage{
		storage: storage{
			dirCache: lru.New(100),
		},
		mockEntries: make(map[int64]fileInfo),
		mockDirs:    make(map[int64][]int64),
	}

	// Root directory (ID: 1, ParentID: 0)
	rootInfo := fileInfo{
		entryID:  1,
		parentID: 0,
		name:     "/",
		fileType: FileTypeDir,
		mode:     os.ModeDir | 0755,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[1] = rootInfo
	ms.mockDirs[0] = []int64{1} // Root's parent is 0

	// dir1 (ID: 2, ParentID: 1)
	dir1Info := fileInfo{
		entryID:  2,
		parentID: 1,
		name:     "dir1",
		fileType: FileTypeDir,
		mode:     os.ModeDir | 0755,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[2] = dir1Info
	ms.mockDirs[1] = append(ms.mockDirs[1], 2)

	// dir2 (ID: 3, ParentID: 1)
	dir2Info := fileInfo{
		entryID:  3,
		parentID: 1,
		name:     "dir2",
		fileType: FileTypeDir,
		mode:     os.ModeDir | 0755,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[3] = dir2Info
	ms.mockDirs[1] = append(ms.mockDirs[1], 3)

	// subdir1 (ID: 4, ParentID: 2)
	subdir1Info := fileInfo{
		entryID:  4,
		parentID: 2,
		name:     "subdir1",
		fileType: FileTypeDir,
		mode:     os.ModeDir | 0755,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[4] = subdir1Info
	ms.mockDirs[2] = append(ms.mockDirs[2], 4)

	// file1.txt (ID: 5, ParentID: 2)
	file1Info := fileInfo{
		entryID:  5,
		parentID: 2,
		name:     "file1.txt",
		fileType: FileTypeFile,
		mode:     0644,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[5] = file1Info
	ms.mockDirs[2] = append(ms.mockDirs[2], 5)

	// file2.txt (ID: 6, ParentID: 4)
	file2Info := fileInfo{
		entryID:  6,
		parentID: 4,
		name:     "file2.txt",
		fileType: FileTypeFile,
		mode:     0644,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[6] = file2Info
	ms.mockDirs[4] = append(ms.mockDirs[4], 6)

	// file3.txt (ID: 7, ParentID: 3)
	file3Info := fileInfo{
		entryID:  7,
		parentID: 3,
		name:     "file3.txt",
		fileType: FileTypeFile,
		mode:     0644,
		createAt: now,
		modTime:  now,
	}
	ms.mockEntries[7] = file3Info
	ms.mockDirs[3] = append(ms.mockDirs[3], 7)

	return ms
}

func (ms *mockStorage) loadEntry(entryID int64) (*fileInfo, error) {
	if info, ok := ms.mockEntries[entryID]; ok {
		return &info, nil
	}
	return nil, os.ErrNotExist
}

func (ms *mockStorage) loadEntriesByParentDir(parentID int64) ([]fileInfo, error) {
	if entryIDs, ok := ms.mockDirs[parentID]; ok {
		entries := make([]fileInfo, 0, len(entryIDs))
		for _, id := range entryIDs {
			if info, ok := ms.mockEntries[id]; ok {
				entries = append(entries, info)
			}
		}
		return entries, nil
	}
	return nil, os.ErrNotExist
}

func TestGetDir(t *testing.T) {
	tests := []struct {
		name          string
		path          string
		expectedFiles []string
		expectError   bool
	}{
		{
			name: "Root directory",
			path: "/",
			expectedFiles: []string{
				"dir1",
				"dir2",
			},
			expectError: false,
		},
		{
			name: "First level directory",
			path: "/dir1",
			expectedFiles: []string{
				"file1.txt",
				"subdir1",
			},
			expectError: false,
		},
		{
			name: "Nested directory",
			path: "/dir1/subdir1",
			expectedFiles: []string{
				"file2.txt",
			},
			expectError: false,
		},
		{
			name:          "Non-existent directory",
			path:          "/nonexistent",
			expectedFiles: nil,
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := newMockStorage()
			dir, err := ms.getDir(tt.path)

			if tt.expectError {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, dir)

			// Check if all expected files are present
			for _, expectedFile := range tt.expectedFiles {
				_, exists := dir.entries[expectedFile]
				assert.True(t, exists, "Expected file %s not found in directory", expectedFile)
			}

			// Check if no unexpected files are present
			assert.Equal(t, len(tt.expectedFiles), len(dir.entries),
				"Directory contains unexpected files")
		})
	}
}

func TestGetDirCaching(t *testing.T) {
	ms := newMockStorage()

	// First access should load from mock storage
	dir1, err := ms.getDir("/dir1")
	assert.NoError(t, err)
	assert.NotNil(t, dir1)

	// Modify the mock data to ensure subsequent access uses cache
	delete(ms.mockDirs[1], 0) // Remove dir1 from root
	
	// Second access should use cache
	dir2, err := ms.getDir("/dir1")
	assert.NoError(t, err)
	assert.NotNil(t, dir2)
	assert.Equal(t, dir1.entries, dir2.entries)
}
