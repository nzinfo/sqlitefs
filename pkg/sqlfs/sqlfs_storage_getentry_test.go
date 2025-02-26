package sqlfs

import (
	"fmt"
	"io/fs"
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
		func(_ *storage, parentID EntryID, parentPath string) *AsyncResult[[]fileInfo] {
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
		func(_ *storage, parentID EntryID, parentPath string) *AsyncResult[[]fileInfo] {
			var entries []fileInfo
			switch parentID {
			case 1: // Root directory
				entries = []fileInfo{
					{name: "path", entryID: 2, mode: os.ModeDir, fullPath: "/path"},
				}
			case 2: // path directory
				entries = []fileInfo{
					{name: "to", entryID: 3, mode: os.ModeDir, fullPath: "/path/to"},
				}
			case 3: // to directory
				entries = []fileInfo{
					{name: "file.txt", entryID: 4, mode: 0, fullPath: "/path/to/file.txt"}, // 0 表示文件模式
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
	assert.Equal(t, "/path/to/file.txt", entry.fullPath) // 检查 fullPath
}

func TestGetEntry_NotFound(t *testing.T) {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	patches := gomonkey.ApplyFunc((*storage).LoadEntriesByParent,
		func(_ *storage, parentID EntryID, parentPath string) *AsyncResult[[]fileInfo] {
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

	// 模拟缓存命中
	s.entriesCache.Add("/path/to/file.txt", &fileInfo{
		name:     "file.txt",
		entryID:  4,
		mode:     0,
		fullPath: "/path/to/file.txt",
	})

	// 即使没有 mock LoadEntriesByParent，也应该能从缓存中获取
	entry, err := s.getEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.name)
	assert.Equal(t, EntryID(4), entry.entryID)
}

func TestGetEntry_WithDatabase(t *testing.T) {
	// 初始化测试存储
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// 使用 InitDatabase 初始化数据库
	err = InitDatabase(s.conn)
	if err != nil {
		t.Fatal(err)
	}

	mode_type := os.ModeDir & ^fs.ModePerm
	insertPathSQL := `INSERT INTO entries (entry_id, parent_id, name, mode_type, mode_perm, uid, gid) VALUES (2, 1, 'path', ` + fmt.Sprintf("%d", mode_type) + `, 0755, 0, 0);`

	// 插入子目录
	err = s.conn.Exec(insertPathSQL)
	if err != nil {
		t.Fatal(err)
	}
	insertPathSQL = `INSERT INTO entries (entry_id, parent_id, name, mode_type, mode_perm, uid, gid) VALUES (3, 2, 'to', ` + fmt.Sprintf("%d", mode_type) + `, 0755, 0, 0);`
	// 插入 to 目录
	err = s.conn.Exec(insertPathSQL)
	if err != nil {
		t.Fatal(err)
	}

	// 插入文件
	err = s.conn.Exec(`INSERT INTO entries (entry_id, parent_id, name, mode_type, mode_perm, uid, gid) VALUES (4, 3, 'file.txt', 0, 0644, 0, 0);`)
	if err != nil {
		t.Fatal(err)
	}

	// 测试获取文件
	entry, err := s.getEntry("/path/to/file.txt")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "file.txt", entry.name)
	assert.Equal(t, "/path/to/file.txt", entry.fullPath)
	assert.Equal(t, EntryID(4), entry.entryID)

	// 获取 /
	entry, err = s.getEntry("/")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "/", entry.fullPath)
	assert.Equal(t, os.ModeDir, os.ModeDir&entry.mode)

	// 获取不存在的文件
	{
		entry, err = s.getEntry("/path/to/nonexistent.txt")
		assert.Error(t, err)
		var notFoundErr *ErrFileNotFound
		assert.ErrorAs(t, err, &notFoundErr)
		assert.Nil(t, entry)
	}

	// 获取目录
	entry, err = s.getEntry("/path/to")
	assert.NoError(t, err)
	assert.NotNil(t, entry)
	assert.Equal(t, "to", entry.name)
	assert.Equal(t, "/path/to", entry.fullPath)

	// 获取不存在的目录
	{
		entry, err = s.getEntry("/path/to/nonexistent")
		assert.Error(t, err)
		var notFoundErr *ErrFileNotFound
		assert.ErrorAs(t, err, &notFoundErr)
		assert.Nil(t, entry)
	}
}
