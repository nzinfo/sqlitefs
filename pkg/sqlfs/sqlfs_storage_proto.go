////// //go:build proto

package sqlfs

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/ncruces/go-sqlite3"
)

func (s *storage) Has(path string) bool {
	path = clean(path)

	entry, err := s.getEntry(path)
	if err != nil {
		// TODO: log err ?
		return false
	}
	return entry != nil
}

func (s *storage) New(path string, mode fs.FileMode, flag int) (*fileInfo, error) {

	path = clean(path)

	if s.Has(path) {
		if !s.MustGet(path).mode.IsDir() {
			return nil, fmt.Errorf("file already exists %q", path)
		}
		return nil, nil
	}

	// Get the parent path components that need to be created
	var dirsToCreate []string
	current := filepath.Dir(path)
	var currentParentID EntryID = 1 // Default to root

	for {
		current = clean(current)
		if current == string(separator) {
			currentParentID = 1 // root
			break
		}

		entry, err := s.getEntry(current)
		if err == nil {
			// Found an existing entry, check if it's a directory
			if entry.mode&os.ModeDir == 0 {
				return nil, fmt.Errorf("parent path component %q exists but is not a directory", current)
			}
			currentParentID = entry.entryID
			break
		}
		dirsToCreate = append([]string{current}, dirsToCreate...)
		current = filepath.Dir(current)
	}

	// 代码中不启用事务
	// fmt.Println("dirsToCreate:", dirsToCreate)

	// 新建文件，需要修改。
	_, err := func() (*AsyncResult[WriteResult], error) {
		// fmt.Println("[Lock]New")
		s.connMutex.Lock()
		defer s.connMutex.Unlock()

		// Prepare statement for both directory and file creation
		stmt, _, err := s.conn.Prepare(`
		INSERT INTO entries (
			entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, size, create_at, modify_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
	`)
		if err != nil {
			return nil, fmt.Errorf("prepare statement error: %v", err)
		}
		defer stmt.Close()

		// Create parent directories if needed
		for _, dirPath := range dirsToCreate {
			s.maxEntryID++ // Increment for each new directory
			dirName := filepath.Base(dirPath)

			if err := stmt.BindInt64(1, int64(s.maxEntryID)); err != nil {
				return nil, fmt.Errorf("bind entry_id error: %v", err)
			}
			if err := stmt.BindInt64(2, int64(currentParentID)); err != nil {
				return nil, fmt.Errorf("bind parent_id error: %v", err)
			}
			if err := stmt.BindText(3, dirName); err != nil {
				return nil, fmt.Errorf("bind name error: %v", err)
			}
			if err := stmt.BindInt64(4, int64(os.ModeDir)); err != nil {
				return nil, fmt.Errorf("bind mode_type error: %v", err)
			}
			if err := stmt.BindInt64(5, int64(mode.Perm())); err != nil { // FIXME: 应继承权限，当前版本暂不考虑
				return nil, fmt.Errorf("bind mode_perm error: %v", err)
			}
			if err := stmt.BindInt64(6, 0); err != nil {
				return nil, fmt.Errorf("bind uid error: %v", err)
			}
			if err := stmt.BindInt64(7, 0); err != nil {
				return nil, fmt.Errorf("bind gid error: %v", err)
			}
			if err := stmt.BindText(8, ""); err != nil {
				return nil, fmt.Errorf("bind target error: %v", err)
			}
			if err := stmt.BindInt64(9, 0); err != nil { // 目录大小默认为 0
				return nil, fmt.Errorf("bind size error: %v", err)
			}

			stmt.Step()
			if stmt.Err() != nil {
				return nil, fmt.Errorf("execute directory creation error: %v", stmt.Err())
			}
			// 需要清空父目录的缓存
			s.dirsCache.Remove(currentParentID)

			currentParentID = s.maxEntryID
			stmt.Reset()
		}

		// Create the new file using the same statement
		s.maxEntryID++
		name := filepath.Base(path)

		if err := stmt.BindInt64(1, int64(s.maxEntryID)); err != nil {
			return nil, fmt.Errorf("bind entry_id error: %v", err)
		}
		if err := stmt.BindInt64(2, int64(currentParentID)); err != nil {
			return nil, fmt.Errorf("bind parent_id error: %v", err)
		}
		if err := stmt.BindText(3, name); err != nil {
			return nil, fmt.Errorf("bind name error: %v", err)
		}
		if err := stmt.BindInt64(4, int64(mode.Type())); err != nil { // mode&os.ModeType  不一定是常规文件
			return nil, fmt.Errorf("bind mode_type error: %v", err)
		}
		if err := stmt.BindInt64(5, int64(mode.Perm())); err != nil {
			return nil, fmt.Errorf("bind mode_perm error: %v", err)
		}
		if err := stmt.BindInt64(6, 0); err != nil {
			return nil, fmt.Errorf("bind uid error: %v", err)
		}
		if err := stmt.BindInt64(7, 0); err != nil {
			return nil, fmt.Errorf("bind gid error: %v", err)
		}
		if err := stmt.BindNull(8); err != nil { // NULL for regular file
			return nil, fmt.Errorf("bind target error: %v", err)
		}
		if err := stmt.BindInt64(9, 0); err != nil { // 新建大小为 0
			return nil, fmt.Errorf("bind size error: %v", err)
		}

		// fmt.Println("create file:", s.maxEntryID, path, name)
		if err := stmt.Exec(); err != nil {
			return nil, err
		}
		return nil, nil
	}()

	if err != nil {
		return nil, err
	}

	// 需要清除 path 对应记录的 parent id 的 cache
	s.entriesCache.Remove(path)
	s.dirsCache.Remove(currentParentID)
	// fmt.Println("ReloadEntry:", path, currentParentID)
	return s.getEntry(path) // 从数据库中再次加载
}

/*
func (s *storage) createParent(path string, mode fs.FileMode, f *file) error {
	base := filepath.Dir(path)
	base = clean(base)
	if s.Has(base) {
		return nil
	}

	if base == string(separator) {
		return nil
	}

	return s.createParent(base, mode.Perm()|os.ModeDir, f)
}
*/

func (s *storage) Children(path string) (*[]fileInfo, error) {
	path = clean(path)

	entry, err := s.getEntry(path)

	if !entry.mode.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", path)
	}

	entriesResult := s.LoadEntriesByParent(entry.entryID, path)
	entries, err := entriesResult.Wait()
	if err != nil {
		// FIXME: log the error.
		return nil, err
	}
	return &entries, nil
}

func (s *storage) MustGet(path string) *fileInfo {
	f, ok := s.Get(path)
	if !ok {
		panic(fmt.Sprintf("couldn't find %q", path))
	}

	return f
}

func (s *storage) Get(path string) (*fileInfo, bool) {
	path = clean(path)
	entry, err := s.getEntry(path)
	if err != nil {
		return nil, false
	}
	if entry == nil {
		return nil, false
	}
	return entry, true
}

func (s *storage) Rename(from, to string) error {
	// fmt.Println("Rename====", from, to)
	from = clean(from)
	to = clean(to)

	// 1. 确认 from 存在
	fromEntry, err := s.getEntry(from)
	if err != nil {
		return fmt.Errorf("source path %q does not exist", from)
	}

	// 2. 尝试获取 to
	toEntry, err := s.getEntry(to)
	if err == nil {
		// to 存在
		if toEntry.mode&os.ModeDir != 0 {
			// to 是目录，将 from 移动到这个目录下
			to = filepath.Join(to, filepath.Base(from))
		} else {
			// to 是文件，报错
			return fmt.Errorf("destination path %q already exists and is not a directory", to)
		}
	}

	// 3. 获取 to 的父目录
	toParentPath := filepath.Dir(to)
	var toParentID EntryID
	if toParentPath == string(separator) {
		toParentID = 1 // root
	} else {
		toParentEntry, err := s.getEntry(toParentPath)
		if err != nil {
			return fmt.Errorf("destination parent directory %q does not exist", toParentPath)
		}
		if toParentEntry.mode&os.ModeDir == 0 {
			return fmt.Errorf("destination parent path %q exists but is not a directory", toParentPath)
		}
		toParentID = toParentEntry.entryID
	}

	// 4. 修改 from 的 parent ID 和 name
	// 获取锁
	// fmt.Println("[Lock]Rename")
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	stmt, _, err := s.conn.Prepare(`
		UPDATE entries 
		SET parent_id = ?, name = ?, modify_at = CURRENT_TIMESTAMP 
		WHERE entry_id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare rename statement error: %v", err)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(toParentID)); err != nil {
		return fmt.Errorf("bind parent_id error: %v", err)
	}
	if err := stmt.BindText(2, filepath.Base(to)); err != nil {
		return fmt.Errorf("bind name error: %v", err)
	}
	if err := stmt.BindInt64(3, int64(fromEntry.entryID)); err != nil {
		return fmt.Errorf("bind entry_id error: %v", err)
	}

	if err := stmt.Exec(); err != nil {
		return fmt.Errorf("execute rename error: %v", stmt.Err())
	}

	// if err := tx.Commit(); err != nil {
	// 	return fmt.Errorf("commit transaction error: %v", err)
	// }

	// 清理缓存
	// 1. 清理源文件所在目录的 dirsCache（通过 parentID）
	// fmt.Println("remove from parent", fromEntry.parentID)
	s.dirsCache.Remove(fromEntry.parentID)

	// 2. 清理目标目录的 dirsCache（通过 parentID）
	// fmt.Println("remove to parent", toParentID)
	s.dirsCache.Remove(toParentID)

	// 3. 清理源文件在 entriesCache 中的缓存（通过 full_path*/)
	// fmt.Println("remove from", from)
	s.entriesCache.Remove(from)

	// 4. 如果源文件是目录，清理其 dirsCache
	if fromEntry.mode&os.ModeDir != 0 {
		// fmt.Println("remove from dir", fromEntry.entryID)
		s.dirsCache.Remove(fromEntry.entryID)
	}

	return nil
}

func (s *storage) Remove(path string) error {

	path = clean(path)

	f, err := s.getEntry(path)
	if err != nil {
		return os.ErrNotExist
	}

	if f.mode.IsDir() {
		subItems, err := s.Children(path)
		if err != nil {
			return err
		}
		if subItems != nil && len(*subItems) != 0 {
			return fmt.Errorf("directory not empty: %s", path)
		}
	}

	//fmt.Println("[Lock]Remove")
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	// 开始事务删除
	stmt, _, err := s.conn.Prepare(`
		DELETE FROM entries 
		WHERE entry_id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare delete statement error: %v", err)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(f.entryID)); err != nil {
		return fmt.Errorf("bind entry_id error: %v", err)
	}

	if err := stmt.Exec(); err != nil {
		return fmt.Errorf("execute delete error: %v", stmt.Err())
	}

	// if err := tx.Commit(); err != nil {
	// 	return fmt.Errorf("commit transaction error: %v", err)
	// }

	// 清理缓存
	// 1. 清理被删除项在 entriesCache 中的缓存
	s.entriesCache.Remove(path)

	// 2. 清理父目录在 dirsCache 中的缓存，不需要特别考虑 rootDir
	s.dirsCache.Remove(f.parentID)

	// 3. 如果删除的是目录，清理其 dirsCache
	if f.mode.IsDir() {
		s.dirsCache.Remove(f.entryID)
	}

	return nil
}

func (s *storage) LoadFileChunks(fileID EntryID) *AsyncResult[[]fileChunk] {
	result := NewAsyncResult[[]fileChunk]()

	// Start a goroutine to load chunks asynchronously
	go func() {
		chunks, err := s.loadFileChunksSync(fileID)
		result.Complete(chunks, err)
	}()

	return result
}

func (s *storage) loadFileChunksSync(fileID EntryID) ([]fileChunk, error) {
	// fmt.Println("[Lock][Read]loadFileChunksSync")
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	stmt, _, err := s.conn.Prepare(`
		SELECT rowid, offset, size, block_id, block_offset
		FROM file_chunks
		WHERE entry_id = ?
		ORDER BY rowid ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare select chunks statement error: %v", err)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(fileID)); err != nil {
		return nil, fmt.Errorf("bind entry_id error: %v", err)
	}

	var chunks []fileChunk
	for stmt.Step() {
		chunk := fileChunk{
			rowID:       stmt.ColumnInt64(0),
			offset:      stmt.ColumnInt64(1),
			size:        stmt.ColumnInt64(2),
			blockID:     stmt.ColumnInt64(3),
			blockOffset: stmt.ColumnInt64(4),
		}
		chunks = append(chunks, chunk)
	}

	if err := stmt.Err(); err != nil {
		return nil, fmt.Errorf("execute select chunks error: %v", err)
	}

	return chunks, nil
}

func (s *storage) FileWrite(fileID EntryID, reqID int64, p []byte, offset int64) *AsyncResult[WriteResult] {
	result := NewAsyncResult[WriteResult]()
	go func() {
		rs, err := s.fileWriteSync(fileID, reqID, p, offset)
		result.Complete(rs, err)
	}()
	return result
}

func (s *storage) FileRead(fileID EntryID, p []byte, offset int64) *AsyncResult[int] {
	// TODO: implement me
	panic("implement me")
}

func (s *storage) BlockRead(blockID BlockID, p []byte, offset int64) *AsyncResult[int] {
	result := NewAsyncResult[int]()

	go func() {
		// 获取数据块
		data, _, err := s.getBlock(blockID)
		if err != nil {
			result.Complete(0, fmt.Errorf("failed to get block %d: %w", blockID, err))
			return
		}

		// 检查偏移量是否超出范围
		if offset >= int64(len(data)) {
			result.Complete(0, fmt.Errorf("offset %d is out of range for block %d with size %d", offset, blockID, len(data)))
			return
		}

		// 计算可读取的字节数
		bytesToRead := int64(len(p))
		if offset+bytesToRead > int64(len(data)) {
			bytesToRead = int64(len(data)) - offset
		}

		// 从数据块中读取数据
		copy(p, data[offset:offset+bytesToRead])

		result.Complete(int(bytesToRead), nil)
	}()

	return result
}

/*
func (s *storage) fileWriteSync(fileID EntryID, p []byte, offset int64) (int, error) {
	// TODO: implement me
	panic("implement me")
}
*/

// LoadEntriesByParent implements StorageOps.LoadEntriesByParent
func (s *storage) LoadEntriesByParent(parentID EntryID, parentPath string) *AsyncResult[[]fileInfo] {
	result := NewAsyncResult[[]fileInfo]()
	if cached, ok := s.dirsCache.Get(parentID); ok {
		result.Complete(cached.([]fileInfo), nil)
		return result
	}
	// Load from database if not cached
	go func() {
		entries, err := s.loadEntriesByParentSync(parentID, parentPath)
		if err != nil {
			result.Complete(nil, err)
			return
		}
		s.dirsCache.Add(parentID, entries)
		result.Complete(entries, nil)
	}()
	return result
}

func (s *storage) FileTruncate(fileID EntryID, size int64) *AsyncResult[error] {
	result := NewAsyncResult[error]()

	go func() {

		if size == 0 {
			// 特殊处理：删除所有chunks
			err := s.deleteAllChunksInTx(s.conn, fileID)
			if err != nil {
				result.Complete(nil, err)
				return
			}
		} else {
			// fmt.Println("[Lock]FileTruncate")
			s.connMutex.Lock()
			defer s.connMutex.Unlock()

			// 1. 删除完全在截断点后的chunks
			stmt, _, err := s.conn.Prepare(`
				DELETE FROM file_chunks 
				WHERE file_id = ? AND offset >= ?
			`)
			if err != nil {
				result.Complete(nil, fmt.Errorf("prepare delete statement error: %v", err))
				return
			}
			defer stmt.Close()

			if err := stmt.BindInt64(1, int64(fileID)); err != nil {
				result.Complete(nil, fmt.Errorf("bind file_id error: %v", err))
				return
			}
			if err := stmt.BindInt64(2, size); err != nil {
				result.Complete(nil, fmt.Errorf("bind size error: %v", err))
				return
			}

			if err := stmt.Exec(); err != nil {
				result.Complete(nil, fmt.Errorf("execute delete error: %v", err))
				return
			}

			// 2. 更新跨越截断点的chunks
			stmt, _, err = s.conn.Prepare(`
				UPDATE file_chunks 
				SET size = ? - offset
				WHERE file_id = ? 
				AND offset < ?
				AND offset + size > ?
			`)
			if err != nil {
				result.Complete(nil, fmt.Errorf("prepare update statement error: %v", err))
				return
			}
			defer stmt.Close()

			if err := stmt.BindInt64(1, size); err != nil {
				result.Complete(nil, fmt.Errorf("bind size(1) error: %v", err))
				return
			}
			if err := stmt.BindInt64(2, int64(fileID)); err != nil {
				result.Complete(nil, fmt.Errorf("bind file_id error: %v", err))
				return
			}
			if err := stmt.BindInt64(3, size); err != nil {
				result.Complete(nil, fmt.Errorf("bind size(2) error: %v", err))
				return
			}
			if err := stmt.BindInt64(4, size); err != nil {
				result.Complete(nil, fmt.Errorf("bind size(3) error: %v", err))
				return
			}

			if err := stmt.Exec(); err != nil {
				result.Complete(nil, fmt.Errorf("execute update error: %v", err))
				return
			}
		}

		// if err := tx.Commit(); err != nil {
		// 	result.Complete(nil, fmt.Errorf("commit transaction error: %v", err))
		// 	return
		// }

		result.Complete(nil, nil)
	}()

	// TODO: 还需要处理待写入的操作。

	return result
}

func (s *storage) deleteAllChunksInTx(tx *sqlite3.Conn, fileID EntryID) error {
	stmt, _, err := tx.Prepare(`
		DELETE FROM file_chunks 
		WHERE file_id = ?
	`)
	if err != nil {
		return fmt.Errorf("prepare delete all chunks statement error: %v", err)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(fileID)); err != nil {
		return fmt.Errorf("bind file_id error: %v", err)
	}

	if err := stmt.Exec(); err != nil {
		return fmt.Errorf("execute delete all chunks error: %v", err)
	}

	return nil
}

func (s *storage) loadEntriesByParentSync(parentID EntryID, parentPath string) ([]fileInfo, error) {
	// 获得锁
	// fmt.Println("[Lock][Read]loadEntriesByParentSync")
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	stmt, tail, err := s.conn.Prepare(`
		SELECT entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, size,create_at, modify_at
		FROM entries
		WHERE parent_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return nil, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(parentID)); err != nil {
		return nil, fmt.Errorf("bind parent_id error: %v", err)
	}

	var entries []fileInfo
	for stmt.Step() {
		if err := stmt.Err(); err != nil {
			return nil, fmt.Errorf("execute statement error: %v", err)
		}

		createTime := time.Unix(stmt.ColumnInt64(8), 0)
		modTime := time.Unix(stmt.ColumnInt64(9), 0)

		// Combine mode_type and mode_perm
		modeType := stmt.ColumnInt64(3)
		modePerm := stmt.ColumnInt64(4)
		mode := fs.FileMode(modeType | modePerm)

		fi := fileInfo{
			entryID:  EntryID(stmt.ColumnInt64(0)),
			parentID: EntryID(stmt.ColumnInt64(1)),
			name:     stmt.ColumnText(2),
			fullPath: path.Join(parentPath, stmt.ColumnText(2)),
			mode:     mode,
			uid:      int(stmt.ColumnInt64(5)),
			gid:      int(stmt.ColumnInt64(6)),
			target:   stmt.ColumnText(7),
			size:     stmt.ColumnInt64(8),
			createAt: createTime,
			modTime:  modTime,
		}
		entries = append(entries, fi)
	}

	return entries, nil
}

// getBlock 获取指定 ID 的数据块内容，优先从缓存中获取
func (s *storage) getBlock(blockID BlockID) (data []byte, inWriteBuffer bool, err error) {
	// 首先检查写缓冲区
	s.stateLock.RLock()
	for _, wb := range s.writeBuffers {
		if wb == nil {
			continue
		}
		if wb.blockID == blockID {
			s.stateLock.RUnlock()
			return wb.data, true, nil
		}
	}
	s.stateLock.RUnlock()

	// 检查 LRU 缓存
	if value, ok := s.blockCache.Get(blockID); ok {
		return value.([]byte), false, nil
	}

	// 获得锁
	//fmt.Println("[Lock][Read]getBlock")
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	var stmt *sqlite3.Stmt
	stmt, _, err = s.conn.Prepare(`SELECT data FROM blocks WHERE block_id = ? LIMIT 1`)
	if err != nil {
		return nil, false, fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// 绑定参数
	if err = stmt.BindInt64(1, int64(blockID)); err != nil {
		return nil, false, fmt.Errorf("failed to bind block_id: %w", err)
	}

	if !stmt.Step() {
		return nil, false, fmt.Errorf("block %d not found", blockID)
	}

	// 获取原始数据
	rawData := stmt.ColumnRawBlob(0)

	// 为缓存复制一份数据
	data = make([]byte, len(rawData))
	copy(data, rawData)

	// 将复制的数据放入缓存
	s.blockCache.Add(blockID, data)

	return data, false, nil
}
