package sqlfs

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/ncruces/go-sqlite3"
)

const (
	// 存储层基本参数
	DefaultBufferSize = 2 * 1024 * 1024 // 2MB
	DefaultBufferNum  = 8               // 默认缓冲区数量
	AlignSize         = 4 * 1024        // 4KB 对齐
)

// AsyncResult represents an asynchronous operation result
type AsyncResult[T any] struct {
	Result T
	Err    error
	Done   chan struct{}
}

// NewAsyncResult creates a new AsyncResult
func NewAsyncResult[T any]() *AsyncResult[T] {
	return &AsyncResult[T]{
		Done: make(chan struct{}),
	}
}

// Wait waits for the async operation to complete and returns the result
func (ar *AsyncResult[T]) Wait() (T, error) {
	<-ar.Done
	return ar.Result, ar.Err
}

func (ar *AsyncResult[T]) Complete(result T, err error) {
	ar.Result = result
	ar.Err = err
	close(ar.Done)
}

type WriteResult struct {
	BytesWritten int64
	Err          error
}

// StorageOps defines the interface for storage operations
type StorageOps interface {
	// LoadEntry loads a single entry by its ID asynchronously
	// LoadEntry(entryID int64) *AsyncResult[*fileInfo]

	// LoadEntriesByParent loads all entries in a directory by parent_id asynchronously
	LoadEntriesByParent(parentID EntryID, parentPath string) *AsyncResult[[]fileInfo]
	LoadFileChunks(fileID EntryID) *AsyncResult[[]fileChunk]

	// 文件相关的操作
	FileTruncate(fileID EntryID, size int64) *AsyncResult[error]
	FileWrite(fileID EntryID, p []byte, offset int64) *AsyncResult[int]

	// 写入到数据块
	Flush() *AsyncResult[[]BlockID]
}

// 细化的缓冲区状态
type bufferState int

const (
	bufferEmpty bufferState = iota
	bufferWriting
	bufferFull
	bufferFlushing
)

// WriteBuffer 增加独立的锁和状态管理
type WriteBuffer struct {
	data     []byte
	position int
	state    bufferState
	lock     sync.RWMutex    // 缓冲区独立的锁
	pending  []*PendingChunk // 关联的待写入chunks
}

// PendingChunk 扩展自 fileChunk
type PendingChunk struct {
	fileChunk        // 继承基本字段
	bufferOffset int // 在缓冲区中的起始位置
}

type storage struct {
	//files    map[string]*file
	//children map[string]map[string]*file

	/// 新增的与 SQLiteFS 相关的字段
	mu           sync.Mutex
	conn         *sqlite3.Conn
	entriesCache *lru.Cache // LRU cache for file/directory information, full_path -> entryId.
	dirsCache    *lru.Cache // LRU cache for directory information	entryId -> infoList.
	rootEntry    *fileInfo
	maxEntryID   EntryID

	// 可能需要调整、重构，暂时先在此处。 仅针对 proto tag 有效。
	writeBuffers []*WriteBuffer
	bufferCond   *sync.Cond   // 用于等待可用缓冲区
	stateLock    sync.RWMutex // 仅用于状态变更
}

func newStorage(dbName string) (*storage, error) {
	conn, err := sqlite3.Open(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := InitDatabase(conn); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return &storage{
		// files:        make(map[string]*file, 0),
		// children:     make(map[string]map[string]*file, 0),
		conn:         conn,
		entriesCache: lru.New(500), // Initialize entries cache
		dirsCache:    lru.New(100), // Initialize directories cache
		rootEntry:    loadRootEntry(conn),
		maxEntryID:   loadMaxEntryID(conn),
	}, nil
}

func loadRootEntry(conn *sqlite3.Conn) *fileInfo {

	entryID := int64(1)

	stmt, _, err := conn.Prepare(`
		SELECT entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
		FROM entries
		WHERE entry_id = ?
	`)
	if err != nil {
		return nil
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, entryID); err != nil {
		return nil
	}

	if !stmt.Step() {
		if err := stmt.Err(); err != nil {
			return nil
		}
		return nil
	}

	createTime := time.Unix(stmt.ColumnInt64(8), 0)
	modTime := time.Unix(stmt.ColumnInt64(9), 0)

	// Combine mode_type and mode_perm
	modeType := stmt.ColumnInt64(3)
	modePerm := stmt.ColumnInt64(4)
	mode := fs.FileMode(modeType | modePerm)

	fi := &fileInfo{
		entryID:  EntryID(stmt.ColumnInt64(0)),
		parentID: EntryID(stmt.ColumnInt64(1)),
		name:     stmt.ColumnText(2),
		fullPath: "/", // 因为是根目录, 所以 fullPath 固定是 /
		mode:     mode,
		uid:      int(stmt.ColumnInt64(5)),
		gid:      int(stmt.ColumnInt64(6)),
		target:   stmt.ColumnText(7),
		createAt: createTime,
		modTime:  modTime,
	}

	return fi
}

func loadMaxEntryID(conn *sqlite3.Conn) EntryID {
	stmt, tail, err := conn.Prepare(`
		SELECT MAX(entry_id) FROM entries
	`)
	if err != nil {
		return 0 // , fmt.Errorf("prepare statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return 0 //, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	if !stmt.Step() {
		return 0 //, fmt.Errorf("no results found: %v", stmt.Err())
	}

	// Get the max entry_id
	maxEntryID := EntryID(stmt.ColumnInt64(0))
	return maxEntryID //, nil
}

/////////////////////////////////////////////

// getEntry retrieves the fileInfo for the given full_path, recursively searching through directories.
func (s *storage) getEntry(full_path string) (*fileInfo, error) {
	// Clean and normalize the path
	full_path = clean(full_path)

	// Check cache first
	if cached, ok := s.entriesCache.Get(full_path); ok {
		return cached.(*fileInfo), nil
	}

	// Handle root directory specially
	if full_path == "/" {
		return s.rootEntry, nil
	}

	// Split into directory and file name
	dirPath := filepath.Dir(full_path)
	fileName := filepath.Base(full_path)

	// fmt.Println("getEntry:", full_path, dirPath, fileName)

	var parentID EntryID
	if dirPath == "/" {
		parentID = 1 // root directory
	} else {
		parentDirInfo, err := s.getEntry(dirPath)
		if err != nil {
			return nil, err
		}
		parentID = parentDirInfo.entryID
	}

	// Load entries in the parent directory
	entriesResult := s.LoadEntriesByParent(parentID, dirPath)
	entries, err := entriesResult.Wait()
	if err != nil {
		return nil, err
	}

	// fmt.Println("getEntry「DONE」:", full_path, dirPath, fileName, parentID, entries)
	// Check for the file in the loaded entries
	for _, entry := range entries {
		if entry.name == fileName {
			s.entriesCache.Add(full_path, &entry) // Add to cache
			return &entry, nil
		}
	}

	// If we reach here, the file was not found
	return nil, &ErrFileNotFound{Path: full_path}
}

/////////////////////////////////////////////////////////

func clean(path string) string {
	return filepath.Clean(filepath.FromSlash(path))
}

/////////////////////////////////////////////////////////

// 处理 storage 的写入前可交换缓存

// 初始化存储层时初始化缓冲区
func (s *storage) initBuffer(idx int) *WriteBuffer {
	//s.writeBuffers = make([]*WriteBuffer, DefaultBufferNum)
	// 初始化前两个缓冲区
	//s.writeBuffers[0] = &WriteBuffer{data: make([]byte, DefaultBufferSize)}
	s.writeBuffers[idx] = &WriteBuffer{data: make([]byte, DefaultBufferSize)}
	return s.writeBuffers[idx]
}

// 获取可写入的缓冲区
func (s *storage) getAvailableBuffer() (*WriteBuffer, int) {
	s.stateLock.RLock()
	for i, buf := range s.writeBuffers {
		if buf == nil {
			// 延迟到返回前解锁，避免 s.writeBuffers[i] 被多次 alloc.
			defer s.stateLock.RUnlock()
			return s.initBuffer(i), i
		}

		// 快速检查状态
		buf.lock.RLock()
		state := buf.state
		buf.lock.RUnlock()

		if state == bufferEmpty || state == bufferWriting {
			s.stateLock.RUnlock()
			return buf, i
		}
	}
	s.stateLock.RUnlock()
	return nil, -1
}

// 写入操作
func (s *storage) fileWriteSync(fileID EntryID, p []byte, offset int64) (int, error) {
	for {
		buffer, _ := s.getAvailableBuffer()
		if buffer == nil {
			// 等待可用缓冲区
			s.bufferCond.Wait()
			continue
		}

		// 尝试锁定选中的缓冲区
		buffer.lock.Lock()
		if buffer.state != bufferEmpty && buffer.state != bufferWriting {
			// 状态已改变，释放锁并重试
			buffer.lock.Unlock()
			continue
		}

		// 执行写入
		writeSize := s.writeToBuffer(buffer, fileID, p, offset)
		buffer.lock.Unlock()

		return writeSize, nil
	}
}

// 写入到缓冲区
func (s *storage) writeToBuffer(buffer *WriteBuffer, fileID EntryID, p []byte, offset int64) int {
	// buffer.lock 已在调用方加锁

	remainSpace := DefaultBufferSize - buffer.position
	writeSize := min(len(p), remainSpace)

	// 写入数据
	copy(buffer.data[buffer.position:], p[:writeSize])

	// 添加 PendingChunk
	chunk := &PendingChunk{
		fileChunk: fileChunk{
			offset: offset,
			size:   int64(writeSize),
		},
		bufferOffset: buffer.position,
	}
	buffer.pending = append(buffer.pending, chunk)

	// 更新状态
	buffer.position += writeSize
	buffer.state = bufferWriting
	if buffer.position >= DefaultBufferSize {
		buffer.state = bufferFull
		// 触发异步刷新
		s.triggerFlush()
	}

	return writeSize
}

// 刷新工作器
func (s *storage) flushWorker() {
	for range s.flushChan {
		s.stateLock.RLock()
		var buffersToFlush []*WriteBuffer
		for _, buf := range s.writeBuffers {
			if buf == nil {
				continue
			}

			buf.lock.RLock()
			if buf.state == bufferFull {
				buffersToFlush = append(buffersToFlush, buf)
			}
			buf.lock.RUnlock()
		}
		s.stateLock.RUnlock()

		// 处理需要刷新的缓冲区
		for _, buffer := range buffersToFlush {
			buffer.lock.Lock()
			if buffer.state != bufferFull {
				buffer.lock.Unlock()
				continue
			}
			buffer.state = bufferFlushing

			// 复制需要的数据
			alignedSize := (buffer.position + AlignSize - 1) & ^(AlignSize - 1)
			data := make([]byte, alignedSize)
			copy(data, buffer.data[:buffer.position])
			pendingChunks := make([]*PendingChunk, len(buffer.pending))
			copy(pendingChunks, buffer.pending)

			buffer.lock.Unlock()

			// 执行实际的刷新操作
			if err := s.flushBuffer(data, pendingChunks); err == nil {
				buffer.lock.Lock()
				buffer.position = 0
				buffer.pending = buffer.pending[:0]
				buffer.state = bufferEmpty
				buffer.lock.Unlock()

				// 通知等待的写入操作
				s.bufferCond.Broadcast()
			}
		}
	}
}

type content struct {
	name  string
	bytes []byte

	m sync.RWMutex
}

// ///////////////////////////////////////////////////////
// ErrFileNotFound represents an error when a file is not found in the storage.
type ErrFileNotFound struct {
	Path string
}

func (e *ErrFileNotFound) Error() string {
	return fmt.Sprintf("file not found: %s", e.Path)
}
