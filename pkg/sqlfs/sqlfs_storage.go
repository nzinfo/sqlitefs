package sqlfs

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
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
	BytesWritten int
	BlockID      BlockID
	BlockOffset  int
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
	FileWrite(fileID EntryID, reqID int64, p []byte, offset int64) *AsyncResult[WriteResult]
	// buffer size / 需要读取的大小 由 p 给出， 返回实际写入的大小
	FileRead(fileID EntryID, p []byte, offset int64) *AsyncResult[int]

	// 数据块 Block 相关操作
	BlockRead(blockID BlockID, p []byte, offset int64) *AsyncResult[int]

	// 写入到数据块
	Flush() *AsyncResult[[]BlockID]
	// 关闭存储
	Close() error
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
	blockID  BlockID         // 预分配的块 ID, 如果为 0 表示还为编码
}

// PendingChunk 扩展自 fileChunk
type PendingChunk struct {
	fileChunk    // 继承基本字段
	fileID       EntryID
	reqID        int64 // 对于每一个 file ，每次 Open reqID 均不重复
	bufferOffset int   // 在缓冲区中的起始位置
}

// ChunkUpdateInfo 存储 chunk 更新的信息
type ChunkUpdateInfo struct {
	FileID      EntryID
	ReqID       int64
	BlockID     int64
	BlockOffset int64
}

// ChunkUpdateBatch 存储批量更新信息
type ChunkUpdateBatch struct {
	Updates map[EntryID]map[int64]ChunkUpdateInfo
}

type storage struct {
	//files    map[string]*file
	//children map[string]map[string]*file

	/// 新增的与 SQLiteFS 相关的字段
	conn      *sqlite3.Conn
	connMutex sync.RWMutex
	// entriesLock  sync.RWMutex
	// dirsLock     sync.RWMutex
	// LRU 缓存，这两个缓存都是线程安全的，不需要额外的锁
	entriesCache *lru.Cache // LRU cache for file/directory information, full_path -> entryId.
	dirsCache    *lru.Cache // LRU cache for directory information	entryId -> infoList.
	blockCache   *lru.Cache // LRU cache for block data, block_id -> []byte
	rootEntry    *fileInfo
	maxEntryID   EntryID
	maxBlockID   BlockID

	// 可能需要调整、重构，暂时先在此处。 仅针对 proto tag 有效。
	writeBuffers     []*WriteBuffer
	writeBufferIndex int              // 记录当前使用的 Buffer
	bufferCond       *sync.Cond       // 用于等待可用缓冲区
	stateLock        sync.RWMutex     // 仅用于状态变更
	controlChan      chan flushSignal // 控制通道，用于刷新和停止信号
	workerDone       chan struct{}    // flushWorker 停止完成
}

type flushSignal int

const (
	signalFlush flushSignal = iota
	signalStop
)

func newStorage(dbName string) (*storage, error) {
	conn, err := sqlite3.Open(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %v", err)
	}

	// 初始化数据库表结构
	if err := InitDatabase(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize database: %v", err)
	}
	maxEntryID, err := loadMaxEntryID(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to load max entry ID: %v", err)
	}
	maxBlockID, err := loadMaxBlockID(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to load max block ID: %v", err)
	}

	s := &storage{
		conn:             conn,
		writeBuffers:     make([]*WriteBuffer, DefaultBufferNum),
		writeBufferIndex: 0, // 检查缓冲区时，最先检查是否可用的位置，实际需要  % DefaultBufferNum
		controlChan:      make(chan flushSignal, DefaultBufferNum+1),
		workerDone:       make(chan struct{}),
		entriesCache:     lru.New(1024), // 添加 entries 缓存初始化
		dirsCache:        lru.New(1024), // 添加 dirs 缓存初始化
		blockCache:       lru.New(8),    // 添加 blocks 缓存初始化
		rootEntry:        loadRootEntry(conn),
		maxEntryID:       maxEntryID,
		maxBlockID:       maxBlockID,
	}

	// 初始化条件变量
	s.bufferCond = sync.NewCond(&s.stateLock)

	// 启动刷新工作器
	go s.flushWorker()

	return s, nil
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
		size:     int64(0),
		createAt: createTime,
		modTime:  modTime,
	}

	return fi
}

func loadMaxEntryID(conn *sqlite3.Conn) (EntryID, error) {
	stmt, tail, err := conn.Prepare(`
		SELECT MAX(entry_id) FROM entries
	`)
	if err != nil {
		return 0, err
	}
	if tail != "" {
		stmt.Close()
		return 0, fmt.Errorf("prepare error: %v", tail)
	}
	defer stmt.Close()

	if !stmt.Step() {
		return 0, fmt.Errorf("step error: %v", stmt.Err())
	}

	// Get the max entry_id
	maxEntryID := EntryID(stmt.ColumnInt64(0))
	return maxEntryID, nil
}

func loadMaxBlockID(conn *sqlite3.Conn) (BlockID, error) {
	stmt, tail, err := conn.Prepare(`
		SELECT MAX(block_id) FROM blocks
	`)
	if err != nil {
		return 0, err
	}
	if tail != "" {
		stmt.Close()
		return 0, fmt.Errorf("prepare error: %v", tail)
	}
	defer stmt.Close()

	if !stmt.Step() {
		return 0, fmt.Errorf("step error: %v", stmt.Err())
	}

	// Get the max block_id
	maxBlockID := BlockID(stmt.ColumnInt64(0))
	return maxBlockID, nil
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

	// fmt.Println("getEntry「DONE」:", full_path, dirPath, fileName, parentID, len(entries))
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
	s.writeBuffers[idx] = &WriteBuffer{
		data:     make([]byte, DefaultBufferSize),
		position: 0,
		state:    bufferEmpty,
		lock:     sync.RWMutex{},
		pending:  nil,
		blockID:  0,
	}
	return s.writeBuffers[idx]
}

// 获取可写入的缓冲区
func (s *storage) getAvailableBuffer(dataSize int) (*WriteBuffer, int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	// fmt.Printf("getAvailableBuffer: 开始查找可用缓冲区，当前索引=%d，需要空间=%d\n", s.writeBufferIndex, dataSize)

	// 从当前索引开始检查，最多检查一轮
	checkedCount := 0
	for checkedCount < DefaultBufferNum {
		idx := (s.writeBufferIndex + checkedCount) % DefaultBufferNum

		// 如果缓冲区未初始化，初始化它
		if s.writeBuffers[idx] == nil {
			// fmt.Printf("getAvailableBuffer: 缓冲区[%d]未初始化，正在初始化\n", idx)
			s.writeBuffers[idx] = s.initBuffer(idx)
		}

		buffer := s.writeBuffers[idx]
		buffer.lock.Lock()

		// 如果缓冲区为空，可以直接使用
		if buffer.state == bufferEmpty {
			buffer.state = bufferWriting
			// 这表明 buffer 已经被启用，需要分配 block_id
			s.maxBlockID++ // 更新最大块ID
			buffer.blockID = s.maxBlockID
			buffer.position = 0
			buffer.lock.Unlock()
			// s.writeBufferIndex = idx // 避免 writeBufferIndex 溢出
			// fmt.Printf("getAvailableBuffer: 找到空闲缓冲区[%d]\n", idx)
			return buffer, idx
		}

		// 如果缓冲区正在写入，检查剩余空间是否足够
		if buffer.state == bufferWriting {
			remainSpace := DefaultBufferSize - buffer.position
			if remainSpace >= dataSize {
				buffer.lock.Unlock()
				// fmt.Printf("getAvailableBuffer: 缓冲区[%d]正在写入但空间足够，剩余空间=%d\n", idx, remainSpace)
				return buffer, idx
			}
			// fmt.Printf("getAvailableBuffer: 缓冲区[%d]空间不足，剩余空间=%d，需要空间=%d\n", idx, remainSpace, dataSize)
		}

		// 处理 state == Full 的情况
		if buffer.state == bufferFull {
			// fmt.Printf("getAvailableBuffer: 缓冲区[%d]状态=%d，已满，触发异步刷新\n", idx, buffer.state)
			s.triggerFlush()
			s.writeBufferIndex = (idx + 1) % DefaultBufferNum
			checkedCount = 0
			buffer.lock.Unlock()
			continue
		}

		// 特殊处理 buffer 的状态，如果剩余空间不足 4K 则标记为 full, 并激活更新
		if (buffer.state == bufferWriting) && (DefaultBufferSize-buffer.position < AlignSize) {
			buffer.state = bufferFull
			// 触发异步刷新
			// fmt.Println("writeToBuffer: 缓冲区已满，触发异步刷新")
			s.triggerFlush()

			//更新下一次检查的起始位置
			s.writeBufferIndex = (idx + 1) % DefaultBufferNum
			checkedCount = 0
		}

		buffer.lock.Unlock()

		// 移动到下一个缓冲区
		checkedCount++
		// s.writeBufferIndex = (idx + 1) % DefaultBufferNum  // 不必然需要修改，完全可能是当前待写入的数据过大。
	}

	// fmt.Printf("getAvailableBuffer: 检查了 %d 个缓冲区后未找到可用缓冲区\n", checkedCount)
	// 没有找到可用的缓冲区
	return nil, -1
}

// 写入操作
func (s *storage) fileWriteSync(fileID EntryID, reqID int64, p []byte, offset int64) (WriteResult, error) {
	for {
		buffer, _ := s.getAvailableBuffer(len(p))
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
		position := buffer.position
		writeSize := s.writeToBuffer(buffer, fileID, reqID, p, offset)
		buffer.lock.Unlock()

		return WriteResult{
			BytesWritten: int(writeSize),
			BlockID:      buffer.blockID,
			BlockOffset:  position,
		}, nil
	}
}

// 写入到缓冲区
func (s *storage) writeToBuffer(buffer *WriteBuffer, fileID EntryID, reqID int64, p []byte, offset int64) int64 {
	// buffer.lock 已在调用方加锁
	// fmt.Printf("writeToBuffer: 文件ID=%d, reqID=%d, 数据大小=%d, 偏移量=%d\n",
	//	fileID, reqID, len(p), offset)

	remainSpace := int64(DefaultBufferSize - buffer.position)
	writeSize := min64(int64(len(p)), remainSpace)

	// fmt.Printf("writeToBuffer: 缓冲区剩余空间=%d, 实际写入大小=%d\n",
	//	remainSpace, writeSize)

	// 写入数据
	copy(buffer.data[buffer.position:], p[:writeSize])

	// 添加 PendingChunk
	chunk := &PendingChunk{
		fileChunk: fileChunk{
			offset: offset,
			size:   writeSize,
		},
		fileID:       fileID,
		reqID:        reqID,
		bufferOffset: buffer.position,
	}
	buffer.pending = append(buffer.pending, chunk)

	// fmt.Printf("writeToBuffer: 添加 PendingChunk: fileID=%d, reqID=%d, offset=%d, size=%d, bufferOffset=%d\n",
	//	fileID, reqID, offset, writeSize, buffer.position)

	// 更新状态
	buffer.position += int(writeSize)
	buffer.state = bufferWriting
	if buffer.position >= DefaultBufferSize {
		// 此处代码目前永远不会激活，因为调用时会检查。
		buffer.state = bufferFull
		// 触发异步刷新
		// fmt.Println("writeToBuffer: 缓冲区已满，触发异步刷新")
		s.triggerFlush()
	}

	return writeSize
}

// flushWorker 在后台运行，处理缓冲区刷新
func (s *storage) flushWorker() {
	defer close(s.workerDone)

	for {
		signal := <-s.controlChan
		switch signal {
		case signalStop:
			// 在退出前执行最后一次刷新
			s.doFlush()
			return

		case signalFlush:
			s.doFlush()
		}
	}
}

// doFlush 执行实际的刷新操作
func (s *storage) doFlush() {
	var buffersToFlush []*WriteBuffer
	s.stateLock.RLock()
	if s.writeBuffers != nil {
		for _, buffer := range s.writeBuffers {
			if buffer == nil {
				continue
			}
			buffer.lock.RLock()
			if buffer.state == bufferFull && len(buffer.pending) > 0 {
				buffersToFlush = append(buffersToFlush, buffer)
			}
			buffer.lock.RUnlock()
			// fmt.Println("Buffer", buffer.blockID, len(buffer.pending), buffer.state, buffer.position)
		}
	}
	s.stateLock.RUnlock()

	// fmt.Printf("flushWorker 找到 %d 个需要刷新的缓冲区\n", len(buffersToFlush))
	for _, buffer := range buffersToFlush {
		// fmt.Printf("flushWorker 准备刷新缓冲区: BlockID=%d, Block数据大小=%d, chunks数量=%d\n",
		//	buffer.blockID, buffer.position, len(buffer.pending))

		// 执行实际的刷新操作
		err := s.flushBuffer(buffer.blockID, buffer.data[:buffer.position], buffer.pending)
		if err == nil {
			buffer.lock.Lock()
			buffer.position = 0
			buffer.pending = nil
			buffer.state = bufferEmpty
			buffer.lock.Unlock()
		} else {
			fmt.Printf("flushWorker 刷新缓冲区失败: %v\n", err)
		}
	}
}

// Flush 将所有缓冲区的数据刷新到数据库
func (s *storage) Flush() *AsyncResult[[]BlockID] {
	result := NewAsyncResult[[]BlockID]()

	// go func()
	{
		// 获取所有需要刷新的缓冲区
		var buffersToFlush []*WriteBuffer
		s.stateLock.RLock()
		// 检查 writeBuffers 是否为 nil
		if s.writeBuffers != nil {
			for _, buffer := range s.writeBuffers {
				if buffer == nil {
					continue
				}
				buffer.lock.RLock()
				if buffer.state == bufferWriting && len(buffer.pending) > 0 {
					buffersToFlush = append(buffersToFlush, buffer)
				}
				buffer.lock.RUnlock()
			}
		}
		s.stateLock.RUnlock()

		// 刷新所有缓冲区
		var blockIDs []BlockID
		var flushErr error
		for _, buffer := range buffersToFlush {
			buffer.lock.RLock()
			defer buffer.lock.RUnlock()

			// fmt.Printf("Flush: 正在刷新缓冲区 %d/%d, 状态=%d, 位置=%d, 待处理项=%d\n",
			//	i+1, len(buffersToFlush), buffer.state, buffer.position, len(buffer.pending))

			if len(buffer.pending) > 0 {
				err := s.flushBuffer(buffer.blockID, buffer.data[:buffer.position], buffer.pending)
				if err != nil {
					flushErr = fmt.Errorf("flush buffer error: %v", err)
					// fmt.Printf("Flush: 刷新缓冲区 %d 失败: %v\n", i+1, err)
					break
				}

				// fmt.Printf("Flush: 刷新缓冲区 %d 成功, BlockID=%d\n", i+1, buffer.blockID)
				blockIDs = append(blockIDs, buffer.blockID)

				// 重置缓冲区
				buffer.position = 0
				buffer.pending = nil
				buffer.state = bufferEmpty
				// fmt.Printf("Flush: 重置缓冲区 %d\n", i+1)
			} else {
				// fmt.Printf("Flush: 缓冲区 %d 没有待处理项，跳过\n", i+1)
			}
			// buffer.lock.Unlock()
		}

		// fmt.Printf("Flush: 完成，共刷新 %d 个块，错误: %v\n", len(blockIDs), flushErr)
		result.Complete(blockIDs, flushErr)
	} // ()

	return result
}

// Close 关闭存储，确保所有数据都已刷新
func (s *storage) Close() error {
	// fmt.Println("storage Close: 开始关闭")

	// 发送停止信号
	s.controlChan <- signalStop

	// 等待 worker 完成
	<-s.workerDone

	// fmt.Println("storage Close: flushWorker 已停止")

	// fmt.Println("[Lock]storage Close")
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	// 关闭其他资源

	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			return fmt.Errorf("close sqlite connection error: %v", err)
		}
		s.conn = nil
	}

	// fmt.Println("storage Close: 完成")
	return nil
}

// ///////////////////////////////////////////////////////
// ErrFileNotFound represents an error when a file is not found in the storage.
type ErrFileNotFound struct {
	Path string
}

func (e *ErrFileNotFound) Error() string {
	return fmt.Sprintf("file not found: %s", e.Path)
}

// min 返回两个 int64 中的较小值
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// triggerFlush 触发缓冲区刷新
func (s *storage) triggerFlush() {
	// fmt.Println("triggerFlush: 尝试发送刷新信号")
	select {
	case s.controlChan <- signalFlush:
		// fmt.Println("triggerFlush: 成功发送刷新信号")
	default:
		// fmt.Println("triggerFlush: 控制通道已满，无法发送信号")
	}
}

// flushBuffer 将数据写入数据库
func (s *storage) flushBuffer(blockID BlockID, data []byte, chunks []*PendingChunk) error {
	// s.conn 因为涉及 Prepare / Next 等操作，只能单线程操作
	// 考虑到代码易用性，约定仅在实际进行数据库操作的函数中 尝试获得锁。
	// fmt.Println("[Lock]flushBuffer")
	s.connMutex.Lock()
	defer s.connMutex.Unlock()

	// 1. 写入 blocks 表
	stmt, _, err := s.conn.Prepare(`
		INSERT INTO blocks (block_id, data)
		VALUES (?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, int64(blockID)); err != nil {
		return err
	}
	if err := stmt.BindBlob(2, data); err != nil {
		return err
	}
	if err := stmt.Exec(); err != nil {
		return err
	}

	// 2. 计算每个文件的最大大小
	// 合并连续的chunk: 如果前后chunks中两个 chunk 的 entryID 相同，
	//		且 前一个 offset + size 正好是下一个的 offset 则合并
	maxSizes := make(map[EntryID]int64)

	// fmt.Printf("flushBuffer: 开始处理 %d 个chunks\n", len(chunks))

	// 按文件ID和偏移量排序chunks，以便合并连续的chunks
	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].fileID != chunks[j].fileID {
			return chunks[i].fileID < chunks[j].fileID
		}
		return chunks[i].offset < chunks[j].offset
	})
	/*
		fmt.Println("排序后的chunks:")
		for i, chunk := range chunks {
			fmt.Printf("  [%d] fileID=%d, offset=%d, size=%d, blockID=%d, blockOffset=%d, bufferOffset=%d\n",
				i, chunk.fileID, chunk.offset, chunk.size, chunk.blockID, chunk.blockOffset, chunk.bufferOffset)
		}
	*/

	// 合并连续的chunks并计算最大大小
	mergedChunks := make([]*PendingChunk, 0, len(chunks))
	var current *PendingChunk

	for _, chunk := range chunks {
		// 计算最大大小
		endOffset := chunk.offset + chunk.size
		if current, exists := maxSizes[chunk.fileID]; !exists || endOffset > current {
			maxSizes[chunk.fileID] = endOffset
		}

		// 尝试合并chunks
		if current == nil {
			current = chunk
			mergedChunks = append(mergedChunks, current)
			// fmt.Printf("首个chunk: fileID=%d, offset=%d, size=%d, blockID=%d, blockOffset=%d, bufferOffset=%d\n",
			// 	current.fileID, current.offset, current.size, current.blockID, current.blockOffset, current.bufferOffset)
			continue
		}

		// 检查是否可以合并
		canMerge := current.fileID == chunk.fileID &&
			current.blockID == chunk.blockID && // 不需要检查 blockOffset， 当前上下文对应的是 bufferOffset
			current.offset+current.size == chunk.offset &&
			current.bufferOffset+int(current.size) == chunk.bufferOffset
		/*
			fmt.Printf("检查合并条件:\n")
			fmt.Printf("  当前chunk: fileID=%d, offset=%d, size=%d, blockID=%d, blockOffset=%d, bufferOffset=%d\n",
				current.fileID, current.offset, current.size, current.blockID, current.blockOffset, current.bufferOffset)
			fmt.Printf("  新chunk: fileID=%d, offset=%d, size=%d, blockID=%d, blockOffset=%d, bufferOffset=%d\n",
				chunk.fileID, chunk.offset, chunk.size, chunk.blockID, chunk.blockOffset, chunk.bufferOffset)
			fmt.Printf("  条件检查:\n")
			fmt.Printf("    - 相同fileID: %v\n", current.fileID == chunk.fileID)
			fmt.Printf("    - 相同blockID: %v\n", current.blockID == chunk.blockID)
			fmt.Printf("    - 连续blockOffset: %v (current=%d + size=%d == chunk=%d)\n",
				current.blockOffset+current.size == chunk.blockOffset,
				current.blockOffset, current.size, chunk.blockOffset)
			fmt.Printf("    - 连续文件offset: %v (current=%d + size=%d == chunk=%d)\n",
				current.offset+current.size == chunk.offset,
				current.offset, current.size, chunk.offset)
			fmt.Printf("    - 连续bufferOffset: %v (current=%d + size=%d == chunk=%d)\n",
				current.bufferOffset+int(current.size) == chunk.bufferOffset,
				current.bufferOffset, current.size, chunk.bufferOffset)
			fmt.Printf("  可以合并: %v\n", canMerge)
		*/
		if canMerge {
			// 合并chunks
			current.size += chunk.size
			// fmt.Printf("合并完成: 新size=%d\n", current.size)
		} else {
			// 不能合并，添加为新的chunk
			current = chunk
			mergedChunks = append(mergedChunks, current)
			// fmt.Printf("无法合并，添加新chunk\n")
		}
	}

	// fmt.Printf("合并完成: 从 %d 个chunks合并为 %d 个\n", len(chunks), len(mergedChunks))

	// 使用合并后的chunks替换原始chunks
	chunks = mergedChunks

	// 3. 写入 file_chunks 表
	stmt, _, err = s.conn.Prepare(`
		INSERT INTO file_chunks (entry_id, offset, size, block_id, block_offset)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, chunk := range chunks {
		if err := stmt.BindInt64(1, int64(chunk.fileID)); err != nil {
			return err
		}
		if err := stmt.BindInt64(2, chunk.offset); err != nil {
			return err
		}
		if err := stmt.BindInt64(3, int64(chunk.size)); err != nil {
			return err
		}
		if err := stmt.BindInt64(4, int64(blockID)); err != nil {
			return err
		}
		if err := stmt.BindInt64(5, int64(chunk.bufferOffset)); err != nil {
			return err
		}
		if stmt.Step(); stmt.Err() != nil {
			return stmt.Err()
		}
		stmt.Reset()
	}

	// 创建临时表并插入数据
	stmt, _, err = s.conn.Prepare(`
		CREATE TEMP TABLE IF NOT EXISTS temp_file_sizes (
			entry_id INTEGER PRIMARY KEY,
			max_size INTEGER
		)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.Exec(); err != nil {
		return err
	}

	// 批量插入数据
	stmt, _, err = s.conn.Prepare(`
		INSERT INTO temp_file_sizes (entry_id, max_size)
		VALUES (?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for entryID, maxSize := range maxSizes {
		if err := stmt.BindInt64(1, int64(entryID)); err != nil {
			return err
		}
		if err := stmt.BindInt64(2, maxSize); err != nil {
			return err
		}
		if stmt.Step(); stmt.Err() != nil {
			return stmt.Err()
		}
		stmt.Reset()
	}

	// fmt.Println("maxSizes:", maxSizes)

	// 一次性更新所有需要更新的文件大小
	stmt, _, err = s.conn.Prepare(`
		UPDATE entries
		SET size = temp_file_sizes.max_size
		FROM temp_file_sizes
		WHERE entries.entry_id = temp_file_sizes.entry_id
		AND temp_file_sizes.max_size > entries.size
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err := stmt.Exec(); err != nil {
		// fmt.Println("update sqlfs size:", stmt, stmt.Err())
		return err
	}

	// 清理临时表
	stmt, _, err = s.conn.Prepare(`DELETE FROM temp_file_sizes`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	if err := stmt.Exec(); err != nil {
		return err
	}

	// if err := tx.Commit(); err != nil {
	// 	return err
	// }
	return nil
}
