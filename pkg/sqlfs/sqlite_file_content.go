package sqlfs

import "fmt"

// fileChunk represents a chunk of file data stored in the file_chunks table
type fileChunk struct {
	rowID       int64 // Primary key from file_chunks table, used for ordering
	offset      int64 // Offset in the file where this chunk starts
	size        int64 // Size of this chunk
	blockID     int64 // ID of the block containing the chunk data
	blockOffset int64 // Offset within the block where chunk data starts
	// bytes       []byte
}

// fileContent manages file chunks using a segment tree for efficient range queries
type fileContent struct {
	chunks        []fileChunk // Sorted by offset
	chunkIndex    *SegmentTree
	endChunkIndex int // 记录 offset + size 最大的 chunk 的索引
}

// newFileContent creates a new fileContent from a slice of chunks
func newFileContent(chunks []fileChunk) *fileContent {
	// 不可对 chunk 排序，因为加载时严格按时间顺序加载的
	fc := &fileContent{
		chunks:     chunks,
		chunkIndex: NewSegmentTree(chunks),
	}
	// fc.buildSegmentTree()
	fc.updateMaxEndChunkIndex()
	return fc
}

// updateMaxEndChunkIndex 更新 maxEndChunkIndex
func (fc *fileContent) updateMaxEndChunkIndex() {
	if len(fc.chunks) == 0 {
		fc.endChunkIndex = -1
		return
	}

	maxIndex := 0
	maxEnd := fc.chunks[0].offset + fc.chunks[0].size

	for i, chunk := range fc.chunks {
		end := chunk.offset + chunk.size
		if end > maxEnd {
			maxEnd = end
			maxIndex = i
		}
	}

	fc.endChunkIndex = maxIndex
}

// Len 返回文件内容的总长度
func (fc *fileContent) Len() int64 {
	if fc.endChunkIndex < 0 {
		return 0
	}
	chunk := fc.chunks[fc.endChunkIndex]
	return chunk.offset + chunk.size
}

// Truncate 调整文件大小，并更新内存中的 chunks
func (fc *fileContent) Truncate(fs *SQLiteFS, fileID EntryID, size int64) error {
	// 1. 调用存储层进行实际的数据库更新
	result := fs.s.FileTruncate(fileID, size)
	truncated_err, err := result.Wait()

	if truncated_err != nil {
		return truncated_err
	}

	if err != nil {
		return err
	}

	// 2. 更新内存中的 chunks
	if size == 0 {
		// 清空所有 chunks
		fc.chunks = nil
		//fc.tree = nil
		fc.endChunkIndex = -1
		return nil
	}

	// 找到需要保留的 chunks
	var newChunks []fileChunk
	for _, chunk := range fc.chunks {
		if chunk.offset >= size {
			// 完全在截断点后的 chunk 被删除
			continue
		}

		if chunk.offset+chunk.size > size {
			// 跨越截断点的 chunk 需要调整大小
			adjustedChunk := chunk
			adjustedChunk.size = size - chunk.offset
			newChunks = append(newChunks, adjustedChunk)
		} else {
			// 完全在截断点前的 chunk 保持不变
			newChunks = append(newChunks, chunk)
		}
	}

	// 3. 更新 chunks 并重建线段树
	fc.chunks = newChunks
	// fc.buildSegmentTree()
	fc.updateMaxEndChunkIndex()

	return nil
}

func (fc *fileContent) Read(fs *SQLiteFS, fileID EntryID, p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 从 segment tree 中查询
	chunkSegments := fc.chunkIndex.QueryRange(offset, offset+int64(len(p)))

	fmt.Printf("Read: 查询范围 [%d, %d), 找到 %d 个段\n", offset, offset+int64(len(p)), len(chunkSegments))

	// 如果没有找到任何段，返回错误
	if len(chunkSegments) == 0 {
		return 0, fmt.Errorf("no chunks found for range [%d, %d)", offset, offset+int64(len(p)))
	}

	// 用于跟踪已读取的字节数
	totalBytesRead := 0

	// 遍历所有段
	for _, seg := range chunkSegments {
		// 获取对应的 chunk
		chunk := fc.chunks[seg.ChunkIndex]

		// 计算在当前 chunk 中的读取范围
		chunkStart := chunk.offset + seg.Delta
		chunkEnd := chunk.offset + chunk.size
		readStart := max(offset, chunkStart)
		readEnd := min(offset+int64(len(p)), chunkEnd)

		// 计算读取的字节数和在目标缓冲区中的偏移
		bytesToRead := int(readEnd - readStart)
		// bufOffset := int(readStart - offset)
		chunkOffset := int(readStart - chunk.offset)

		fmt.Printf("读取 block %d: offset=%d, size=%d, chunkOffset=%d, bytesToRead=%d\n",
			chunk.blockID, chunk.blockOffset, chunk.size, chunkOffset, bytesToRead)
	}
	panic("implement me")
	return totalBytesRead, nil
}

// Write implements io.Writer interface
func (fc *fileContent) Write(fs *SQLiteFS, fileID EntryID, p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	reqID := len(fc.chunks)
	// 写入数据库
	result := fs.s.FileWrite(fileID, int64(reqID), p, offset)
	rs, err := result.Wait()
	if err != nil {
		return 0, err
	}

	// 更新内存中的数据
	chunk := fileChunk{
		offset:      offset,
		size:        int64(len(p)),
		blockID:     int64(rs.BlockID),
		blockOffset: int64(rs.BlockOffset),
	}

	fmt.Printf("Write: 写入的 chunk 为: offset=%d, size=%d, blockID=%d, blockOffset=%d\n",
		chunk.offset, chunk.size, chunk.blockID, chunk.blockOffset)

	// 添加新chunk
	newIndex := len(fc.chunks)
	fc.chunks = append(fc.chunks, chunk)

	// 检查是否需要更新 maxEndChunkIndex
	newEnd := chunk.offset + chunk.size
	if fc.endChunkIndex < 0 || newEnd > fc.chunks[fc.endChunkIndex].offset+fc.chunks[fc.endChunkIndex].size {
		fc.endChunkIndex = newIndex
	}

	// 更新到 chunkIndex
	fc.chunkIndex.UpdateChunks([]fileChunk{chunk})

	// fc.buildSegmentTree()
	return int(rs.BytesWritten), nil
}
