package sqlfs

import (
	"sort"
)

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
	tree          *segmentTree
	endChunkIndex int // 记录 offset + size 最大的 chunk 的索引
}

// segmentTree implements an interval tree for chunk management
type segmentTree struct {
	root *segmentNode
}

type segmentNode struct {
	start, end int64
	chunk      *fileChunk
	left       *segmentNode
	right      *segmentNode
}

// newFileContent creates a new fileContent from a slice of chunks
func newFileContent(chunks []fileChunk) *fileContent {
	// Sort chunks by offset to ensure proper ordering
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].offset < chunks[j].offset
	})

	fc := &fileContent{
		chunks: chunks,
	}
	fc.buildSegmentTree()
	fc.updateMaxEndChunkIndex()
	return fc
}

// buildSegmentTree constructs the segment tree from sorted chunks
func (fc *fileContent) buildSegmentTree() {
	if len(fc.chunks) == 0 {
		return
	}

	fc.tree = &segmentTree{}
	fc.tree.root = fc.buildTreeNode(0, len(fc.chunks)-1)
}

func (fc *fileContent) buildTreeNode(start, end int) *segmentNode {
	if start > end {
		return nil
	}

	node := &segmentNode{}

	if start == end {
		chunk := &fc.chunks[start]
		node.start = chunk.offset
		node.end = chunk.offset + chunk.size
		node.chunk = chunk
		return node
	}

	mid := (start + end) / 2
	node.left = fc.buildTreeNode(start, mid)
	node.right = fc.buildTreeNode(mid+1, end)

	node.start = node.left.start
	if node.right != nil {
		node.end = node.right.end
	} else {
		node.end = node.left.end
	}

	return node
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

// findChunkAt finds the chunk containing the given position
func (fc *fileContent) findChunkAt(position int64) *fileChunk {
	if fc.tree == nil || fc.tree.root == nil {
		return nil
	}
	return fc.findChunkInNode(fc.tree.root, position)
}

func (fc *fileContent) findChunkInNode(node *segmentNode, position int64) *fileChunk {
	if node == nil || position < node.start || position >= node.end {
		return nil
	}

	if node.chunk != nil {
		return node.chunk
	}

	if node.left != nil && position < node.left.end {
		return fc.findChunkInNode(node.left, position)
	}
	return fc.findChunkInNode(node.right, position)
}

// ChunkRange represents a range within a chunk
type ChunkRange struct {
	Chunk *fileChunk
	Start int64 // Start offset within the chunk
	End   int64 // End offset within the chunk
}

// findChunksInRange finds all chunks that overlap with the given range
func (fc *fileContent) findChunksInRange(start, end int64) []ChunkRange {
	if fc.tree == nil || fc.tree.root == nil {
		return nil
	}

	var ranges []ChunkRange
	fc.findRangesInNode(fc.tree.root, start, end, &ranges)
	return ranges
}

func (fc *fileContent) findRangesInNode(node *segmentNode, start, end int64, ranges *[]ChunkRange) {
	if node == nil || end < node.start || start >= node.end {
		return
	}

	if node.chunk != nil {
		// Calculate the overlap range
		rangeStart := max(start, node.chunk.offset)
		rangeEnd := min(end, node.chunk.offset+node.chunk.size)

		*ranges = append(*ranges, ChunkRange{
			Chunk: node.chunk,
			Start: rangeStart - node.chunk.offset, // Convert to chunk-relative offset
			End:   rangeEnd - node.chunk.offset,   // Convert to chunk-relative offset
		})
		return
	}

	fc.findRangesInNode(node.left, start, end, ranges)
	fc.findRangesInNode(node.right, start, end, ranges)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
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
		fc.tree = nil
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
	fc.buildSegmentTree()
	fc.updateMaxEndChunkIndex()

	return nil
}

// Write implements io.Writer interface
func (fc *fileContent) Write(fs *SQLiteFS, fileID EntryID, p []byte, offset int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 写入数据库
	result := fs.s.FileWrite(fileID, p, offset)
	written, err := result.Wait()
	if err != nil {
		return 0, err
	}

	// 更新内存中的数据
	chunk := fileChunk{
		offset: offset,
		size:   int64(len(p)),
	}

	// 添加新chunk
	newIndex := len(fc.chunks)
	fc.chunks = append(fc.chunks, chunk)

	// 检查是否需要更新 maxEndChunkIndex
	newEnd := chunk.offset + chunk.size
	if fc.endChunkIndex < 0 || newEnd > fc.chunks[fc.endChunkIndex].offset+fc.chunks[fc.endChunkIndex].size {
		fc.endChunkIndex = newIndex
	}

	fc.buildSegmentTree()
	return written, nil
}
