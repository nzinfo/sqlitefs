package sqlfs

import (
	"fmt"
	"sort"
)

// ChunkSegment 表示文件中的一个线段，对应一个chunk的一部分
type ChunkSegment struct {
	Start      int64 // 线段在文件中的起始位置
	End        int64 // 线段在文件中的结束位置（不包含）
	ChunkIndex int   // 对应的chunk在fileContent.chunks中的索引
	Delta      int64 // 线段起始位置相对于chunk起始位置的偏移量
	// Chunk      *fileChunk // 指向原始chunk的指针，方便直接访问
}

// SegmentTree 实现一个用于管理文件段的线段树
type SegmentTree struct {
	root *SegmentNode
}

// SegmentNode 线段树的节点
type SegmentNode struct {
	Start    int64          // 节点表示的线段起始位置
	End      int64          // 节点表示的线段结束位置（不包含）
	Segments []ChunkSegment // 覆盖此节点范围的所有线段，按时间顺序排序（最新的在最后）
	Left     *SegmentNode   // 左子树
	Right    *SegmentNode   // 右子树
}

// NewSegmentTree 创建一个新的线段树
func NewSegmentTree(chunks []fileChunk) *SegmentTree {
	if len(chunks) == 0 {
		return &SegmentTree{}
	}

	// 确定文件的最大范围
	var maxEnd int64 = 0
	for _, chunk := range chunks {
		end := chunk.offset + chunk.size
		if end > maxEnd {
			maxEnd = end
		}
	}

	// 创建线段
	segments := make([]ChunkSegment, len(chunks))
	for i, chunk := range chunks {
		segments[i] = ChunkSegment{
			Start:      chunk.offset,
			End:        chunk.offset + chunk.size,
			ChunkIndex: i,
			Delta:      0, // 初始时delta为0，表示从chunk的起始位置开始
			//Chunk:      &chunks[i],
		}
	}

	// 构建线段树
	tree := &SegmentTree{}
	if maxEnd > 0 {
		tree.root = buildSegmentTree(0, maxEnd, segments)
	}
	return tree
}

// buildSegmentTree 递归构建线段树
func buildSegmentTree(start, end int64, segments []ChunkSegment) *SegmentNode {
	node := &SegmentNode{
		Start: start,
		End:   end,
	}

	// 找出所有与当前节点范围有交集的线段
	var nodeSegments []ChunkSegment
	for _, seg := range segments {
		if seg.End > start && seg.Start < end {
			// 计算与当前节点的交集
			segStart := max(seg.Start, start)
			segEnd := min(seg.End, end)

			// 计算新的delta
			delta := seg.Delta + (segStart - seg.Start)

			// 创建新的线段
			nodeSeg := ChunkSegment{
				Start:      segStart,
				End:        segEnd,
				ChunkIndex: seg.ChunkIndex,
				Delta:      delta,
				//Chunk:      seg.Chunk,
			}
			nodeSegments = append(nodeSegments, nodeSeg)
		}
	}

	// 将线段按ChunkIndex排序，保证最新的在最后
	sort.Slice(nodeSegments, func(i, j int) bool {
		return nodeSegments[i].ChunkIndex < nodeSegments[j].ChunkIndex
	})

	node.Segments = nodeSegments

	// 如果是叶子节点，不再继续分割
	if end-start <= 1 || len(nodeSegments) <= 1 {
		return node
	}

	// 分割节点
	mid := start + (end-start)/2
	node.Left = buildSegmentTree(start, mid, segments)
	node.Right = buildSegmentTree(mid, end, segments)

	return node
}

// QueryRange 查询指定范围内的所有线段
// 返回的线段按照时间顺序排序，最新的在最后
func (st *SegmentTree) QueryRange(start, end int64) []ChunkSegment {
	if st.root == nil {
		return nil
	}

	var result []ChunkSegment
	querySegmentTree(st.root, start, end, &result)

	// 对结果按照起始位置排序，然后按ChunkIndex排序（保证最新的在后面）
	sort.Slice(result, func(i, j int) bool {
		if result[i].Start != result[j].Start {
			return result[i].Start < result[j].Start
		}
		return result[i].ChunkIndex < result[j].ChunkIndex
	})

	return result
}

// querySegmentTree 递归查询线段树
func querySegmentTree(node *SegmentNode, start, end int64, result *[]ChunkSegment) {
	if node == nil || start >= node.End || end <= node.Start {
		return
	}

	// 如果查询范围完全包含当前节点范围
	if start <= node.Start && end >= node.End {
		*result = append(*result, node.Segments...)
		return
	}

	// 查询左右子树
	querySegmentTree(node.Left, start, end, result)
	querySegmentTree(node.Right, start, end, result)

	// 处理当前节点的线段
	for _, seg := range node.Segments {
		if seg.End > start && seg.Start < end {
			// 计算与查询范围的交集
			segStart := max(seg.Start, start)
			segEnd := min(seg.End, end)

			// 计算新的delta
			delta := seg.Delta + (segStart - seg.Start)

			// 创建新的线段
			querySeg := ChunkSegment{
				Start:      segStart,
				End:        segEnd,
				ChunkIndex: seg.ChunkIndex,
				Delta:      delta,
				// Chunk:      seg.Chunk,
			}
			*result = append(*result, querySeg)
		}
	}
}

// MergeOverlappingSegments 合并重叠的线段，保留最新的数据
func MergeOverlappingSegments(segments []ChunkSegment) []ChunkSegment {
	if len(segments) <= 1 {
		return segments
	}

	// 按起始位置排序
	sort.Slice(segments, func(i, j int) bool {
		if segments[i].Start != segments[j].Start {
			return segments[i].Start < segments[j].Start
		}
		return segments[i].ChunkIndex < segments[j].ChunkIndex
	})

	var result []ChunkSegment
	var current ChunkSegment = segments[0]

	for i := 1; i < len(segments); i++ {
		// 如果当前线段与下一个线段不重叠
		if current.End <= segments[i].Start {
			result = append(result, current)
			current = segments[i]
			continue
		}

		// 如果下一个线段的ChunkIndex更大（更新），则更新当前线段
		if segments[i].ChunkIndex > current.ChunkIndex {
			// 处理重叠部分
			if segments[i].Start > current.Start {
				// 保留当前线段的非重叠部分
				nonOverlap := ChunkSegment{
					Start:      current.Start,
					End:        segments[i].Start,
					ChunkIndex: current.ChunkIndex,
					Delta:      current.Delta,
					//Chunk:      current.Chunk,
				}
				result = append(result, nonOverlap)
			}

			// 如果下一个线段完全覆盖当前线段的剩余部分
			if segments[i].End >= current.End {
				current = segments[i]
			} else {
				// 下一个线段只覆盖部分，需要分割
				overlap := ChunkSegment{
					Start:      segments[i].Start,
					End:        segments[i].End,
					ChunkIndex: segments[i].ChunkIndex,
					Delta:      segments[i].Delta,
					//Chunk:      segments[i].Chunk,
				}
				result = append(result, overlap)

				// 更新当前线段为剩余部分
				current.Start = segments[i].End
				current.Delta = current.Delta + (segments[i].End - current.Start)
			}
		} else {
			// 如果当前线段的ChunkIndex更大，则忽略下一个线段的重叠部分
			if current.End < segments[i].End {
				// 但需要考虑下一个线段的非重叠部分
				nonOverlap := ChunkSegment{
					Start:      current.End,
					End:        segments[i].End,
					ChunkIndex: segments[i].ChunkIndex,
					Delta:      segments[i].Delta + (current.End - segments[i].Start),
					//Chunk:      segments[i].Chunk,
				}
				segments[i] = nonOverlap
				i-- // 重新处理这个线段
			}
		}
	}

	// 添加最后一个线段
	result = append(result, current)

	return result
}

// ReadData 从多个线段中读取数据
func ReadData(segments []ChunkSegment, buffer []byte) (int, error) {
	// 首先合并重叠的线段，保留最新的数据
	mergedSegments := MergeOverlappingSegments(segments)

	totalRead := 0
	bufferOffset := 0

	for _, seg := range mergedSegments {
		// 计算需要从chunk中读取的起始位置和长度
		chunkOffset := seg.Delta
		length := int(seg.End - seg.Start)

		if bufferOffset+length > len(buffer) {
			length = len(buffer) - bufferOffset
			if length <= 0 {
				break
			}
		}

		// 从数据库中读取数据
		// 这里需要实际实现从数据库读取数据的逻辑
		// 例如：
		// blockData := readBlockData(seg.Chunk.blockID)
		// copy(buffer[bufferOffset:bufferOffset+length], blockData[chunkOffset:chunkOffset+int64(length)])

		// 更新已读取的总字节数和缓冲区偏移量
		totalRead += length
		bufferOffset += length

		if bufferOffset >= len(buffer) {
			break
		}
	}

	return totalRead, nil
}

// 辅助函数
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// PrintSegmentTree 打印线段树结构（用于调试）
func (st *SegmentTree) PrintSegmentTree() {
	if st.root == nil {
		fmt.Println("Empty segment tree")
		return
	}

	printNode(st.root, 0)
}

// printNode 递归打印节点
func printNode(node *SegmentNode, level int) {
	if node == nil {
		return
	}

	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	fmt.Printf("%sNode [%d, %d) with %d segments\n", indent, node.Start, node.End, len(node.Segments))

	for i, seg := range node.Segments {
		fmt.Printf("%s  Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d\n",
			indent, i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
	}

	printNode(node.Left, level+1)
	printNode(node.Right, level+1)
}
