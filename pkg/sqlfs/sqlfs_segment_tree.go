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
}

// SegmentTree 实现一个用于管理文件段的线段树
type SegmentTree struct {
	// 存储所有的文件段，按ChunkIndex排序
	segments []ChunkSegment
	// 记录已添加的chunk数量，用于增量更新时确定新chunk的索引
	chunkCount int
}

// NewSegmentTree 从一组chunks创建线段树
func NewSegmentTree(chunks []fileChunk) *SegmentTree {
	if len(chunks) == 0 {
		return &SegmentTree{}
	}

	// 创建线段
	segments := make([]ChunkSegment, len(chunks))
	for i, chunk := range chunks {
		segments[i] = ChunkSegment{
			Start:      chunk.offset,
			End:        chunk.offset + chunk.size,
			ChunkIndex: i,
			Delta:      0, // 初始时delta为0，表示从chunk的起始位置开始
		}
	}

	// 构建线段树
	st := &SegmentTree{
		segments:   segments,
		chunkCount: len(chunks),
	}
	return st
}

// UpdateChunks 增量更新线段树，添加新的chunks
// 注意：新chunks的写入时间一定比之前的chunks晚，且按写入时间排序
func (st *SegmentTree) UpdateChunks(newChunks []fileChunk) {
	if len(newChunks) == 0 {
		return
	}

	// 如果线段树为空，直接创建新的线段树
	if len(st.segments) == 0 {
		*st = *NewSegmentTree(newChunks)
		return
	}

	// 创建新的线段
	startIndex := st.chunkCount
	newSegments := make([]ChunkSegment, len(newChunks))
	for i, chunk := range newChunks {
		newSegments[i] = ChunkSegment{
			Start:      chunk.offset,
			End:        chunk.offset + chunk.size,
			ChunkIndex: startIndex + i,
			Delta:      0,
		}
	}

	// 合并新旧线段
	st.segments = append(st.segments, newSegments...)

	// 更新chunk计数
	st.chunkCount += len(newChunks)
}

// QueryRange 查询指定范围内的所有线段
func (st *SegmentTree) QueryRange(start, end int64) []ChunkSegment {
	if len(st.segments) == 0 {
		return nil
	}

	var result []ChunkSegment
	for _, seg := range st.segments {
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
			}
			result = append(result, querySeg)
		}
	}

	// 对结果按照起始位置排序，然后按ChunkIndex排序（保证最新的在后面）
	sort.Slice(result, func(i, j int) bool {
		if result[i].Start != result[j].Start {
			return result[i].Start < result[j].Start
		}
		return result[i].ChunkIndex < result[j].ChunkIndex
	})

	return result
}

// SegmentsToRead 返回需要读取的段以满足读取请求
func (st *SegmentTree) SegmentsToRead(start, end int64) []ChunkSegment {
	// 首先查询范围内的所有段
	segments := st.QueryRange(start, end)

	// 合并重叠的段，保留最新的数据
	mergedSegments := MergeOverlappingSegments(segments)

	return mergedSegments
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
				nonOverlap := ChunkSegment{
					Start:      current.Start,
					End:        segments[i].Start,
					ChunkIndex: current.ChunkIndex,
					Delta:      current.Delta,
				}
				result = append(result, nonOverlap)
			}

			// 如果下一个线段完全覆盖当前线段的剩余部分
			if segments[i].End >= current.End {
				current = segments[i]
			} else {
				// 如果下一个线段只覆盖了当前线段的一部分
				overlap := ChunkSegment{
					Start:      segments[i].Start,
					End:        segments[i].End,
					ChunkIndex: segments[i].ChunkIndex,
					Delta:      segments[i].Delta,
				}
				result = append(result, overlap)

				// 更新当前线段为剩余部分
				current.Start = segments[i].End
				current.Delta = current.Delta + (segments[i].End - current.Start)
			}
		} else {
			// 如果下一个线段的ChunkIndex更小（更旧），则保留当前线段
			if segments[i].End > current.End {
				nonOverlap := ChunkSegment{
					Start:      current.End,
					End:        segments[i].End,
					ChunkIndex: segments[i].ChunkIndex,
					Delta:      segments[i].Delta + (current.End - segments[i].Start),
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

// 辅助函数
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// PrintSegmentTree 打印线段树结构（用于调试）
func (st *SegmentTree) PrintSegmentTree() {
	if len(st.segments) == 0 {
		fmt.Println("Empty segment tree")
		return
	}

	fmt.Printf("Segment Tree with %d segments:\n", len(st.segments))
	for i, seg := range st.segments {
		fmt.Printf("  Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d\n",
			i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
	}
}
