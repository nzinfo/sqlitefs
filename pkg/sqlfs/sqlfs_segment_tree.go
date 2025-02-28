package sqlfs

import (
	"fmt"
)

// ChunkSegment 表示文件中的一个线段，对应一个chunk的一部分
type ChunkSegment struct {
	Start      int64 // 线段在文件中的起始位置
	End        int64 // 线段在文件中的结束位置（不包含）
	ChunkIndex int   // 对应的chunk在fileContent.chunks中的索引
	Delta      int64 // 线段起始位置相对于chunk起始位置的偏移量
}

// AVLNode 表示AVL树中的一个节点
type AVLNode struct {
	Segment     ChunkSegment // 节点存储的线段
	Height      int          // 节点高度
	Left, Right *AVLNode     // 左右子树
	MaxEnd      int64        // 子树中所有节点的最大结束位置
}

// SegmentTree 实现一个用于管理文件段的区间树（基于AVL树）
type SegmentTree struct {
	root       *AVLNode // AVL树的根节点
	chunkCount int      // 记录已添加的chunk数量，用于增量更新时确定新chunk的索引
}

// NewSegmentTree 从一组chunks创建线段树
func NewSegmentTree(chunks []fileChunk) *SegmentTree {
	st := &SegmentTree{}

	if len(chunks) == 0 {
		return st
	}

	// 创建线段并插入树中
	for i, chunk := range chunks {
		segment := ChunkSegment{
			Start:      chunk.offset,
			End:        chunk.offset + chunk.size,
			ChunkIndex: i,
			Delta:      0, // 初始时delta为0，表示从chunk的起始位置开始
		}
		st.root = st.insertNode(st.root, segment)
	}

	st.chunkCount = len(chunks)
	return st
}

// UpdateChunks 增量更新线段树，添加新的chunks
// 注意：新chunks的写入时间一定比之前的chunks晚，且按写入时间排序
func (st *SegmentTree) UpdateChunks(newChunks []fileChunk) {
	if len(newChunks) == 0 {
		return
	}

	// 创建新的线段并插入树中
	startIndex := st.chunkCount
	for i, chunk := range newChunks {
		segment := ChunkSegment{
			Start:      chunk.offset,
			End:        chunk.offset + chunk.size,
			ChunkIndex: startIndex + i,
			Delta:      0,
		}
		st.root = st.insertNode(st.root, segment)
	}

	// 更新chunk计数
	st.chunkCount += len(newChunks)
}

// QueryRange 查询指定范围内的所有线段
func (st *SegmentTree) QueryRange(start, end int64) []ChunkSegment {
	if st.root == nil {
		return nil
	}

	// 查询范围内的所有线段
	var result []ChunkSegment
	st.queryRangeRecursive(st.root, start, end, &result)

	// 对结果按照起始位置排序，然后按ChunkIndex排序（保证最新的在后面）
	sortSegments(result)

	// 合并重叠的段，保留最新的数据
	result = st.mergeOverlappingSegments(result)

	return result
}

// SegmentsToRead 返回需要读取的段以满足读取请求
func (st *SegmentTree) SegmentsToRead(start, end int64) []ChunkSegment {
	// 首先查询范围内的所有段
	segments := st.QueryRange(start, end)

	// 合并重叠的段，保留最新的数据
	mergedSegments := st.mergeOverlappingSegments(segments)

	return mergedSegments
}

// PrintSegmentTree 打印线段树结构（用于调试）
func (st *SegmentTree) PrintSegmentTree() {
	fmt.Println("SegmentTree:")
	if st.root == nil {
		fmt.Println("  <empty>")
		return
	}
	st.printNode(st.root, 0)
}

// 辅助方法

// height 返回节点的高度，如果节点为nil则返回-1
func height(node *AVLNode) int {
	if node == nil {
		return -1
	}
	return node.Height
}

/*
// max 返回两个数中的较大值
func max[T int | int64](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// min 返回两个数中的较小值
func min[T int | int64](a, b T) T {
	if a < b {
		return a
	}
	return b
}
*/

// updateHeight 更新节点的高度
func updateHeight(node *AVLNode) {
	node.Height = 1 + max(height(node.Left), height(node.Right))
}

// updateMaxEnd 更新节点的MaxEnd值
func updateMaxEnd(node *AVLNode) {
	node.MaxEnd = node.Segment.End

	if node.Left != nil {
		node.MaxEnd = max(node.MaxEnd, node.Left.MaxEnd)
	}

	if node.Right != nil {
		node.MaxEnd = max(node.MaxEnd, node.Right.MaxEnd)
	}
}

// getBalance 获取节点的平衡因子
func getBalance(node *AVLNode) int {
	if node == nil {
		return 0
	}
	return height(node.Left) - height(node.Right)
}

// rotateRight 右旋转
func rotateRight(y *AVLNode) *AVLNode {
	x := y.Left
	T2 := x.Right

	// 执行旋转
	x.Right = y
	y.Left = T2

	// 更新高度和MaxEnd
	updateHeight(y)
	updateMaxEnd(y)
	updateHeight(x)
	updateMaxEnd(x)

	return x
}

// rotateLeft 左旋转
func rotateLeft(x *AVLNode) *AVLNode {
	y := x.Right
	T2 := y.Left

	// 执行旋转
	y.Left = x
	x.Right = T2

	// 更新高度和MaxEnd
	updateHeight(x)
	updateMaxEnd(x)
	updateHeight(y)
	updateMaxEnd(y)

	return y
}

// insertNode 将一个线段插入AVL树
func (st *SegmentTree) insertNode(node *AVLNode, segment ChunkSegment) *AVLNode {
	// 执行标准的BST插入
	if node == nil {
		return &AVLNode{
			Segment: segment,
			Height:  0,
			MaxEnd:  segment.End,
		}
	}

	// 按照Start值决定插入左子树还是右子树
	if segment.Start < node.Segment.Start {
		node.Left = st.insertNode(node.Left, segment)
	} else {
		node.Right = st.insertNode(node.Right, segment)
	}

	// 更新当前节点的高度和MaxEnd
	updateHeight(node)
	updateMaxEnd(node)

	// 获取平衡因子
	balance := getBalance(node)

	// 左左情况
	if balance > 1 && segment.Start < node.Left.Segment.Start {
		return rotateRight(node)
	}

	// 右右情况
	if balance < -1 && segment.Start > node.Right.Segment.Start {
		return rotateLeft(node)
	}

	// 左右情况
	if balance > 1 && segment.Start > node.Left.Segment.Start {
		node.Left = rotateLeft(node.Left)
		return rotateRight(node)
	}

	// 右左情况
	if balance < -1 && segment.Start < node.Right.Segment.Start {
		node.Right = rotateRight(node.Right)
		return rotateLeft(node)
	}

	// 返回未更改的节点指针
	return node
}

/*
// overlaps 检查两个线段是否重叠
func overlaps(a, b ChunkSegment) bool {
	return a.Start < b.End && b.Start < a.End
}
*/

// queryRangeRecursive 递归查询指定范围内的所有线段
func (st *SegmentTree) queryRangeRecursive(node *AVLNode, start, end int64, result *[]ChunkSegment) {
	if node == nil {
		return
	}

	// 如果当前节点的MaxEnd小于查询范围的开始，则无需继续搜索
	if node.MaxEnd <= start {
		return
	}

	// 递归搜索左子树
	if node.Left != nil {
		st.queryRangeRecursive(node.Left, start, end, result)
	}

	// 检查当前节点是否与查询范围重叠
	if node.Segment.End > start && node.Segment.Start < end {
		// 计算与查询范围的交集
		segStart := max(node.Segment.Start, start)
		segEnd := min(node.Segment.End, end)

		// 计算新的delta
		delta := node.Segment.Delta + (segStart - node.Segment.Start)

		// 创建新的线段
		querySeg := ChunkSegment{
			Start:      segStart,
			End:        segEnd,
			ChunkIndex: node.Segment.ChunkIndex,
			Delta:      delta,
		}
		*result = append(*result, querySeg)
	}

	// 如果当前节点的起始位置大于等于查询范围的结束，则无需搜索右子树
	if node.Segment.Start >= end {
		return
	}

	// 递归搜索右子树
	if node.Right != nil {
		st.queryRangeRecursive(node.Right, start, end, result)
	}
}

// sortSegments 对线段进行排序，先按起始位置排序，再按ChunkIndex排序
func sortSegments(segments []ChunkSegment) {
	// 首先按ChunkIndex排序，确保最新的chunk在后面
	for i := 0; i < len(segments); i++ {
		for j := i + 1; j < len(segments); j++ {
			if segments[i].ChunkIndex > segments[j].ChunkIndex {
				segments[i], segments[j] = segments[j], segments[i]
			}
		}
	}

	// 然后按Start排序
	for i := 0; i < len(segments); i++ {
		for j := i + 1; j < len(segments); j++ {
			if segments[i].Start > segments[j].Start {
				segments[i], segments[j] = segments[j], segments[i]
			}
		}
	}
}

// mergeOverlappingSegments 合并重叠的线段，保留最新的数据
func (st *SegmentTree) mergeOverlappingSegments(segments []ChunkSegment) []ChunkSegment {
	if len(segments) <= 1 {
		return segments
	}

	// 收集所有的断点
	breakpoints := make(map[int64]struct{})
	for _, seg := range segments {
		breakpoints[seg.Start] = struct{}{}
		breakpoints[seg.End] = struct{}{}
	}

	// 将断点转换为有序数组
	var points []int64
	for p := range breakpoints {
		points = append(points, p)
	}

	// 对断点进行排序
	for i := 0; i < len(points); i++ {
		for j := i + 1; j < len(points); j++ {
			if points[i] > points[j] {
				points[i], points[j] = points[j], points[i]
			}
		}
	}

	// 对于每个区间，找出覆盖它的最新段
	var result []ChunkSegment
	for i := 0; i < len(points)-1; i++ {
		start, end := points[i], points[i+1]
		if start == end {
			continue
		}

		// 找出覆盖当前区间的所有段
		var covering []ChunkSegment
		for _, seg := range segments {
			if seg.Start <= start && seg.End >= end {
				covering = append(covering, seg)
			}
		}

		// 如果没有段覆盖当前区间，跳过
		if len(covering) == 0 {
			continue
		}

		// 找出覆盖当前区间的最新段
		var latest ChunkSegment
		latestIndex := -1
		for _, seg := range covering {
			if seg.ChunkIndex > latestIndex {
				latest = seg
				latestIndex = seg.ChunkIndex
			}
		}

		// 创建新段
		result = append(result, ChunkSegment{
			Start:      start,
			End:        end,
			ChunkIndex: latest.ChunkIndex,
			Delta:      latest.Delta + (start - latest.Start),
		})
	}

	// 合并相邻的相同 ChunkIndex 的段
	if len(result) > 1 {
		merged := make([]ChunkSegment, 0, len(result))
		current := result[0]
		for i := 1; i < len(result); i++ {
			next := result[i]
			if current.ChunkIndex == next.ChunkIndex && current.End == next.Start {
				current.End = next.End
			} else {
				merged = append(merged, current)
				current = next
			}
		}
		merged = append(merged, current)
		result = merged
	}

	return result
}

// printNode 递归打印节点及其子树（用于调试）
func (st *SegmentTree) printNode(node *AVLNode, level int) {
	if node == nil {
		return
	}

	indent := ""
	for i := 0; i < level; i++ {
		indent += "  "
	}

	fmt.Printf("%s[%d,%d) ChunkIndex=%d Delta=%d Height=%d MaxEnd=%d\n",
		indent, node.Segment.Start, node.Segment.End, node.Segment.ChunkIndex,
		node.Segment.Delta, node.Height, node.MaxEnd)

	st.printNode(node.Left, level+1)
	st.printNode(node.Right, level+1)
}
