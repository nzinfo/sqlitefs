package sqlfs

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// 基础功能测试
func TestSegmentTreeBasic(t *testing.T) {
	t.Run("Empty tree", func(t *testing.T) {
		st := NewSegmentTree(nil)
		assert.Equal(t, 0, len(st.segments))
	})

	t.Run("Single chunk", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 100}})
		segments := st.QueryRange(0, 100)
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, int64(0), segments[0].Start)
		assert.Equal(t, int64(100), segments[0].End)
	})

	t.Run("Non-overlapping chunks", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},
			{offset: 200, size: 100},
		})
		segments := st.QueryRange(0, 300)
		assert.Equal(t, 2, len(segments))
	})

	t.Run("Fully overlapping chunks", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},
			{offset: 0, size: 100},
		})
		segments := st.QueryRange(0, 100)
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, 1, segments[0].ChunkIndex)
	})

	t.Run("Partially overlapping chunks", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},
			{offset: 50, size: 100},
		})
		segments := st.QueryRange(0, 150)
		assert.Equal(t, 2, len(segments))
	})
}

// 边界条件测试
func TestSegmentTreeEdgeCases(t *testing.T) {
	t.Run("Zero length chunk", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 0}})
		segments := st.QueryRange(0, 0)
		assert.Equal(t, 0, len(segments))
	})

	t.Run("Large chunk", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 1<<63 - 1}})
		segments := st.QueryRange(0, 1<<63 - 1)
		assert.Equal(t, 1, len(segments))
	})

	t.Run("Negative position", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: -100, size: 100}})
		segments := st.QueryRange(-100, 0)
		assert.Equal(t, 1, len(segments))
	})

	t.Run("Duplicate chunks", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},
			{offset: 0, size: 100},
		})
		segments := st.QueryRange(0, 100)
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, 1, segments[0].ChunkIndex)
	})
}

// 增量更新测试
func TestSegmentTreeIncrementalUpdate(t *testing.T) {
	t.Run("Update empty tree", func(t *testing.T) {
		st := NewSegmentTree(nil)
		st.UpdateChunks([]fileChunk{{offset: 0, size: 100}})
		segments := st.QueryRange(0, 100)
		assert.Equal(t, 1, len(segments))
	})

	t.Run("Update non-empty tree", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 100}})
		st.UpdateChunks([]fileChunk{{offset: 100, size: 100}})
		segments := st.QueryRange(0, 200)
		assert.Equal(t, 2, len(segments))
	})

	t.Run("Update causing overlap", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 100}})
		st.UpdateChunks([]fileChunk{{offset: 50, size: 100}})
		segments := st.QueryRange(0, 150)
		assert.Equal(t, 2, len(segments))
	})

	t.Run("Update causing complete coverage", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{{offset: 0, size: 100}})
		st.UpdateChunks([]fileChunk{{offset: 0, size: 200}})
		segments := st.QueryRange(0, 200)
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, 1, segments[0].ChunkIndex)
	})
}

// 性能测试
func TestSegmentTreePerformance(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// 生成大量随机 chunk
	numChunks := 10000
	chunks := make([]fileChunk, numChunks)
	for i := 0; i < numChunks; i++ {
		start := rand.Int63n(1 << 30)
		end := start + rand.Int63n(1<<20)
		chunks[i] = fileChunk{offset: start, size: end - start}
	}

	// 测试构建性能
	start := time.Now()
	st := NewSegmentTree(chunks)
	duration := time.Since(start)
	t.Logf("Built segment tree with %d chunks in %v", numChunks, duration)

	// 测试查询性能
	start = time.Now()
	for i := 0; i < 1000; i++ {
		startPos := rand.Int63n(1 << 30)
		endPos := startPos + rand.Int63n(1<<20)
		st.QueryRange(startPos, endPos)
	}
	duration = time.Since(start)
	t.Logf("Performed 1000 queries in %v", duration)

	// 测试增量更新性能
	start = time.Now()
	for i := 0; i < 1000; i++ {
		startPos := rand.Int63n(1 << 30)
		endPos := startPos + rand.Int63n(1<<20)
		st.UpdateChunks([]fileChunk{{offset: startPos, size: endPos - startPos}})
	}
	duration = time.Since(start)
	t.Logf("Performed 1000 updates in %v", duration)
}

// TestSegmentTreeComplexScenarios 测试更复杂的场景
func TestSegmentTreeComplexScenarios(t *testing.T) {
	t.Run("Multiple overlapping chunks with gaps", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},   // [0, 100)
			{offset: 50, size: 100},  // [50, 150)
			{offset: 200, size: 100}, // [200, 300)
			{offset: 250, size: 100}, // [250, 350)
		})

		// 查询包含所有段
		segments := st.QueryRange(0, 400)
		assert.Equal(t, 4, len(segments))

		// 查询跨越空隙的范围
		segments = st.QueryRange(100, 250)
		assert.Equal(t, 2, len(segments))
		assert.Equal(t, int64(100), segments[0].Start)
		assert.Equal(t, int64(150), segments[0].End)
		assert.Equal(t, int64(200), segments[1].Start)
		assert.Equal(t, int64(250), segments[1].End)
	})

	t.Run("Incremental updates with complex overlaps", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},   // [0, 100)
			{offset: 200, size: 100}, // [200, 300)
		})

		// 添加部分重叠的chunks
		st.UpdateChunks([]fileChunk{
			{offset: 50, size: 200},  // [50, 250)
			{offset: 150, size: 100}, // [150, 250)
		})

		segments := st.QueryRange(0, 300)
		// 我们的通用算法实现会返回4个段
		assert.Equal(t, 4, len(segments))

		// 验证段的顺序和范围
		assert.Equal(t, int64(0), segments[0].Start)
		assert.Equal(t, int64(50), segments[0].End)
		assert.Equal(t, int64(50), segments[1].Start)
		assert.Equal(t, int64(150), segments[1].End)
		assert.Equal(t, int64(150), segments[2].Start)
		assert.Equal(t, int64(250), segments[2].End)
		assert.Equal(t, int64(250), segments[3].Start)
		assert.Equal(t, int64(300), segments[3].End)
	})

	t.Run("Multiple updates with complete overlaps", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100}, // [0, 100)
		})

		// 完全覆盖第一个chunk
		st.UpdateChunks([]fileChunk{
			{offset: 0, size: 100}, // [0, 100)
		})

		// 再次完全覆盖
		st.UpdateChunks([]fileChunk{
			{offset: 0, size: 100}, // [0, 100)
		})

		segments := st.QueryRange(0, 100)
		assert.Equal(t, 1, len(segments))
		assert.Equal(t, 2, segments[0].ChunkIndex) // 应该是最新的chunk
	})

	t.Run("Complex delta calculations", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},   // [0, 100)
			{offset: 50, size: 100},  // [50, 150)
			{offset: 200, size: 100}, // [200, 300)
		})

		// 查询部分重叠的范围
		segments := st.QueryRange(25, 125)
		assert.Equal(t, 2, len(segments))

		// 验证delta计算
		assert.Equal(t, int64(25), segments[0].Delta)  // 从0开始，偏移25
		assert.Equal(t, int64(0), segments[1].Delta)   // 从50开始，无需偏移
	})

	t.Run("Edge case with adjacent segments", func(t *testing.T) {
		st := NewSegmentTree([]fileChunk{
			{offset: 0, size: 100},   // [0, 100)
			{offset: 100, size: 100}, // [100, 200)
			{offset: 200, size: 100}, // [200, 300)
		})

		// 更新中间的chunk
		st.UpdateChunks([]fileChunk{
			{offset: 100, size: 100}, // [100, 200)
		})

		segments := st.QueryRange(0, 300)
		assert.Equal(t, 3, len(segments))

		// 验证相邻段的连接
		assert.Equal(t, segments[0].End, segments[1].Start)
		assert.Equal(t, segments[1].End, segments[2].Start)
	})
}
