package sqlfs

import (
	"testing"
)

func TestNewSegmentTree(t *testing.T) {
	chunks := []fileChunk{
		{offset: 0, size: 100},
		{offset: 50, size: 100},
		{offset: 200, size: 25},
	}

	st := NewSegmentTree(chunks)
	if st == nil {
		t.Fatal("Failed to create segment tree")
	}

	if len(st.segments) != 3 {
		t.Errorf("Expected 3 segments, got %d", len(st.segments))
	}

	if st.chunkCount != 3 {
		t.Errorf("Expected chunkCount to be 3, got %d", st.chunkCount)
	}
}

func TestQueryRange(t *testing.T) {
	chunks := []fileChunk{
		{offset: 0, size: 100},
		{offset: 50, size: 100},
		{offset: 200, size: 25},
	}

	st := NewSegmentTree(chunks)

	t.Run("Query entire range", func(t *testing.T) {
		segments := st.QueryRange(0, 350)

		// We expect to get all segments that overlap with the range [0, 350)
		expectedCount := 3
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d for range [0, 350)", expectedCount, len(segments))
		}

		for i, seg := range segments {
			t.Logf("Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d", i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
		}
	})

	t.Run("Query first chunk only", func(t *testing.T) {
		segments := st.QueryRange(0, 50)

		// We expect to get only the first chunk
		expectedCount := 1
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d for range [0, 50)", expectedCount, len(segments))
		}

		if len(segments) > 0 && segments[0].ChunkIndex != 0 {
			t.Errorf("Expected ChunkIndex 0, got %d", segments[0].ChunkIndex)
		}
	})

	t.Run("Query overlapping chunks", func(t *testing.T) {
		segments := st.QueryRange(50, 100)

		// We expect to get both chunks that overlap with [50, 100)
		expectedCount := 2
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d for range [50, 100)", expectedCount, len(segments))
		}
	})

	t.Run("Query gap between chunks", func(t *testing.T) {
		segments := st.QueryRange(150, 200)

		// 第二个chunk的范围是[50, 150)，第三个chunk的范围是[200, 225)
		// 所以查询范围[150, 200)实际上不与任何chunk重叠
		expectedCount := 0
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d for range [150, 200)", expectedCount, len(segments))
			for i, seg := range segments {
				t.Logf("Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d", i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
			}
			return // Skip the rest of the test if we don't have the expected segments
		}
	})

	t.Run("Query partial overlap", func(t *testing.T) {
		segments := st.QueryRange(210, 220)

		// We expect to get only the third chunk
		expectedCount := 1
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d for range [210, 220)", expectedCount, len(segments))
			return // Skip the rest of the test if we don't have the expected segments
		}

		if len(segments) > 0 && segments[0].ChunkIndex != 2 {
			t.Errorf("Expected ChunkIndex 2, got %d", segments[0].ChunkIndex)
		}

		if len(segments) > 0 && segments[0].Delta != 10 {
			t.Errorf("Expected Delta 10, got %d", segments[0].Delta)
		}
	})
}

func TestUpdateChunks(t *testing.T) {
	chunks := []fileChunk{
		{offset: 0, size: 100},
	}

	st := NewSegmentTree(chunks)

	newChunks := []fileChunk{
		{offset: 50, size: 100},
		{offset: 200, size: 25},
	}

	st.UpdateChunks(newChunks)

	if st.chunkCount != 3 {
		t.Errorf("Expected chunkCount to be 3, got %d", st.chunkCount)
	}

	if len(st.segments) != 3 {
		t.Errorf("Expected 3 segments, got %d", len(st.segments))
	}

	// Test querying after update
	segments := st.QueryRange(0, 350)
	expectedCount := 3
	if len(segments) != expectedCount {
		t.Errorf("Expected %d segments, got %d for range [0, 350)", expectedCount, len(segments))
	}
}

func TestGetSegmentsToRead(t *testing.T) {
	chunks := []fileChunk{
		{offset: 0, size: 100},
		{offset: 50, size: 100}, // Overlaps with first chunk
		{offset: 200, size: 25},
	}

	st := NewSegmentTree(chunks)

	t.Run("Read overlapping chunks", func(t *testing.T) {
		segments := st.SegmentsToRead(0, 150)

		// After merging, we should have two segments:
		// 1. [0, 50) from chunk 0
		// 2. [50, 150) from chunk 1
		expectedCount := 2
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments after merging, got %d", expectedCount, len(segments))
			return // Skip the rest of the test if we don't have the expected segments
		}

		for i, seg := range segments {
			t.Logf("Merged Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d", i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
		}

		// First segment should be from chunk 0
		if len(segments) > 0 && segments[0].ChunkIndex != 0 {
			t.Errorf("Expected first segment to be from chunk 0, got %d", segments[0].ChunkIndex)
		}

		// Second segment should be from chunk 1
		if len(segments) > 1 && segments[1].ChunkIndex != 1 {
			t.Errorf("Expected second segment to be from chunk 1, got %d", segments[1].ChunkIndex)
		}
	})

	t.Run("Read non-overlapping chunks", func(t *testing.T) {
		segments := st.SegmentsToRead(150, 225)

		// 第二个chunk的范围是[50, 150),第三个chunk的范围是[200, 225)
		// 所以查询范围[150, 200)不与任何chunk重叠，[200, 225)与第三个chunk重叠
		// 因此我们只应该得到一个段
		expectedCount := 1
		if len(segments) != expectedCount {
			t.Errorf("Expected %d segments, got %d", expectedCount, len(segments))
			for i, seg := range segments {
				t.Logf("Segment %d: [%d, %d), ChunkIndex=%d, Delta=%d", i, seg.Start, seg.End, seg.ChunkIndex, seg.Delta)
			}
			return // Skip the rest of the test if we don't have the expected segments
		}

		// 唯一的段应该是从chunk 2开始的
		if len(segments) > 0 && segments[0].ChunkIndex != 2 {
			t.Errorf("Expected segment to be from chunk 2, got %d", segments[0].ChunkIndex)
		}
	})
}

func TestMergeOverlappingSegments(t *testing.T) {
	segments := []ChunkSegment{
		{Start: 0, End: 100, ChunkIndex: 0, Delta: 0},
		{Start: 50, End: 150, ChunkIndex: 1, Delta: 0},
		{Start: 200, End: 250, ChunkIndex: 2, Delta: 0},
	}

	merged := MergeOverlappingSegments(segments)

	// After merging, we should have:
	// 1. [0, 50) from chunk 0
	// 2. [50, 150) from chunk 1
	// 3. [200, 250) from chunk 2
	expectedCount := 3
	if len(merged) != expectedCount {
		t.Errorf("Expected %d segments after merging, got %d", expectedCount, len(merged))
		return // Skip the rest of the test if we don't have the expected segments
	}

	// Check first segment
	if len(merged) > 0 && (merged[0].Start != 0 || merged[0].End != 50 || merged[0].ChunkIndex != 0) {
		t.Errorf("First segment incorrect: got [%d, %d) ChunkIndex=%d, expected [0, 50) ChunkIndex=0",
			merged[0].Start, merged[0].End, merged[0].ChunkIndex)
	}

	// Check second segment
	if len(merged) > 1 && (merged[1].Start != 50 || merged[1].End != 150 || merged[1].ChunkIndex != 1) {
		t.Errorf("Second segment incorrect: got [%d, %d) ChunkIndex=%d, expected [50, 150) ChunkIndex=1",
			merged[1].Start, merged[1].End, merged[1].ChunkIndex)
	}

	// Check third segment
	if len(merged) > 2 && (merged[2].Start != 200 || merged[2].End != 250 || merged[2].ChunkIndex != 2) {
		t.Errorf("Third segment incorrect: got [%d, %d) ChunkIndex=%d, expected [200, 250) ChunkIndex=2",
			merged[2].Start, merged[2].End, merged[2].ChunkIndex)
	}
}

func TestEmptySegmentTree(t *testing.T) {
	st := NewSegmentTree(nil)

	if st == nil {
		t.Fatal("Failed to create empty segment tree")
	}

	if len(st.segments) != 0 {
		t.Errorf("Expected 0 segments, got %d", len(st.segments))
	}

	segments := st.QueryRange(0, 100)
	if len(segments) != 0 {
		t.Errorf("Expected 0 segments from query, got %d", len(segments))
	}

	// Test updating empty tree
	chunks := []fileChunk{
		{offset: 0, size: 100},
	}

	st.UpdateChunks(chunks)

	if len(st.segments) != 1 {
		t.Errorf("Expected 1 segment after update, got %d", len(st.segments))
	}

	segments = st.QueryRange(0, 100)
	if len(segments) != 1 {
		t.Errorf("Expected 1 segment from query after update, got %d", len(segments))
	}
}
