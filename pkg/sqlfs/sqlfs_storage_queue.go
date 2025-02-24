//go:build !proto

package sqlfs

func (s *storage) LoadEntry(entryID int64) *AsyncResult[*fileInfo] {
	result := NewAsyncResult[*fileInfo]()
	return result
}

func (s *storage) LoadEntriesByParent(parentID int64) *AsyncResult[[]fileInfo] {
	result := NewAsyncResult[[]fileInfo]()
	return result
}
