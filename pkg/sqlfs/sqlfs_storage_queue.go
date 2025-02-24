//go:build !proto

package sqlfs

func (s *storage) loadEntry(entryID int64) (*fileInfo, error) {

	return nil, nil
}

func (s *storage) loadEntriesByParentDir(parentID int64) ([]fileInfo, error) {

	return nil, nil
}
