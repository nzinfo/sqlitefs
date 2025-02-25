//go:build !proto

package sqlfs

import "io/fs"

func (s *storage) LoadEntry(entryID int64) *AsyncResult[*fileInfo] {
	result := NewAsyncResult[*fileInfo]()
	return result
}

func (s *storage) LoadEntriesByParent(parentID int64, parentPath string) *AsyncResult[[]fileInfo] {
	result := NewAsyncResult[[]fileInfo]()
	return result
}

func (s *storage) New(path string, mode fs.FileMode, flag int) (*file, error) {
	return nil, nil
}

func (s *storage) Get(path string) (*file, bool) {
	return nil, false
}

func (s *storage) Children(path string) []*file {
	return nil
}

func (s *storage) Rename(from, to string) error {
	return nil
}

func (s *storage) Remove(path string) error {
	return nil
}
