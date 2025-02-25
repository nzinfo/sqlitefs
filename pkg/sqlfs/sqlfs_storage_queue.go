//go:build !proto

package sqlfs

import "io/fs"

func (s *storage) LoadEntriesByParent(parentID EntryID, parentPath string) *AsyncResult[[]fileInfo] {
	result := NewAsyncResult[[]fileInfo]()
	return result
}

func (s *storage) LoadFileChunks(fileID EntryID) *AsyncResult[[]fileChunk] {
	result := NewAsyncResult[[]fileChunk]()
	return result
}

func (s *storage) FileTruncate(fileID EntryID, size int64) *AsyncResult[error] {
	return nil
}

func (s *storage) FileWrite(fileID EntryID, p []byte, offset int64) *AsyncResult[int] {
	result := NewAsyncResult[int]()
	return result
}

func (s *storage) New(path string, mode fs.FileMode, flag int) (*fileInfo, error) {
	return nil, nil
}

func (s *storage) Get(path string) (*fileInfo, bool) {
	return nil, false
}

func (s *storage) Children(path string) (*[]fileInfo, error) {
	return nil, nil
}

func (s *storage) Rename(from, to string) error {
	return nil
}

func (s *storage) Remove(path string) error {
	return nil
}
