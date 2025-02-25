package sqlfs

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/go-git/go-billy/v6"
)

// 定义为 file，实现 billy.File 约定的接口
type file struct {
	fs       *SQLiteFS
	fileInfo *fileInfo
	flag     int
	mode     os.FileMode
	position int64
}

func OpenFile(fs *SQLiteFS, fileInfo *fileInfo, flag int, perm os.FileMode) (*file, error) {
	return &file{
		fs:       fs,
		fileInfo: fileInfo,
		flag:     flag,
		mode:     perm,
	}, nil
}

func (f *file) Name() string {
	return f.fileInfo.Name()
}

func (f *file) Read(b []byte) (int, error) {
	n, err := f.ReadAt(b, f.position)
	f.position += int64(n)

	if errors.Is(err, io.EOF) && n != 0 {
		err = nil
	}

	return n, err
}

func (f *file) ReadAt(b []byte, off int64) (int, error) {
	if f.fileInfo == nil {
		return 0, os.ErrClosed
	}

	if !isReadAndWrite(f.flag) && !isReadOnly(f.flag) {
		return 0, errors.New("read not supported")
	}

	return 0, nil
	//n, err := f.content.ReadAt(b, off)

	//return n, err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	if f.fileInfo == nil {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekCurrent:
		f.position += offset
	case io.SeekStart:
		f.position = offset
	case io.SeekEnd:
		f.position = int64(f.content.Len()) + offset
	}

	return f.position, nil
}

func (f *file) Write(p []byte) (int, error) {
	return f.WriteAt(p, f.position)
}

func (f *file) WriteAt(p []byte, off int64) (int, error) {
	if f.fileInfo == nil {
		return 0, os.ErrClosed
	}

	if !isReadAndWrite(f.flag) && !isWriteOnly(f.flag) {
		return 0, errors.New("write not supported")
	}

	f.modTime = time.Now()
	return 0, nil
	// n, err := f.content.WriteAt(p, off)
	// f.position = off + int64(n)

	// return n, err
}

func (f *file) Close() error {
	if f.fileInfo == nil {
		return os.ErrClosed
	}

	f.fs.closeFile(f.fileInfo) // 通知文件系统文件关闭
	f.fileInfo = nil
	return nil
}

func (f *file) Truncate(size int64) error {
	if size < int64(len(f.content.bytes)) {
		f.content.bytes = f.content.bytes[:size]
	} else if more := int(size) - len(f.content.bytes); more > 0 {
		f.content.bytes = append(f.content.bytes, make([]byte, more)...)
	}

	return nil
}

func (f *file) Duplicate(filename string, mode fs.FileMode, flag int) billy.File {
	nf := &file{
		name:    filename,
		content: f.content,
		mode:    mode,
		flag:    flag,
		modTime: f.modTime,
		fs:      f.fs,
	}

	if isTruncate(flag) {
		nf.content.Truncate()
	}

	if isAppend(flag) {
		nf.position = int64(nf.content.Len())
	}

	return nf
}

func (f *file) Stat() (os.FileInfo, error) {
	// 此处是否需要检查 fileInfo ?
	return f.fileInfo.Stat()
}

// Lock is a no-op in memfs.
func (f *file) Lock() error {
	return nil
}

// Unlock is a no-op in memfs.
func (f *file) Unlock() error {
	return nil
}
