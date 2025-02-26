package sqlfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"

	"github.com/go-git/go-billy/v6"
)

// 定义为 file，实现 billy.File 约定的接口
type file struct {
	fs       *SQLiteFS
	fileInfo *fileInfo
	flag     int
	mode     os.FileMode
	position int64
	content  *fileContent
}

func OpenFile(fs *SQLiteFS, fileInfo *fileInfo, flag int, perm os.FileMode) (*file, error) {
	// 需要加载 Chunks, 然后构造 Content
	result := fs.s.LoadFileChunks(fileInfo.entryID)
	chunks, err := result.Wait()
	if err != nil {
		return nil, err
	}

	fmt.Println(chunks)
	content := newFileContent(chunks)
	return &file{
		fs:       fs,
		fileInfo: fileInfo,
		flag:     flag,
		mode:     perm,
		content:  content,
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

	result := f.fs.s.FileRead(f.fileInfo.entryID, b, off)
	n, err := result.Wait()
	if err != nil {
		return 0, err
	}
	return n, nil
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

	// 等待异步写入完成
	result := f.fs.s.FileWrite(f.fileInfo.entryID, p, off)
	n, err := result.Wait()
	if err != nil {
		return 0, err
	}

	f.position = off + int64(n)
	return n, nil
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
	return f.content.Truncate(f.fs, f.fileInfo.entryID, size)
}

func (f *file) Duplicate(filename string, mode fs.FileMode, flag int) billy.File {
	// 暂时不处理 filename
	nf := &file{
		fs:       f.fs,
		fileInfo: f.fileInfo,
		flag:     flag,
		mode:     mode,
		position: f.position,
		content:  f.content,
	}

	if isTruncate(flag) {
		nf.content.Truncate(nf.fs, f.fileInfo.entryID, 0)
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
