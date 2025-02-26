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

	// 在读取之前，确保处理所有待更新的 chunk
	//fmt.Printf("file.ReadAt: 文件 %s (ID=%d) 在读取前处理所有待更新的 chunk\n",
	//	f.Name(), f.fileInfo.entryID)
	//f.fs.processAllPendingUpdates()

	fmt.Printf("file.ReadAt: 文件 %s (ID=%d) 开始读取，偏移量=%d，长度=%d\n",
		f.Name(), f.fileInfo.entryID, off, len(b))
	result := f.fs.s.FileRead(f.fileInfo.entryID, b, off)
	n, err := result.Wait()
	if err != nil {
		fmt.Printf("file.ReadAt: 文件 %s (ID=%d) 读取失败: %v\n",
			f.Name(), f.fileInfo.entryID, err)
		return 0, err
	}
	fmt.Printf("file.ReadAt: 文件 %s (ID=%d) 读取成功，实际读取长度=%d\n",
		f.Name(), f.fileInfo.entryID, n)
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

	fmt.Printf("file.WriteAt: 文件 %s (ID=%d) 开始写入，偏移量=%d，长度=%d\n",
		f.Name(), f.fileInfo.entryID, off, len(p))

	// 等待异步写入完成
	n, err := f.content.Write(f.fs, f.fileInfo.entryID, p, off)
	if err != nil {
		fmt.Printf("file.WriteAt: 文件 %s (ID=%d) 写入失败: %v\n",
			f.Name(), f.fileInfo.entryID, err)
		return 0, err
	}

	// 写入完成后，处理所有待更新的 chunk
	//fmt.Printf("file.WriteAt: 文件 %s (ID=%d) 写入成功，长度=%d，处理所有待更新的 chunk\n",
	//	f.Name(), f.fileInfo.entryID, n)
	//f.fs.processAllPendingUpdates()

	f.position = off + int64(n)
	return n, nil
}

func (f *file) Close() error {
	if f.fileInfo == nil {
		return os.ErrClosed
	}

	// 在关闭文件前，确保处理所有待更新的 chunk
	// f.fs.processAllPendingUpdates()

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

// updateChunks 更新文件内容中的 chunk 信息
func (f *file) updateChunks(updates map[int64]ChunkUpdateInfo) {
	fmt.Printf("file.updateChunks: 文件 %s (ID=%d) 开始更新 %d 个 chunks\n",
		f.Name(), f.fileInfo.entryID, len(updates))

	if f.content == nil {
		fmt.Printf("file.updateChunks: 文件 %s (ID=%d) 的 content 为 nil，跳过更新\n",
			f.Name(), f.fileInfo.entryID)
		return
	}

	changed := false

	// 遍历所有 chunks，查找匹配的 reqID 并更新
	for i := range f.content.chunks {
		reqID := int64(i)
		if update, exists := updates[reqID]; exists {
			fmt.Printf("file.updateChunks: 更新 chunk[%d]: blockID 从 %d 更新到 %d, blockOffset 从 %d 更新到 %d\n",
				i, f.content.chunks[i].blockID, update.BlockID,
				f.content.chunks[i].blockOffset, update.BlockOffset)

			f.content.chunks[i].blockID = update.BlockID
			f.content.chunks[i].blockOffset = update.BlockOffset
			changed = true
		}
	}

	// 只有在确实有更新时才重建段树
	if changed {
		fmt.Printf("file.updateChunks: 文件 %s (ID=%d) 有更新，重建段树\n",
			f.Name(), f.fileInfo.entryID)
		// f.content.buildSegmentTree()
	} else {
		fmt.Printf("file.updateChunks: 文件 %s (ID=%d) 没有实际更新\n",
			f.Name(), f.fileInfo.entryID)
	}
}
