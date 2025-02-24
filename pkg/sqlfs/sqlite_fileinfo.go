package sqlfs

import (
	"io/fs"
	"os"
	"time"
)

func (fi *fileInfo) Name() string {
	return fi.name
}

func (fi *fileInfo) Size() int64 {
	return int64(fi.size)
}

func (fi *fileInfo) Mode() fs.FileMode {
	return fi.mode
}

func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi *fileInfo) IsDir() bool {
	return fi.mode.IsDir()
}

func (*fileInfo) Sys() interface{} {
	return nil
}

func (c *content) Truncate() {
	c.bytes = make([]byte, 0)
}

func (c *content) Len() int {
	return len(c.bytes)
}

func isCreate(flag int) bool {
	return flag&os.O_CREATE != 0
}

func isExclusive(flag int) bool {
	return flag&os.O_EXCL != 0
}

func isAppend(flag int) bool {
	return flag&os.O_APPEND != 0
}

func isTruncate(flag int) bool {
	return flag&os.O_TRUNC != 0
}

func isReadAndWrite(flag int) bool {
	return flag&os.O_RDWR != 0
}

func isReadOnly(flag int) bool {
	return flag == os.O_RDONLY
}

func isWriteOnly(flag int) bool {
	return flag&os.O_WRONLY != 0
}

func isSymlink(m fs.FileMode) bool {
	return m&os.ModeSymlink != 0
}
