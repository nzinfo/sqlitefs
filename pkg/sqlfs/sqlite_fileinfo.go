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

func (fi *fileInfo) Stat() (os.FileInfo, error) {
	return &fileInfo{
		name:    fi.name,
		mode:    fi.mode,
		size:    fi.size,
		modTime: fi.modTime,
	}, nil
}

func (c *content) Truncate() {
	c.bytes = make([]byte, 0)
}

func (c *content) Len() int {
	return len(c.bytes)
}
