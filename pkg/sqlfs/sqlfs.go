package sqlfs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/helper/chroot"
	"github.com/go-git/go-billy/v6/util"
)

const separator = filepath.Separator

// Filesystem 实现了 billy.Filesystem 接口，并添加了 io.Closer 接口
type Filesystem interface {
	// billy.Filesystem all except Chroot
	billy.Basic
	billy.TempFile
	billy.Dir
	billy.Symlink
	// io.Closer 接口方法
	Close() error
}

// Memory a very convenient filesystem based on memory files.
type SQLiteFS struct {
	s         *storage
	mu        sync.Mutex
	openFiles map[EntryID]*file
}

// New returns a new Memory filesystem.
func NewSQLiteFS(dbName string) (Filesystem, billy.Filesystem, error) {
	s, err := newStorage(dbName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open database: %w", err)
	}
	fs := &SQLiteFS{
		s:         s,
		openFiles: make(map[EntryID]*file),
	}
	//return fs, nil
	return fs, chroot.New(fs, string(separator)), nil
}

func (fs *SQLiteFS) Create(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
}

func (fs *SQLiteFS) Open(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_RDONLY, 0)
}

func (fs *SQLiteFS) OpenFile(filename string, flag int, perm fs.FileMode) (billy.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	f, has := fs.s.Get(filename)
	fmt.Printf("OpenFile: %s, flag: %d, perm: %v, has: %v\n", filename, flag, perm, has)
	if !has {
		if !isCreate(flag) {
			return nil, os.ErrNotExist
		}

		var err error
		f, err = fs.s.New(filename, perm, flag)
		if err != nil {
			return nil, err
		}
	} else {
		if isExclusive(flag) {
			return nil, os.ErrExist
		}

		if target, isLink := fs.resolveLink(filename, f); isLink {
			if target != filename {
				return fs.OpenFile(target, flag, perm)
			}
		}
	}

	if f.mode.IsDir() {
		return nil, fmt.Errorf("cannot open directory: %s", filename)
	}

	// 仅有 fileInfo 不足以打开文件，需要加载文件的 chunks, 这个过程应该是异步的。
	file, err := OpenFile(fs, f, flag, perm)
	if err != nil {
		return nil, err
	}
	fs.openFiles[f.entryID] = file
	return file, nil
}

func (fs *SQLiteFS) closeFile(fi *fileInfo) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.openFiles, fi.entryID)
}

func (fs *SQLiteFS) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// 关闭所有打开的文件
	for entryID, f := range fs.openFiles {
		if err := f.Close(); err != nil {
			return fmt.Errorf("failed to close file: %v", err)
		}
		delete(fs.openFiles, entryID)
	}

	// 等待刷新完成
	result := fs.s.Flush()
	if _, err := result.Wait(); err != nil {
		return fmt.Errorf("failed to flush storage: %v", err)
	}
	// 关闭数据库连接
	if err := fs.s.Close(); err != nil {
		return fmt.Errorf("failed to close storage: %v", err)
	}

	return nil
}

func (fs *SQLiteFS) resolveLink(fullpath string, f *fileInfo) (target string, isLink bool) {
	// 移植完成
	if !isSymlink(f.mode) {
		return fullpath, false
	}

	target = f.target
	if !isAbs(target) {
		target = fs.Join(filepath.Dir(fullpath), target)
	}

	return target, true
}

// On Windows OS, IsAbs validates if a path is valid based on if stars with a
// unit (eg.: `C:\`)  to assert that is absolute, but in this mem implementation
// any path starting by `separator` is also considered absolute.
func isAbs(path string) bool {
	return filepath.IsAbs(path) || strings.HasPrefix(path, string(separator))
}

func (fs *SQLiteFS) Stat(filename string) (os.FileInfo, error) {
	f, has := fs.s.Get(filename)
	if !has {
		return nil, os.ErrNotExist
	}

	fi, _ := f.Stat()

	var err error
	if target, isLink := fs.resolveLink(filename, f); isLink {
		fi, err = fs.Stat(target)
		if err != nil {
			return nil, err
		}
	}

	// the name of the file should always the name of the stated file, so we
	// overwrite the Stat returned from the storage with it, since the
	// filename may belong to a link.
	fi.(*fileInfo).name = filepath.Base(filename)
	return fi, nil
}

func (fs *SQLiteFS) Lstat(filename string) (os.FileInfo, error) {
	f, has := fs.s.Get(filename)
	if !has {
		return nil, os.ErrNotExist
	}

	return f.Stat()
}

type ByName []os.FileInfo

func (a ByName) Len() int           { return len(a) }
func (a ByName) Less(i, j int) bool { return a[i].Name() < a[j].Name() }
func (a ByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func (fs *SQLiteFS) ReadDir(path string) ([]os.FileInfo, error) {
	if f, has := fs.s.Get(path); has {
		if target, isLink := fs.resolveLink(path, f); isLink {
			if target != path {
				return fs.ReadDir(target)
			}
		}
	} else {
		return nil, &os.PathError{Op: "open", Path: path, Err: syscall.ENOENT}
	}

	var entries []os.FileInfo
	subItems, err := fs.s.Children(path)
	if err != nil {
		return nil, err
	}

	for _, f := range *subItems {
		fi, _ := f.Stat()
		entries = append(entries, fi)
	}

	sort.Sort(ByName(entries))

	return entries, nil
}

func (fs *SQLiteFS) MkdirAll(path string, perm fs.FileMode) error {
	_, err := fs.s.New(path, perm|os.ModeDir, 0)
	return err
}

func (fs *SQLiteFS) TempFile(dir, prefix string) (billy.File, error) {
	return util.TempFile(fs, dir, prefix)
}

func (fs *SQLiteFS) Rename(from, to string) error {
	return fs.s.Rename(from, to)
}

func (fs *SQLiteFS) Remove(filename string) error {
	return fs.s.Remove(filename)
}

// Falls back to Go's filepath.Join, which works differently depending on the
// OS where the code is being executed.
func (fs *SQLiteFS) Join(elem ...string) string {
	return filepath.Join(elem...)
}

func (fs *SQLiteFS) Symlink(target, link string) error {
	_, err := fs.Lstat(link)
	if err == nil {
		return os.ErrExist
	}

	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// FIXME: re-implement save target to fileInfo's target.
	return util.WriteFile(fs, link, []byte(target), 0777|os.ModeSymlink)
}

func (fs *SQLiteFS) Readlink(link string) (string, error) {
	f, has := fs.s.Get(link)
	if !has {
		return "", os.ErrNotExist
	}

	if !isSymlink(f.mode) {
		return "", &os.PathError{
			Op:   "readlink",
			Path: link,
			Err:  fmt.Errorf("not a symlink"),
		}
	}

	return f.target, nil
}

// Capabilities implements the Capable interface.
func (fs *SQLiteFS) Capabilities() billy.Capability {
	return billy.WriteCapability |
		billy.ReadCapability |
		billy.ReadAndWriteCapability |
		billy.SeekCapability |
		billy.TruncateCapability
}

//////////////////////////////////////////////////

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

//////////////////////////////////////////////

type FileType string

const (
	FileTypeFile    FileType = "file"
	FileTypeDir     FileType = "dir"
	FileTypeSymlink FileType = "symlink"
)

type fileInfo struct {
	entryID  EntryID     // Primary key from entries table
	parentID EntryID     // Parent directory ID (0 for root)
	name     string      // File/directory name
	fullPath string      // Full path of the file/directory
	mode     os.FileMode // File mode/permissions
	uid      int         // User ID
	gid      int         // Group ID
	target   string      // Symlink target or directory children
	size     int64       // File size
	createAt time.Time   // Creation time
	modTime  time.Time   // Last modification time
}
