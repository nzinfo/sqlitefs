//go:build proto

package sqlfs

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

func (s *storage) Has(path string) bool {
	path = clean(path)

	_, ok := s.files[path]
	return ok
}

func (s *storage) New(path string, mode fs.FileMode, flag int) (*file, error) {
	path = clean(path)
	if s.Has(path) {
		if !s.MustGet(path).mode.IsDir() {
			return nil, fmt.Errorf("file already exists %q", path)
		}

		return nil, nil
	}

	name := filepath.Base(path)

	f := &file{
		name:    name,
		content: &content{name: name},
		mode:    mode,
		flag:    flag,
		modTime: time.Now(),
	}

	s.files[path] = f
	err := s.createParent(path, mode, f)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (s *storage) createParent(path string, mode fs.FileMode, f *file) error {
	base := filepath.Dir(path)
	base = clean(base)
	if s.Has(base) {
		return nil
	}

	if base == string(separator) {
		return nil
	}

	return s.createParent(base, mode.Perm()|os.ModeDir, f)
}

func (s *storage) Children(path string) []*file {
	path = clean(path)

	children, ok := s.children[path]
	if !ok {
		return nil
	}

	var c []*file
	for _, f := range children {
		c = append(c, f)
	}

	return c
}

func (s *storage) MustGet(path string) *file {
	f, ok := s.Get(path)
	if !ok {
		panic(fmt.Sprintf("couldn't find %q", path))
	}

	return f
}

func (s *storage) Get(path string) (*file, bool) {
	path = clean(path)
	f, ok := s.files[path]
	return f, ok
}

func (s *storage) Rename(from, to string) error {
	from = clean(from)
	to = clean(to)

	if !s.Has(from) {
		return os.ErrNotExist
	}

	move := [][2]string{{from, to}}

	for pathFrom := range s.files {
		if pathFrom == from {
			continue
		}

		if strings.HasPrefix(pathFrom, from+string(separator)) {
			rel, _ := filepath.Rel(from, pathFrom)
			pathTo := filepath.Join(to, rel)
			move = append(move, [2]string{pathFrom, pathTo})
		}
	}

	for _, ops := range move {
		from := ops[0]
		to := ops[1]

		if err := s.move(from, to); err != nil {
			return err
		}
	}

	return nil
}

func (s *storage) move(from, to string) error {
	if s.Has(to) {
		return fmt.Errorf("file already exists %q", to)
	}

	f, ok := s.files[from]
	if !ok {
		return os.ErrNotExist
	}

	s.files[to] = f
	delete(s.files, from)
	return nil
}

func (s *storage) Remove(path string) error {
	path = clean(path)

	f, has := s.Get(path)
	if !has {
		return os.ErrNotExist
	}

	if f.mode.IsDir() && len(s.Children(path)) != 0 {
		return fmt.Errorf("directory not empty: %s", path)
	}

	delete(s.files, path)
	return nil
}

/////////////////////////////

func (c *content) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, &os.PathError{
			Op:   "writeat",
			Path: c.name,
			Err:  fmt.Errorf("negative offset"),
		}
	}

	c.m.Lock()
	defer c.m.Unlock()

	prev := len(c.bytes)

	diff := int(off) - prev
	if diff > 0 {
		c.bytes = append(c.bytes, make([]byte, diff)...)
	}

	c.bytes = append(c.bytes[:off], p...)
	return len(p), nil
}

func (c *content) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, &os.PathError{
			Op:   "readat",
			Path: c.name,
			Err:  fmt.Errorf("negative offset"),
		}
	}

	c.m.RLock()
	defer c.m.RUnlock()

	size := int64(len(c.bytes))
	if off >= size {
		return 0, io.EOF
	}

	l := int64(len(b))
	if off+l > size {
		l = size - off
	}

	n = copy(b, c.bytes[off:off+l])
	return
}

/////////////////////////////
/*
// LoadEntry implements StorageOps.LoadEntry
func (s *storage) LoadEntry(entryID int64) *AsyncResult[*fileInfo] {
	result := NewAsyncResult[*fileInfo]()

	// Load from database if not cached
	go func() {
		fi, err := s.loadEntrySync(entryID)
		if err != nil {
			result.Complete(nil, err)
			return
		}
		result.Complete(fi, nil)
	}()
	return result
}
*/

// LoadEntriesByParent implements StorageOps.LoadEntriesByParent
func (s *storage) LoadEntriesByParent(parentID int64, parentPath string) *AsyncResult[[]fileInfo] {
	result := NewAsyncResult[[]fileInfo]()
	if cached, ok := s.dirsCache.Get(parentID); ok {
		result.Complete(cached.([]fileInfo), nil)
		return result
	}

	// Load from database if not cached
	go func() {
		entries, err := s.loadEntriesByParentSync(parentID, parentPath)
		if err != nil {
			result.Complete(nil, err)
			return
		}
		s.dirsCache.Add(parentID, entries)
		result.Complete(entries, nil)
	}()
	return result
}

/*
// loadEntry loads a single entry by its ID
func (s *storage) loadEntrySync(entryID int64) (*fileInfo, error) {
	stmt, tail, err := s.conn.Prepare(`
		SELECT entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
		FROM entries
		WHERE entry_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return nil, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, entryID); err != nil {
		return nil, fmt.Errorf("bind entry_id error: %v", err)
	}

	if !stmt.Step() {
		if err := stmt.Err(); err != nil {
			return nil, fmt.Errorf("execute statement error: %v", err)
		}
		return nil, os.ErrNotExist
	}

	createTime := time.Unix(stmt.ColumnInt64(8), 0)
	modTime := time.Unix(stmt.ColumnInt64(9), 0)

	// Combine mode_type and mode_perm
	modeType := stmt.ColumnInt64(3)
	modePerm := stmt.ColumnInt64(4)
	mode := fs.FileMode(modeType | modePerm)

	fi := &fileInfo{
		entryID:  stmt.ColumnInt64(0),
		parentID: stmt.ColumnInt64(1),
		name:     stmt.ColumnText(2),
		fullPath: path.Join(parentPath, stmt.ColumnText(2)), // FIXME: 使用文件系統自身提供的 Join.
		mode:     mode,
		uid:      int(stmt.ColumnInt64(5)),
		gid:      int(stmt.ColumnInt64(6)),
		target:   stmt.ColumnText(7),
		createAt: createTime,
		modTime:  modTime,
	}

	return fi, nil
}
*/

// loadEntriesByParent loads all entries in a directory by parent_id
func (s *storage) loadEntriesByParentSync(parentID int64, parentPath string) ([]fileInfo, error) {
	stmt, tail, err := s.conn.Prepare(`
		SELECT entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
		FROM entries
		WHERE parent_id = ?
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return nil, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, parentID); err != nil {
		return nil, fmt.Errorf("bind parent_id error: %v", err)
	}

	var entries []fileInfo
	for stmt.Step() {
		if err := stmt.Err(); err != nil {
			return nil, fmt.Errorf("execute statement error: %v", err)
		}

		createTime := time.Unix(stmt.ColumnInt64(8), 0)
		modTime := time.Unix(stmt.ColumnInt64(9), 0)

		// Combine mode_type and mode_perm
		modeType := stmt.ColumnInt64(3)
		modePerm := stmt.ColumnInt64(4)
		mode := fs.FileMode(modeType | modePerm)

		fi := fileInfo{
			entryID:  stmt.ColumnInt64(0),
			parentID: stmt.ColumnInt64(1),
			name:     stmt.ColumnText(2),
			fullPath: path.Join(parentPath, stmt.ColumnText(2)),
			mode:     mode,
			uid:      int(stmt.ColumnInt64(5)),
			gid:      int(stmt.ColumnInt64(6)),
			target:   stmt.ColumnText(7),
			createAt: createTime,
			modTime:  modTime,
		}
		entries = append(entries, fi)
	}

	return entries, nil
}
