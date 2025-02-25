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

	entry, err := s.getEntry(path)
	if err != nil {
		// TODO: log err ?
		return false
	}
	return entry != nil
}

func (s *storage) New(path string, mode fs.FileMode, flag int) (*file, error) {
	path = clean(path)

	// Check if file already exists
	entry, err := s.getEntry(path)
	if err == nil {
		if !entry.mode.IsDir() {
			return nil, fmt.Errorf("file already exists %q", path)
		}
		return nil, nil
	}

	// Get the parent path components that need to be created
	var dirsToCreate []string
	current := filepath.Dir(path)
	for {
		current = clean(current)
		if current == string(separator) {
			break
		}
		
		if _, err := s.getEntry(current); err != nil {
			dirsToCreate = append([]string{current}, dirsToCreate...)
		}
		current = filepath.Dir(current)
	}

	// Begin transaction
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("begin transaction error: %v", err)
	}
	defer tx.Rollback()

	// Get parent directory ID
	parentPath := filepath.Dir(path)
	var parentID int64
	if parentPath == string(separator) {
		parentID = 1 // Root directory ID
	} else {
		parentEntry, err := s.getEntry(parentPath)
		if err != nil {
			// Create parent directories in batch
			stmt, tail, err := tx.Prepare(`
				INSERT INTO entries (
					parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
				) VALUES (?, ?, ?, ?, ?, ?, '[]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
				RETURNING entry_id
			`)
			if err != nil {
				return nil, fmt.Errorf("prepare directory creation statement error: %v", err)
			}
			if tail != "" {
				stmt.Close()
				return nil, fmt.Errorf("unexpected tail in SQL: %s", tail)
			}
			defer stmt.Close()

			// Create each missing parent directory
			currentParentID := int64(1) // Start from root
			for _, dirPath := range dirsToCreate {
				dirName := filepath.Base(dirPath)
				
				if err := stmt.BindInt64(1, currentParentID); err != nil {
					return nil, fmt.Errorf("bind parent_id error: %v", err)
				}
				if err := stmt.BindText(2, dirName); err != nil {
					return nil, fmt.Errorf("bind name error: %v", err)
				}
				if err := stmt.BindInt64(3, int64(os.ModeDir)); err != nil {
					return nil, fmt.Errorf("bind mode_type error: %v", err)
				}
				if err := stmt.BindInt64(4, int64(0755)); err != nil {
					return nil, fmt.Errorf("bind mode_perm error: %v", err)
				}
				if err := stmt.BindInt64(5, 0); err != nil {
					return nil, fmt.Errorf("bind uid error: %v", err)
				}
				if err := stmt.BindInt64(6, 0); err != nil {
					return nil, fmt.Errorf("bind gid error: %v", err)
				}

				if !stmt.Step() {
					return nil, fmt.Errorf("execute directory creation error: %v", stmt.Err())
				}
				
				currentParentID = stmt.ColumnInt64(0)
				stmt.Reset()
			}
			parentID = currentParentID
		} else {
			parentID = parentEntry.entryID
		}
	}

	// Create the new file
	stmt, tail, err := tx.Prepare(`
		INSERT INTO entries (
			parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
		) VALUES (?, ?, ?, ?, ?, ?, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		RETURNING entry_id
	`)
	if err != nil {
		return nil, fmt.Errorf("prepare file creation statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return nil, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	name := filepath.Base(path)
	if err := stmt.BindInt64(1, parentID); err != nil {
		return nil, fmt.Errorf("bind parent_id error: %v", err)
	}
	if err := stmt.BindText(2, name); err != nil {
		return nil, fmt.Errorf("bind name error: %v", err)
	}
	if err := stmt.BindInt64(3, int64(mode&os.ModeType)); err != nil {
		return nil, fmt.Errorf("bind mode_type error: %v", err)
	}
	if err := stmt.BindInt64(4, int64(mode.Perm())); err != nil {
		return nil, fmt.Errorf("bind mode_perm error: %v", err)
	}
	if err := stmt.BindInt64(5, 0); err != nil {
		return nil, fmt.Errorf("bind uid error: %v", err)
	}
	if err := stmt.BindInt64(6, 0); err != nil {
		return nil, fmt.Errorf("bind gid error: %v", err)
	}

	if !stmt.Step() {
		return nil, fmt.Errorf("execute file creation error: %v", stmt.Err())
	}

	entryID := stmt.ColumnInt64(0)

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit transaction error: %v", err)
	}

	// Create and return the file object
	f := &file{
		name:    name,
		content: &content{name: name},
		mode:    mode,
		flag:    flag,
		modTime: time.Now(),
		entryID: entryID,
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

func (s *storage) Children(path string) *[]fileInfo {
	path = clean(path)

	entry, err := s.getEntry(path)

	if !entry.mode.IsDir() {
		return nil
	}

	entriesResult := s.LoadEntriesByParent(entry.entryID, path)
	entries, err := entriesResult.Wait()
	if err != nil {
		// FIXME: log the error.
		return nil
	}
	return &entries
}

func (s *storage) MustGet(path string) *fileInfo {
	f, ok := s.Get(path)
	if !ok {
		panic(fmt.Sprintf("couldn't find %q", path))
	}

	return f
}

func (s *storage) Get(path string) (*fileInfo, bool) {
	path = clean(path)
	entry, err := s.getEntry(path)
	if err != nil {
		return nil, false
	}
	return entry, true
}

func (s *storage) Rename(from, to string) error {
	// Rename 与 Remove 均涉及 Cache 的无效
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
