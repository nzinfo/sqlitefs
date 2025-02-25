package sqlfs

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/ncruces/go-sqlite3"
)

// AsyncResult represents an asynchronous operation result
type AsyncResult[T any] struct {
	Result T
	Err    error
	Done   chan struct{}
}

// NewAsyncResult creates a new AsyncResult
func NewAsyncResult[T any]() *AsyncResult[T] {
	return &AsyncResult[T]{
		Done: make(chan struct{}),
	}
}

// Wait waits for the async operation to complete and returns the result
func (ar *AsyncResult[T]) Wait() (T, error) {
	<-ar.Done
	return ar.Result, ar.Err
}

func (ar *AsyncResult[T]) Complete(result T, err error) {
	ar.Result = result
	ar.Err = err
	close(ar.Done)
}

// StorageOps defines the interface for storage operations
type StorageOps interface {
	// LoadEntry loads a single entry by its ID asynchronously
	// LoadEntry(entryID int64) *AsyncResult[*fileInfo]

	// LoadEntriesByParent loads all entries in a directory by parent_id asynchronously
	LoadEntriesByParent(parentID EntryID, parentPath string) *AsyncResult[[]fileInfo]
	LoadFileChunks(fileID EntryID) *AsyncResult[[]fileChunk]

	FileTruncate(fileID EntryID, size int64) *AsyncResult[error]
}

type storage struct {
	//files    map[string]*file
	//children map[string]map[string]*file

	/// 新增的与 SQLiteFS 相关的字段
	mu           sync.Mutex
	conn         *sqlite3.Conn
	entriesCache *lru.Cache // LRU cache for file/directory information, full_path -> entryId.
	dirsCache    *lru.Cache // LRU cache for directory information	entryId -> infoList.
	rootEntry    *fileInfo
	maxEntryID   EntryID
}

func newStorage(dbName string) (*storage, error) {
	conn, err := sqlite3.Open(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := InitDatabase(conn); err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	return &storage{
		// files:        make(map[string]*file, 0),
		// children:     make(map[string]map[string]*file, 0),
		conn:         conn,
		entriesCache: lru.New(500), // Initialize entries cache
		dirsCache:    lru.New(100), // Initialize directories cache
		rootEntry:    loadRootEntry(conn),
		maxEntryID:   loadMaxEntryID(conn),
	}, nil
}

func loadRootEntry(conn *sqlite3.Conn) *fileInfo {

	entryID := int64(1)

	stmt, _, err := conn.Prepare(`
		SELECT entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at
		FROM entries
		WHERE entry_id = ?
	`)
	if err != nil {
		return nil
	}
	defer stmt.Close()

	if err := stmt.BindInt64(1, entryID); err != nil {
		return nil
	}

	if !stmt.Step() {
		if err := stmt.Err(); err != nil {
			return nil
		}
		return nil
	}

	createTime := time.Unix(stmt.ColumnInt64(8), 0)
	modTime := time.Unix(stmt.ColumnInt64(9), 0)

	// Combine mode_type and mode_perm
	modeType := stmt.ColumnInt64(3)
	modePerm := stmt.ColumnInt64(4)
	mode := fs.FileMode(modeType | modePerm)

	fi := &fileInfo{
		entryID:  EntryID(stmt.ColumnInt64(0)),
		parentID: EntryID(stmt.ColumnInt64(1)),
		name:     stmt.ColumnText(2),
		fullPath: "/", // 因为是根目录, 所以 fullPath 固定是 /
		mode:     mode,
		uid:      int(stmt.ColumnInt64(5)),
		gid:      int(stmt.ColumnInt64(6)),
		target:   stmt.ColumnText(7),
		createAt: createTime,
		modTime:  modTime,
	}

	return fi
}

func loadMaxEntryID(conn *sqlite3.Conn) EntryID {
	stmt, tail, err := conn.Prepare(`
		SELECT MAX(entry_id) FROM entries
	`)
	if err != nil {
		return 0 // , fmt.Errorf("prepare statement error: %v", err)
	}
	if tail != "" {
		stmt.Close()
		return 0 //, fmt.Errorf("unexpected tail in SQL: %s", tail)
	}
	defer stmt.Close()

	if !stmt.Step() {
		return 0 //, fmt.Errorf("no results found: %v", stmt.Err())
	}

	// Get the max entry_id
	maxEntryID := EntryID(stmt.ColumnInt64(0))
	return maxEntryID //, nil
}

/////////////////////////////////////////////

// getEntry retrieves the fileInfo for the given full_path, recursively searching through directories.
func (s *storage) getEntry(full_path string) (*fileInfo, error) {
	// Clean and normalize the path
	full_path = clean(full_path)

	// Check cache first
	if cached, ok := s.entriesCache.Get(full_path); ok {
		return cached.(*fileInfo), nil
	}

	// Handle root directory specially
	if full_path == "/" {
		return s.rootEntry, nil
	}

	// Split into directory and file name
	dirPath := filepath.Dir(full_path)
	fileName := filepath.Base(full_path)

	// fmt.Println("getEntry:", full_path, dirPath, fileName)

	var parentID EntryID
	if dirPath == "/" {
		parentID = 1 // root directory
	} else {
		parentDirInfo, err := s.getEntry(dirPath)
		if err != nil {
			return nil, err
		}
		parentID = parentDirInfo.entryID
	}

	// Load entries in the parent directory
	entriesResult := s.LoadEntriesByParent(parentID, dirPath)
	entries, err := entriesResult.Wait()
	if err != nil {
		return nil, err
	}

	// fmt.Println("getEntry「DONE」:", full_path, dirPath, fileName, parentID, entries)
	// Check for the file in the loaded entries
	for _, entry := range entries {
		if entry.name == fileName {
			s.entriesCache.Add(full_path, &entry) // Add to cache
			return &entry, nil
		}
	}

	// If we reach here, the file was not found
	return nil, &ErrFileNotFound{Path: full_path}
}

/////////////////////////////////////////////////////////

func clean(path string) string {
	return filepath.Clean(filepath.FromSlash(path))
}

// ////////////////////////////////////////
type content struct {
	name  string
	bytes []byte

	m sync.RWMutex
}

// ////////////////////////////////////////
// ErrFileNotFound represents an error when a file is not found in the storage.
type ErrFileNotFound struct {
	Path string
}

func (e *ErrFileNotFound) Error() string {
	return fmt.Sprintf("file not found: %s", e.Path)
}
