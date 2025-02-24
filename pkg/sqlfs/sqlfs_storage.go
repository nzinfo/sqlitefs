package sqlfs

import (
	"fmt"
	"os"
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
	LoadEntry(entryID int64) *AsyncResult[*fileInfo]

	// LoadEntriesByParent loads all entries in a directory by parent_id asynchronously
	LoadEntriesByParent(parentID int64) *AsyncResult[[]fileInfo]
}

type storage struct {
	files    map[string]*file
	children map[string]map[string]*file

	/// 新增的与 SQLiteFS 相关的字段
	mu           sync.Mutex
	conn         *sqlite3.Conn
	entriesCache *lru.Cache // LRU cache for file/directory information, full_path -> entryId.
	dirsCache    *lru.Cache // LRU cache for directory information	entryId -> infoList.
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
		files:        make(map[string]*file, 0),
		children:     make(map[string]map[string]*file, 0),
		conn:         conn,
		entriesCache: lru.New(500), // Initialize entries cache
		dirsCache:    lru.New(100), // Initialize directories cache
	}, nil
}

/////////////////////////////////////////////

func clean(path string) string {
	return filepath.Clean(filepath.FromSlash(path))
}

// /////////////////////////////////////////
// dir represents a directory and its contents
type dir struct {
	info    fileInfo
	entries map[string]fileInfo // Set of file names in this directory
	// modTime time.Time           // Last modification time fileInfo 中有了。
}

// ////////////////////////////////////////
type content struct {
	name  string
	bytes []byte

	m sync.RWMutex
}

//////////////////////////////////////////

type file struct {
	name     string
	content  *content
	position int64
	flag     int
	mode     os.FileMode
	modTime  time.Time

	isClosed bool
	fs       *SQLiteFS
}
