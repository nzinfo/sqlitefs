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
	mu       sync.Mutex
	conn     *sqlite3.Conn
	dirCache *lru.Cache // LRU cache for directory information
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
		files:    make(map[string]*file, 0),
		children: make(map[string]map[string]*file, 0),
		conn:     conn,
	}, nil
}

// getDir returns the dir object for a path, either from cache or by loading it
func (s *storage) getDir(path string) (*dir, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clean and normalize the path
	path = clean(path)

	// Check if the exact path is in cache
	if cached, ok := s.dirCache.Get(path); ok {
		return cached.(*dir), nil
	}

	// Stack to store paths that need to be loaded
	type pathInfo struct {
		path    string
		entryID int64
	}
	var toLoad []pathInfo

	// Start from the requested path and work up until we find a cached directory
	current := path
	var cachedDir *dir
	for {
		if current == "/" {
			// Root directory is special case
			if cached, ok := s.dirCache.Get("/"); ok {
				cachedDir = cached.(*dir)
				break
			}

			// Load root directory from database
			rootEntriesResult := s.LoadEntriesByParent(0)
			rootEntries, err := rootEntriesResult.Wait()
			if err != nil {
				return nil, fmt.Errorf("failed to load root directory: %w", err)
			}

			// assert rootEntries must have only one entry
			if len(rootEntries) != 1 {
				return nil, fmt.Errorf("root directory must have only one entry")
			}

			rootID := rootEntries[0].entryID
			// Load root's children
			childrenResult := s.LoadEntriesByParent(rootID)
			children, err := childrenResult.Wait()
			if err != nil {
				return nil, fmt.Errorf("failed to load root directory children: %w", err)
			}

			entriesMap := make(map[string]fileInfo)
			for _, entry := range children {
				entriesMap[entry.name] = entry
			}

			cachedDir = &dir{
				info:    rootEntries[0],
				entries: entriesMap,
			}

			// Add root directory to cache
			s.dirCache.Add("/", cachedDir)
			break
		}

		if cached, ok := s.dirCache.Get(current); ok {
			cachedDir = cached.(*dir)
			break
		}

		// Add current path to the stack and move up
		toLoad = append(toLoad, pathInfo{current, 0})
		current = filepath.Dir(current)
	}

	// Now load directories from the closest cached ancestor down
	var parentID int64
	if cachedDir != nil {
		parentID = cachedDir.info.entryID
	}

	// Process the stack from bottom (closest to cached) to top (requested path)
	for i := len(toLoad) - 1; i >= 0; i-- {
		info := &toLoad[i]

		// Load directory entries using parent_id
		entriesResult := s.LoadEntriesByParent(parentID)
		entries, err := entriesResult.Wait()
		if err != nil {
			return nil, fmt.Errorf("failed to load directory entries for %s: %w", info.path, err)
		}

		// Create new dir object
		d := &dir{
			entries: make(map[string]fileInfo),
		}

		// Add entries to the directory
		for _, entry := range entries {
			d.entries[entry.name] = entry
			if entry.fileType == FileTypeDir {
				parentID = entry.entryID // Update parent ID for next iteration
			}
		}

		// Add to cache
		s.dirCache.Add(info.path, d)

		// If this is the requested path, return it
		if info.path == path {
			return d, nil
		}
	}

	return nil, fmt.Errorf("failed to load directory %s", path)
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
