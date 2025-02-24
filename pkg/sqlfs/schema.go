package sqlfs

import (
	"fmt"

	"github.com/ncruces/go-sqlite3"
	_ "github.com/ncruces/go-sqlite3/embed"
)

// InitDatabase creates all necessary tables for SQLiteFS
func InitDatabase(conn *sqlite3.Conn) error {
	tables := []string{
		// Entries table for all filesystem entries (files, directories, symlinks)
		`CREATE TABLE IF NOT EXISTS entries (
			entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
			parent_id INTEGER,  -- NULL for root directory
			name TEXT NOT NULL,
			type TEXT NOT NULL CHECK(type IN ('file', 'dir', 'symlink')),
			mode INTEGER NOT NULL,
			uid INTEGER NOT NULL,
			gid INTEGER NOT NULL,
			target TEXT,  -- For symlinks: target path; For dirs: JSON array of child entry_ids
			create_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			modify_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			UNIQUE(parent_id, name),
			FOREIGN KEY(parent_id) REFERENCES entries(entry_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_entries_parent ON entries(parent_id)`,

		// File tags for metadata and search
		`CREATE TABLE IF NOT EXISTS file_tags (
			entry_id INTEGER NOT NULL,
			tag TEXT NOT NULL,
			create_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (entry_id, tag),
			FOREIGN KEY(entry_id) REFERENCES entries(entry_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_file_tags ON file_tags(tag)`,

		// Blocks table stores actual file content
		`CREATE TABLE IF NOT EXISTS blocks (
			block_id INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB NOT NULL
		)`,

		// File chunks maps file content to blocks
		`CREATE TABLE IF NOT EXISTS file_chunks (
			entry_id INTEGER NOT NULL,
			offset INTEGER NOT NULL,  -- offset in file
			size INTEGER NOT NULL,    -- chunk size
			block_id INTEGER NOT NULL,
			block_offset INTEGER NOT NULL,  -- offset in block
			crc32 INTEGER NOT NULL,   -- checksum for data integrity
			create_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (entry_id, offset),
			FOREIGN KEY(entry_id) REFERENCES entries(entry_id),
			FOREIGN KEY(block_id) REFERENCES blocks(block_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_chunks_block ON file_chunks(block_id)`,

		// Create root directory if it doesn't exist
		`INSERT OR IGNORE INTO entries (
			entry_id, name, parent_id, type, mode, uid, gid, target, create_at, modify_at
		) VALUES (
			1, '/', NULL, 'dir', 0755, 0, 0, '[]', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
		)`,
	}

	// Execute all table creation statements
	for _, table := range tables {
		stmt, tail, err := conn.Prepare(table)
		if err != nil {
			return fmt.Errorf("prepare table creation error: %v", err)
		}
		if tail != "" {
			stmt.Close()
			return fmt.Errorf("unexpected tail in table creation SQL: %s", tail)
		}
		defer stmt.Close()

		if !stmt.Step() {
			if err := stmt.Err(); err != nil {
				return fmt.Errorf("execute table creation error: %v", err)
			}
		}
	}

	return nil
}

// Constants for block size alignment
const (
	MinBlockSize   = 4 * 1024        // 4KB minimum block size
	MaxBlockSize   = 2 * 1024 * 1024 // 2MB maximum block size
	BlockAlignment = 4 * 1024        // 4KB alignment requirement
)
