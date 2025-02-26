package sqlfs

import (
	"io/fs"
	"os"
	"testing"
	"time"

	_ "github.com/ncruces/go-sqlite3/embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStorage(t *testing.T) *storage {
	s, err := newStorage(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	// Create test entries
	stmt, tail, err := s.conn.Prepare(`
		INSERT OR IGNORE INTO entries (entry_id, parent_id, name, mode_type, mode_perm, uid, gid, target, create_at, modify_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		t.Fatal(err)
	}
	if tail != "" {
		t.Fatal("unexpected tail in SQL: " + tail)
	}
	defer stmt.Close()

	now := time.Now().Unix()

	// Helper function to insert an entry
	insertEntry := func(entryID, parentID int64, name string, mode os.FileMode, target string) {
		if err := stmt.Reset(); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(1, entryID); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(2, parentID); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindText(3, name); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(4, int64(mode & ^fs.ModePerm)); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(5, int64(mode&fs.ModePerm)); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(6, 1000); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(7, 1000); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindText(8, target); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(9, now); err != nil {
			t.Fatal(err)
		}
		if err := stmt.BindInt64(10, now); err != nil {
			t.Fatal(err)
		}

		if err := stmt.Exec(); err != nil {
			t.Fatal(err)
		}
	}

	// Insert root directory
	insertEntry(1, 0, "/", os.ModeDir|0755, "")
	// Insert a directory
	insertEntry(2, 1, "testdir", os.ModeDir|0755, "")
	// Insert a file in root
	insertEntry(3, 1, "test.txt", 0644, "")
	// Insert a file in testdir
	insertEntry(4, 2, "inner.txt", 0644, "")
	// Insert a symlink
	insertEntry(5, 1, "link.txt", os.ModeSymlink|0777, "test.txt")

	return s
}

func TestLoadEntriesByParent(t *testing.T) {
	s := setupTestStorage(t)

	tests := []struct {
		name        string
		parentID    int64
		wantCount   int
		wantErr     bool
		checkResult func(*testing.T, []fileInfo)
	}{
		{
			name:      "root directory entries",
			parentID:  1,
			wantCount: 3,
			checkResult: func(t *testing.T, entries []fileInfo) {
				// Should contain testdir, test.txt, and link.txt
				assert.Len(t, entries, 3)
				var names []string
				for _, e := range entries {
					names = append(names, e.name)
				}
				assert.ElementsMatch(t, []string{"testdir", "test.txt", "link.txt"}, names)
			},
		},
		{
			name:      "subdirectory entries",
			parentID:  2,
			wantCount: 1,
			checkResult: func(t *testing.T, entries []fileInfo) {
				// Should contain only inner.txt
				assert.Len(t, entries, 1)
				assert.Equal(t, "inner.txt", entries[0].name)
			},
		},
		{
			name:      "empty directory",
			parentID:  999,
			wantCount: 0,
			checkResult: func(t *testing.T, entries []fileInfo) {
				assert.Empty(t, entries)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.LoadEntriesByParent(EntryID(tt.parentID), "")
			<-result.Done

			require.NoError(t, result.Err)
			if tt.checkResult != nil {
				tt.checkResult(t, result.Result)
			}
			assert.Equal(t, tt.wantCount, len(result.Result))
		})
	}
}
