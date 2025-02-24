package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdGet(dbName string, mirrorPath string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs get <src> <dst>")
		os.Exit(1)
	}

	src := args[0]
	dst := args[1]

	fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	srcFile, err := fs.Open(src)
	if err != nil {
		fmt.Printf("Failed to open source file: %v\n", err)
		os.Exit(1)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		fmt.Printf("Failed to create destination file: %v\n", err)
		os.Exit(1)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		fmt.Printf("Failed to copy file: %v\n", err)
		os.Exit(1)
	}

	// If mirrorPath is set, check content consistency
	if mirrorPath != "" {
		mirrorFS := osfs.New(mirrorPath)

		// Read content from SQLiteFS
		srcFile.Seek(0, 0) // Reset file pointer to beginning
		sqlContent, err := io.ReadAll(srcFile)
		if err != nil {
			fmt.Printf("Failed to read file from SQLiteFS: %v\n", err)
			os.Exit(1)
		}

		// Read content from mirrorFS for comparison
		mirrorSrcFile, err := mirrorFS.Open(src)
		if err != nil {
			fmt.Printf("Failed to open mirror source file: %v\n", err)
			os.Exit(1)
		}
		defer mirrorSrcFile.Close()

		mirrorContent, err := io.ReadAll(mirrorSrcFile)
		if err != nil {
			fmt.Printf("Failed to read file from mirror: %v\n", err)
			os.Exit(1)
		}

		// Compare contents
		if !bytes.Equal(sqlContent, mirrorContent) {
			fmt.Printf("Warning: Content mismatch between SQLiteFS and mirror for file %s\n", src)
		}
	}

	fmt.Printf("Successfully copied %s to %s\n", src, dst)
}
