package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdMkdir(dbName string, mirrorPath string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs mkdir <directory>")
		os.Exit(1)
	}

	dir := args[0]

	sqlfs, fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}
	defer sqlfs.Close()

	if err := fs.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("Failed to create directory: %v\n", err)
		os.Exit(1)
	}

	if mirrorPath != "" {
		if err := os.MkdirAll(filepath.Join(mirrorPath, dir), 0755); err != nil {
			fmt.Printf("Failed to create mirror directory: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully created directory %s\n", dir)
}
