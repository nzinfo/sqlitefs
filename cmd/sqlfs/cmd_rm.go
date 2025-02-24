package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdRm(dbName string, mirrorPath string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs rm <file>")
		os.Exit(1)
	}

	file := args[0]

	fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	if err := fs.Remove(file); err != nil {
		fmt.Printf("Failed to remove file: %v\n", err)
		os.Exit(1)
	}

	if mirrorPath != "" {
		if err := os.Remove(filepath.Join(mirrorPath, file)); err != nil {
			fmt.Printf("Failed to remove mirror file: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully removed file %s\n", file)
}
