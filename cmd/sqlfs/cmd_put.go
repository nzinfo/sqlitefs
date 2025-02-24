package main

import (
	"fmt"
	"io"
	"os"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdPut(dbName string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs put <src> <dst>")
		os.Exit(1)
	}

	src := args[0]
	dst := args[1]

	fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	srcFile, err := os.Open(src)
	if err != nil {
		fmt.Printf("Failed to open source file: %v\n", err)
		os.Exit(1)
	}
	defer srcFile.Close()

	dstFile, err := fs.Create(dst)
	if err != nil {
		fmt.Printf("Failed to create destination file: %v\n", err)
		os.Exit(1)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		fmt.Printf("Failed to copy file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully copied %s to %s\n", src, dst)
}
