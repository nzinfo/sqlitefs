package main

import (
	"fmt"
	"io"
	"os"

	"github.com/go-git/go-billy/v6"
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

	// Initialize osfs for mirror only if mirrorPath is not empty
	var mirrorFS billy.Filesystem
	if mirrorPath != "" {
		mirrorFS = osfs.New(mirrorPath)
	}

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

	// If mirrorFS is initialized, also copy to mirror
	if mirrorPath != "" && mirrorFS != nil {
		mirrorDstFile, err := mirrorFS.Create(dst)
		if err != nil {
			fmt.Printf("Failed to create mirror destination file: %v\n", err)
			os.Exit(1)
		}
		defer mirrorDstFile.Close()

		if _, err := io.Copy(mirrorDstFile, srcFile); err != nil {
			fmt.Printf("Failed to copy file to mirror: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully copied %s to %s\n", src, dst)
}
