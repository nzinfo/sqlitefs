package main

import (
	"fmt"
	"os"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdLink(dbName string, mirrorPath string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs link <src> <dst>")
		os.Exit(1)
	}

	src := args[0]
	dst := args[1]

	_, fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	if err := fs.Symlink(src, dst); err != nil {
		fmt.Printf("Failed to create link: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Successfully linked %s to %s\n", src, dst)
}
