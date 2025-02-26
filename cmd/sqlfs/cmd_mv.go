package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdMv(dbName string, mirrorPath string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs mv <src> <dst>")
		os.Exit(1)
	}

	src := args[0]
	dst := args[1]

	_, fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	if err := fs.Rename(src, dst); err != nil {
		fmt.Printf("Failed to move file: %v\n", err)
		os.Exit(1)
	}

	if mirrorPath != "" {
		if err := os.Rename(filepath.Join(mirrorPath, src), filepath.Join(mirrorPath, dst)); err != nil {
			fmt.Printf("Failed to move mirror file: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully moved %s to %s\n", src, dst)
}
