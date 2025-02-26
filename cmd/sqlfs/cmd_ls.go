package main

import (
	"fmt"
	"os"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdLs(dbName string, mirrorPath string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs ls <path>")
		os.Exit(1)
	}

	path := args[0]

	_, fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	entries, err := fs.ReadDir(path)
	if err != nil {
		fmt.Printf("Failed to read directory: %v\n", err)
		os.Exit(1)
	}

	for _, entry := range entries {
		fmt.Println(entry.Name())
	}
}
