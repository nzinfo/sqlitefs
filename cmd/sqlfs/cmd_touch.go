package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdTouch(dbName string, mirrorPath string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs touch <file>")
		os.Exit(1)
	}

	file := args[0]

	fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	_, err = fs.Create(file)
	if err != nil {
		fmt.Printf("Failed to create file: %v\n", err)
		os.Exit(1)
	}

	if mirrorPath != "" {
		if err := os.WriteFile(filepath.Join(mirrorPath, file), []byte{}, 0666); err != nil {
			fmt.Printf("Failed to create mirror file: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("Successfully touched file %s\n", file)
}
