package main

import (
	"fmt"
	"io"
	"os"

	"github.com/nzinfo/go-sqlfs/pkg/sqlfs"
)

func cmdCat(dbName string, mirrorPath string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs cat <file>")
		os.Exit(1)
	}

	file := args[0]

	_, fs, err := sqlfs.NewSQLiteFS(dbName)
	if err != nil {
		fmt.Printf("Failed to initialize SQLFS: %v\n", err)
		os.Exit(1)
	}

	srcFile, err := fs.Open(file)
	if err != nil {
		fmt.Printf("Failed to open source file: %v\n", err)
		os.Exit(1)
	}
	defer srcFile.Close()

	content, err := io.ReadAll(srcFile)
	if err != nil {
		fmt.Printf("Failed to read file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Contents of %s:\n%s\n", file, content)
}
