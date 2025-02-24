package main

import (
	"fmt"
	"os"
)

func cmdChown(dbName string, mirrorPath string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs chown <user> <file>")
		os.Exit(1)
	}
	/*
		user := args[0]
		file := args[1]

		fs, err := sqlfs.NewSQLiteFS(dbName)
		if err != nil {
			fmt.Printf("Failed to initialize SQLFS: %v\n", err)
			os.Exit(1)
		}

		if err := fs.Chown(file, user); err != nil {
			fmt.Printf("Failed to change file owner: %v\n", err)
			os.Exit(1)
		}

		if mirrorPath != "" {
			if err := os.Chown(filepath.Join(mirrorPath, file), os.Getuid(), os.Getgid()); err != nil {
				fmt.Printf("Failed to change mirror file owner: %v\n", err)
				os.Exit(1)
			}
		}

		fmt.Printf("Successfully changed owner of %s to %s\n", file, user)
	*/
}
