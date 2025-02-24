package main

import (
	"fmt"
	"os"
)

func cmdChmod(dbName string, mirrorPath string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs chmod <mode> <file>")
		os.Exit(1)
	}

	/*
		mode := args[0]
		file := args[1]

		fs, err := sqlfs.NewSQLiteFS(dbName)
		if err != nil {
			fmt.Printf("Failed to initialize SQLFS: %v\n", err)
			os.Exit(1)
		}

		if err := fs.Chmod(file, mode); err != nil {
			fmt.Printf("Failed to change file mode: %v\n", err)
			os.Exit(1)
		}

		if mirrorPath != "" {
			if err := os.Chmod(filepath.Join(mirrorPath, file), mode); err != nil {
				fmt.Printf("Failed to change mirror file mode: %v\n", err)
				os.Exit(1)
			}
		}

		fmt.Printf("Successfully changed mode of %s to %s\n", file, mode)
	*/
}
