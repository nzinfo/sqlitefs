package main

import (
	"fmt"
	"os"
)

func cmdRm(dbName string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs rm <file>")
		os.Exit(1)
	}
	file := args[0]
	fmt.Printf("Removing file %s\n", file)
	// TODO: Implement file removal logic
}
