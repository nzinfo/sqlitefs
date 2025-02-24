package main

import (
	"fmt"
	"os"
)

func cmdCat(dbName string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs cat <file>")
		os.Exit(1)
	}
	file := args[0]
	fmt.Printf("Displaying contents of %s\n", file)
	// TODO: Implement file content display logic
}
