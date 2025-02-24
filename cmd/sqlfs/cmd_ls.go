package main

import (
	"fmt"
	"os"
)

func cmdLs(dbName string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs ls <path>")
		os.Exit(1)
	}
	path := args[0]
	fmt.Printf("Listing contents of %s\n", path)
	// TODO: Implement directory listing logic
}
