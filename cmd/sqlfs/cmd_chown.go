package main

import (
	"fmt"
	"os"
)

func cmdChown(dbName string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs chown <user> <file>")
		os.Exit(1)
	}
	user := args[0]
	file := args[1]
	fmt.Printf("Changing owner of %s to %s\n", file, user)
	// TODO: Implement file owner change logic
}
