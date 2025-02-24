package main

import (
	"fmt"
	"os"
)

func cmdGet(dbName string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs get <src> <dst>")
		os.Exit(1)
	}
	src := args[0]
	dst := args[1]
	fmt.Printf("Copying from %s to %s\n", src, dst)
	// TODO: Implement file copy logic
}
