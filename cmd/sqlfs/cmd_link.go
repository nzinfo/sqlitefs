package main

import (
	"fmt"
	"os"
)

func cmdLink(dbName string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs link <src> <dst>")
		os.Exit(1)
	}
	src := args[0]
	dst := args[1]
	fmt.Printf("Creating link from %s to %s\n", src, dst)
	// TODO: Implement link creation logic
}
