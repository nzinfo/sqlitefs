package main

import (
	"fmt"
	"os"
)

func cmdMv(dbName string, args []string) {
	if len(args) < 2 {
		fmt.Println("Usage: sqlfs mv <src> <dst>")
		os.Exit(1)
	}
	src := args[0]
	dst := args[1]
	fmt.Printf("Moving %s to %s\n", src, dst)
	// TODO: Implement file move logic
}
