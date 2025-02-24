package main

import (
	"fmt"
	"os"
)

func cmdMkdir(dbName string, args []string) {
	if len(args) < 1 {
		fmt.Println("Usage: sqlfs mkdir <directory>")
		os.Exit(1)
	}
	dir := args[0]
	fmt.Printf("Creating directory %s\n", dir)
	// TODO: Implement directory creation logic
}
