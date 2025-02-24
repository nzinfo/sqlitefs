package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	dbName := flag.String("db", "sqlfs.db", "SQLite database file")
	mirrorPath := flag.String("mirror", "", "Specify a mirror path")

	flag.Parse()

	if len(flag.Args()) < 1 {
		cmdHelp()
		os.Exit(1)
	}

	command := flag.Args()[0]
	args := flag.Args()[1:]

	switch command {
	case "put":
		cmdPut(*dbName, *mirrorPath, args)
	case "get":
		cmdGet(*dbName, *mirrorPath, args)
	case "ls":
		cmdLs(*dbName, *mirrorPath, args)
	case "touch":
		cmdTouch(*dbName, *mirrorPath, args)
	case "mv":
		cmdMv(*dbName, *mirrorPath, args)
	case "cat":
		cmdCat(*dbName, *mirrorPath, args)
	case "chmod":
		cmdChmod(*dbName, *mirrorPath, args)
	case "chown":
		cmdChown(*dbName, *mirrorPath, args)
	case "mkdir":
		cmdMkdir(*dbName, *mirrorPath, args)
	case "rm":
		cmdRm(*dbName, *mirrorPath, args)
	case "link":
		cmdLink(*dbName, *mirrorPath, args)
	//case "truncate":
	//	cmdTruncate(*dbName, *mirrorPath, args)
	case "help":
		cmdHelp()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}
