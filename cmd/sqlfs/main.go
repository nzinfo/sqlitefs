package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	dbName := flag.String("db", "sqlfs.db", "SQLite database file")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Println("Usage: sqlfs [--db=<db_name>] <command>")
		os.Exit(1)
	}

	command := flag.Args()[0]
	args := flag.Args()[1:]

	switch command {
	case "put":
		cmdPut(*dbName, args)
	case "get":
		cmdGet(*dbName, args)
	case "ls":
		cmdLs(*dbName, args)
	case "touch":
		cmdTouch(*dbName, args)
	case "mv":
		cmdMv(*dbName, args)
	case "cat":
		cmdCat(*dbName, args)
	case "chmod":
		cmdChmod(*dbName, args)
	case "chown":
		cmdChown(*dbName, args)
	case "mkdir":
		cmdMkdir(*dbName, args)
	case "rm":
		cmdRm(*dbName, args)
	case "link":
		cmdLink(*dbName, args)
	case "truncate":
		cmdTruncate(*dbName, args)
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}
