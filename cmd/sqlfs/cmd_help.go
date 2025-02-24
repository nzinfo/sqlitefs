package main

import (
	"fmt"
)

func cmdHelp() {
	fmt.Println("Usage: sqlfs [--db=<db_name>] <command>")
	fmt.Println("Available commands:")
	fmt.Println("  put       Copy from external filesystem to sqlfs")
	fmt.Println("  get       Copy from sqlfs to external filesystem")
	fmt.Println("  ls        List contents of a directory")
	fmt.Println("  touch     Create or update a file")
	fmt.Println("  mv        Move a file or directory")
	fmt.Println("  cat       Display contents of a file")
	fmt.Println("  chmod     Change file permissions")
	fmt.Println("  chown     Change file owner")
	fmt.Println("  mkdir     Create a directory")
	fmt.Println("  rm        Remove a file")
	fmt.Println("  link      Create a link")
	fmt.Println("  truncate  Truncate a file to a specified size")
	fmt.Println("  help      Show this help message")
}
