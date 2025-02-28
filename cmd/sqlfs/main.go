package main

import (
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
)

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var memprofile = flag.String("memprofile", "", "write memory profile to file")
	dbName := flag.String("db", "sqlfs.db", "SQLite database file")
	mirrorPath := flag.String("mirror", "", "Specify a mirror path")

	flag.Parse()

	if len(flag.Args()) < 1 {
		cmdHelp()
		os.Exit(1)
	}

	// CPU profiling
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
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

	// Memory profiling
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "could not write memory profile: %v\n", err)
			os.Exit(1)
		}
	}
}
