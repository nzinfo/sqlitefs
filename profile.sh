#!/bin/bash

# Build the program
go build -o bin/sqlfs cmd/sqlfs/main.go

# Run with CPU profiling
./bin/sqlfs -cpuprofile=cpu.prof put /home/nzinfo/Downloads/drawio-24.8.9.tar.gz drawio

# Analyze CPU profile
go tool pprof -http=:8080 cpu.prof

# Memory profiling will be written at program exit
