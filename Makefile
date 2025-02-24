build:
	go build -o bin/sqlfs ./cmd/sqlfs

clean:
	rm -rf bin

.PHONY: build clean
