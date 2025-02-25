build:
	go build -o bin/sqlfs ./cmd/sqlfs

build-proto:
	go build -tags proto -o bin/sqlfs ./cmd/sqlfs

clean:
	rm -rf bin

.PHONY: build build-proto clean
