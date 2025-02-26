build:
	go build -o bin/sqlfs ./cmd/sqlfs

# build-proto:  # 目前使用 proto 的实现作为默认实现。
# 	go build -tags proto -o bin/sqlfs ./cmd/sqlfs

clean:
	rm -rf bin

.PHONY: build build-proto clean
