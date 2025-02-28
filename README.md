# SQLiteFS - A SQLite-backed Filesystem Implementation
# SQLiteFS - 基于 SQLite 的文件系统实现

## Overview / 概述

SQLiteFS is a Go implementation of a filesystem using SQLite as the storage backend. It provides a virtual filesystem interface compatible with the `billy.Filesystem` interface from the go-billy package.

SQLiteFS 是一个使用 SQLite 作为存储后端的文件系统 Go 实现。它提供了一个与 go-billy 包的 `billy.Filesystem` 接口兼容的虚拟文件系统接口。

## Features / 特性

- **Persistent Storage**: All files and directories are stored in a SQLite database, providing durability and portability.
- **持久化存储**: 所有文件和目录都存储在 SQLite 数据库中，提供持久性和可移植性。

- **Transaction Support**: Filesystem operations can be wrapped in transactions for atomicity.
- **事务支持**: 文件系统操作可以包装在事务中，保证原子性。

- **Compatibility**: Implements the standard `billy.Filesystem` interface for easy integration.
- **兼容性**: 实现了标准的 `billy.Filesystem` 接口，便于集成。

- **Concurrency**: Supports concurrent access with proper locking mechanisms.
- **并发性**: 支持通过适当的锁定机制进行并发访问。

## Installation / 安装

```bash
go get github.com/nzinfo/go-sqlfs
```

## Usage / 使用

```go
// Create a new SQLiteFS instance
// 创建一个新的 SQLiteFS 实例
sqlfs, fs, err := sqlfs.NewSQLiteFS(":memory:")
if err != nil {
    log.Fatal(err)
}
defer sqlfs.Close()

// Create a new file
// 创建一个新文件
f, err := fs.Create("test.txt")
if err != nil {
    log.Fatal(err)
}

// Write to the file
// 写入文件
_, err = f.Write([]byte("Hello, SQLiteFS!"))
if err != nil {
    log.Fatal(err)
}

// Read from the file
// 从文件读取
data, err := ioutil.ReadAll(f)
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(data))
```

## Documentation / 文档

For detailed API documentation, please refer to the GoDoc:

完整的 API 文档请参考 GoDoc:

[https://pkg.go.dev/github.com/nzinfo/go-sqlfs](https://pkg.go.dev/github.com/nzinfo/go-sqlfs)

## Contributing / 贡献

We welcome contributions! Please see our [CONTRIBUTING.md](CONTRIBUTING.md) file for details.

我们欢迎贡献！详情请参见 [CONTRIBUTING.md](CONTRIBUTING.md) 文件。

## License / 许可证

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

本项目采用 MIT 许可证 - 详情请参见 [LICENSE](LICENSE) 文件。
