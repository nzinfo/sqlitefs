1. 类似 jupyter , 目录是一个特别的文件，列出该目录下的所有文件
2. 文件关联一个特别的 ID 作为 FileID, 也称作 StreamID
3. 文件可以关联多个 Tag
4. 当访问多级目录下的文件，需要逐级解析子目录
5. 目录作为特别的文件，可以分别存放 子目录 与 常规的文件
6. 文件的正文存储到 Blocks 表中，每个 Block 最大为 2M, 最小为 4K，大小需要对齐到 4K
7. Block 可以包括一个或多个文件的正文
8. Block 中的数据与 File 的关系，记录在表 file chunks 中, 至少包括
    fileId/streamId, offset, size, block_offset, crc32
9. 文件的内容只追加、覆盖、不修改
10. 在实际打开文件时，需要根据 chunks 构造访问文件所需要的线段树
    - 对文件内容的覆盖应对齐到 4K ，如果某个 offset / offset + size 不满足，需要读取原数据作为 padding
    - 当识别到某个 chunk 已经被其他后续的 chunk 所屏蔽，应额外作为日志输出，并进而标记在数据库中。

每次写入最大为 64K 因为 NVME 设备上假定为 8 通道，每次写入 8K;

Blob 不允许改写，一次最大2M , 对齐到 Linux Huge Page 的最小单位;

Blob 如果不足 2M , 则对齐到最近的 4K 的整数倍;
    
    - 避免单一 Page 被改写



1. 如果文件有预先分配的空间，且 >= 1M , 则单独存在 ring Buffer
2. 默认为小文件混合，公用一个 Blob.
3. 目前的实现，只要写入，即不更改
    
    - 仅追加、覆盖

如果后续要支持 mmap , 其默认 page size 应为 4K .

使用类似 jupyter 的结构构造目录

文件系统对 目录结构进行缓存

# 测试

1. 从指定的目录中读取文本文件
    
    - 可以限定并发写入数量

2. 需要定制的写入机制

    - 32K A | B | C | D
