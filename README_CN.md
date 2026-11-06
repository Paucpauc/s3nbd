# s3nbd

[English](README.md) | [Русский](README_RU.md) | 简体中文

用 Go 编写的 NBD 服务器，将块设备数据存储在兼容 S3 的存储中。支持多个命名设备，每个设备允许一个活跃客户端。NBD 仅通过本机回环 TCP 连接提供服务，不使用 TLS；S3 使用 HTTP 或 HTTPS、path-style 寻址和 Signature V4 签名。

## 快速开始

需要 Go 1.26+、预先创建的 S3 存储桶，以及 Linux 上的 `nbd-client`。

```sh
go build -o s3nbd .
cp config.example.json config.json
```

在 `config.json` 中填写服务端点、区域、存储桶和设备大小，然后启动服务器：

```sh
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
./s3nbd -config config.json
```

在另一个终端中连接设备：

```sh
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N disk1
sudo nbd-client 127.0.0.1 10809 /dev/nbd1 -N disk2
```

现在可以使用标准 Linux 工具格式化和挂载设备。停止服务器前，请先卸载文件系统并断开 NBD 连接。

## 配置

完整示例见 [config.example.json](config.example.json)。

| 配置项 | 说明 |
| --- | --- |
| `Listen` | NBD 监听地址，必须为回环 IP 地址。 |
| `Endpoint`, `Region` | S3 服务端点和请求签名区域。 |
| `Devices` | 各设备的 `Name`、`Bucket`、`Prefix` 和以字节为单位的 `Size`。 |
| `BlockSize` | 每个 S3 对象的字节数。设备大小必须是该值的整数倍。 |
| `CacheBlocks` | 每个设备可缓存的块数。 |
| `Workers` | 每个设备的 S3 请求并发数及最大活跃 NBD 读取数。 |
| `FlushSeconds` | 后台刷写间隔，单位为秒。 |
| `YandexPatch` | 启用 Yandex Object Storage PATCH，默认关闭。 |
| `InsecureTLS` | 禁用 S3 证书验证，默认关闭。 |

使用临时凭证时，请设置 `S3_SESSION_TOKEN`。使用自定义 CA 时，请通过系统信任库或 `SSL_CERT_FILE` 配置证书。

## 存储与数据持久性

**写入内存后即确认写入成功。进程崩溃或断电会导致尚未刷写的数据丢失。** FLUSH 和带 FUA 的写入会等待所有待处理更改保存到 S3。上传失败的块会保留为脏块以便重试，客户端会收到 `EIO`。

后台每隔 `FlushSeconds` 秒刷写一次；客户端断开连接或收到 SIGINT/SIGTERM 时也会触发刷写。如果最后一次刷写失败，服务器会报错退出，无法保留内存中的剩余数据。

每个 `Prefix/blocks/<16 hex digits>` 对象存储一个块，名称以 16 位十六进制数字结尾。不存在的对象读取为零。写入会在缓存中累积，不发送 GET 请求。刷写部分修改的块时，会先读取已有数据，再上传整个块；完整覆盖则无需读取。全零块会被删除，而不是上传。

缓存最多占用 **每个设备** `CacheBlocks × BlockSize` 字节，此外还需为请求和 SDK 分配内存。活跃读取的响应缓冲区最多可额外占用 `Workers × 32 MiB`。缓存满时，服务器会淘汰干净块或刷写脏块。读取可并发执行；写入、丢弃和刷写会等待先前的读取完成。

请注意以下限制：

- 对于已有数据，不要更改设备大小或 `BlockSize`；这些值仅保存在配置文件中。
- 每个存储桶/前缀组合只能由一个服务器使用，不要从外部修改其对象。
- 存储服务必须保证写后读一致性，以及已确认写入的数据持久性。跨多个对象的刷写不是原子操作。
- NBD 使用 fixed-newstyle 协商，支持命名导出、READ/WRITE、FLUSH、FUA 和 TRIM。READ/WRITE 请求大小上限为 32 MiB；服务器不声明支持多连接。

## 丢弃未使用的块

运行 `sudo fstrim /mountpoint` 可释放文件系统中未使用的块。整块丢弃会转换为 S3 批量删除；部分丢弃则将指定范围置零。这两种操作都会缓存在内存中，直到刷写时才提交，但读取会立即返回零。删除失败的块会保留为脏块以便重试。

启用 S3 版本控制时，删除操作会创建删除标记。请使用存储生命周期策略清理旧版本。

## Yandex Object Storage PATCH

设置 `"YandexPatch": true`、`"Region": "ru-central1"` 和相应的 HTTPS 服务端点。**必须关闭存储桶版本控制。** PATCH 是 Yandex 的扩展，不属于标准 S3。

此模式在部分更新已有对象时只发送修改过的范围，避免下载和上传整个块。新对象和完整覆盖使用 PUT，整块丢弃使用批量删除。如果 PATCH 返回 `404 NoSuchKey`，服务器会改用 PUT 上传整个块，修改范围之外的内容填零。其他错误会让块保持脏状态以便重试，并使 FLUSH/FUA 失败。

详见 [PATCH API 文档](https://yandex.cloud/ru/docs/storage/s3/api-ref/object/patch)。

## 日志与测试

服务器每五秒记录一次：

- `S3 stats`：按 HTTP 方法统计所有设备的请求数，以及发送和接收的正文总字节数。包含重试和失败请求，不包含 HTTP 头及 TCP/TLS 开销。
- `cache stats`：每个设备的脏对象数和脏数据字节数。字节数按整个块计算，包含待删除的块，而不只是修改过的范围。

运行测试：

```sh
go test -race ./...
```

