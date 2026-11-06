# s3nbd

English | [Русский](README_RU.md) | [简体中文](README_CN.md) | [Español](README_ES.md) | [Français](README_FR.md)

A Go NBD server that stores block devices in S3-compatible storage. Supports multiple named devices, with one active client per device. NBD runs over TCP without TLS; S3 uses HTTP or HTTPS, path-style addressing, and Signature V4.

## Quick start

You need Go 1.26+, an existing S3 bucket, and `nbd-client` on Linux.

```sh
go build -o s3nbd .
cp config.example.json config.json
```

Edit `config.json` with your endpoint, region, bucket, and device sizes, then start the server:

```sh
export S3_ACCESS_KEY=your-access-key
export S3_SECRET_KEY=your-secret-key
./s3nbd -config config.json
```

In another terminal, connect the devices:

```sh
sudo modprobe nbd
sudo nbd-client 127.0.0.1 10809 /dev/nbd0 -N disk1
sudo nbd-client 127.0.0.1 10809 /dev/nbd1 -N disk2
```

You can now format and mount them using standard Linux tools. Before stopping the server, unmount the filesystems and disconnect NBD.

## Configuration

See [config.example.json](config.example.json) for a complete example.

| Setting | What it controls |
| --- | --- |
| `LogLevel` | Minimum log level: `debug`, `info` (default), `warn`, or `error`; case-insensitive. Applied at startup; invalid values prevent startup. Logs go to stderr with timestamps and levels. Statistics and lifecycle events use info, NBD connections/commands debug, warnings warn, and operation failures error. |
| `Listen` | NBD listen address (`host:port`); use `0.0.0.0:10809` to listen on all IPv4 interfaces. Defaults to `127.0.0.1:10809`. |
| `Endpoint`, `Region` | S3 endpoint and signing region. |
| `Devices` | Each device's `Name`, `Bucket`, `Prefix`, and `Size` in bytes. |
| `BlockSize` | Bytes per S3 object. Device sizes must be a multiple of this. |
| `CacheBlocks` | Cached blocks per device. |
| `Workers` | Per-device S3 concurrency and maximum active NBD reads. |
| `FlushSeconds` | Background flush interval. |
| `YandexPatch` | Enable Yandex Object Storage PATCH; off by default. |
| `InsecureTLS` | Disable S3 certificate verification; off by default. |

Set `S3_SESSION_TOKEN` for temporary credentials. For a custom CA, use the system trust store or `SSL_CERT_FILE`.

## Storage and durability

**Writes are acknowledged in RAM. A crash loses any data that has not been flushed.** FLUSH and writes with FUA wait for pending changes to reach S3. Failed uploads stay dirty for retry, and the client receives `EIO`.

Background flushes run every `FlushSeconds`; disconnect and SIGINT/SIGTERM also trigger a flush. If the final flush fails, the server exits with an error and cannot preserve the remaining RAM contents.

Each object at `Prefix/blocks/<16 hex digits>` holds one block. Missing objects read as zeros. Writes accumulate in the cache without GET requests. A partial-block flush loads existing data before uploading the full block; a full overwrite skips that read. All-zero blocks are deleted instead of uploaded.

The cache holds up to `CacheBlocks × BlockSize` bytes **per device**, plus request and SDK memory. Active reads can use another `Workers × 32 MiB` of response buffers. When full, the cache evicts a clean block or flushes dirty blocks. Reads run concurrently; writes, discards, and flushes wait for earlier reads to finish.

Keep these constraints in mind:

- Do not change device sizes or `BlockSize` for existing data; they are stored only in the config.
- Use one server per bucket/prefix, and do not modify its objects externally.
- Storage must provide read-after-write consistency and durable acknowledged writes. A flush across several objects is not atomic.
- NBD uses fixed-newstyle negotiation with named exports, READ/WRITE, FLUSH, FUA, and TRIM. READ/WRITE requests are limited to 32 MiB; multi-connection support is not advertised.

## Discard

Run `sudo fstrim /mountpoint` to release unused filesystem blocks. Full-block discards become batched S3 deletions; partial discards zero the selected range. Both are buffered until flush, and reads return zeros immediately. Failed deletions stay dirty for retry.

With S3 versioning enabled, deletion creates a delete marker. Use a storage lifecycle policy to remove old versions.

## Yandex Object Storage PATCH

Set `"YandexPatch": true`, `"Region": "ru-central1"`, and the appropriate HTTPS endpoint. **Bucket versioning must be disabled.** PATCH is a Yandex extension, not standard S3.

This mode sends only changed ranges for partial updates to existing objects, avoiding full-block downloads and uploads. New objects and full overwrites use PUT; full discards use batch deletion. A PATCH response of `404 NoSuchKey` falls back to a full-block PUT with zeros outside the changed ranges. Other errors leave the block dirty for retry and fail FLUSH/FUA.

See the [PATCH API documentation](https://yandex.cloud/ru/docs/storage/s3/api-ref/object/patch).

## Logs and tests

Every five seconds, the server logs:

- `S3 stats`: request counts and body bytes sent/received per HTTP method, across all devices. Includes retries and failures; excludes headers and TCP/TLS overhead.
- `cache stats`: dirty objects and bytes per device. Bytes count whole blocks, including pending deletions, rather than just modified ranges.

Run the tests with:

```sh
go test -race ./...
```
