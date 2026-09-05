# tcpfwd - 极简 TCP 端口转发器

一行配置、零依赖、单二进制，适合临时端口转发、调试、跨网段跳板。

## 编译

```bash
go build -o tcpfwd .
# 或直接跑
go run . --listen :8080 --upstream 10.0.0.5:3306
```

需要 Go 1.22+。

## 用法

```
tcpfwd --listen <本地地址> --upstream <目标地址>
```

常用参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--listen` | `:8080` | 本地监听地址，如 `:8080` 或 `127.0.0.1:8080` |
| `--upstream` | （必填） | 目标地址，如 `10.0.0.5:3306` |
| `--read-timeout` | `0` | 双向读空闲超时；`0` 表示不限 |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error` |

## 行为约定

- 一条连接两个 goroutine：客户端 → 上游、上游 → 客户端。
- 任一方向关闭 → 立即关闭对端连接。不做保活、不做重试。
- `--read-timeout` 用于清理长时间闲置的连接（如 `300s`）。
- SIGINT / SIGTERM 触发优雅退出。

## 日志

每条事件一行 key=value，扁平化，便于 grep / awk 处理：

```
time=2026-09-05T17:00:01.234+08:00 level=INFO msg=proxy_listen listen=:8080 upstream=10.0.0.5:3306 read_timeout=0s
time=2026-09-05T17:00:03.120+08:00 level=INFO msg=conn_open conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306
time=2026-09-05T17:00:04.010+08:00 level=INFO msg=conn_close conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306 rx_bytes=128 tx_bytes=512 duration_ms=890
```

## 测试

```bash
go test ./...
```

覆盖三个场景：
1. 端到端回环（echo）。
2. upstream 不可达时客户端连接不被挂住。
3. ctx 取消后能优雅退出。
