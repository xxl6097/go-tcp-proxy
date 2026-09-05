# tcpfwd - 极简 TCP 端口转发器（库 + 命令行）

一行配置、零依赖、单二进制。核心转发逻辑以 Go 库形式提供，
`cmd/tcpfwd` 只是命令行壳子。

- 默认：upstream 断开立即关闭客户端（向后兼容）。
- 可选：开启 `--retry-interval` 后，upstream 拨号失败 / 中途断开都按
  间隔重试/重连，对上游抖动有韧性。

## 项目结构

```
tcpfwd/
├── go.mod                      # module github.com/uuxia/tcpfwd
├── proxy/                      # 库，import "github.com/uuxia/tcpfwd/proxy"
│   ├── proxy.go
│   ├── proxy_test.go
│   └── example_test.go
└── cmd/tcpfwd/                 # 命令行入口
    └── main.go
```

## 作为库使用

```bash
go get github.com/uuxia/tcpfwd/proxy
```

```go
import "github.com/uuxia/tcpfwd/proxy"

// 最小用例
p := &proxy.Proxy{
    UpstreamAddr: "10.0.0.5:3306",
    ReadTimeout:  5 * time.Minute,
    DialTimeout:  3 * time.Second,
    Log:          slog.Default(),
}
_ = p.Run(ctx)            // 让 Run 自己 listen
// 或者
p.UseListener(myListener) // 复用已有 socket
_ = p.Run(ctx)

// 开启上游抖动韧性（拨号失败 + 中途断开都重试）
_ = p.Run(ctx, proxy.WithUpstreamRetry(0, 2*time.Second)) // 无限重试
_ = p.Run(ctx, proxy.WithUpstreamRetry(5, 2*time.Second)) // 最多 5 次
```

完整示例见 `proxy/example_test.go`，涵盖：基本用法、自定义 logger、
ctx 触发多个 Proxy 优雅退出、上游抖动重连。

### 公开 API

```go
type Proxy struct {
    ListenAddr    string        // 本地监听地址，如 ":8080"
    UpstreamAddr  string        // 目标地址，如 "10.0.0.5:3306"
    ReadTimeout   time.Duration // 单方向读空闲超时；0 表示关闭
    DialTimeout   time.Duration // 与 upstream 建连超时；0 = 5s 默认
    MaxAttempts   int           // upstream 拨号+重连总尝试上限；0=关闭，<0=无限，>0=N
    RetryInterval time.Duration // 两次尝试间隔；>0 启用重试，<=0=1s 默认
    Log           *slog.Logger  // nil 时回退到 slog.Default()
}

func (p *Proxy) Run(ctx context.Context, opts ...Option) error
func (p *Proxy) UseListener(ln net.Listener)
func (p *Proxy) Addr() string  // 实际监听地址（"127.0.0.1:54321"）

type Option func(*Proxy)
func WithUpstreamRetry(maxAttempts int, interval time.Duration) Option
```

字段都可以直接读 / 直接赋值；常用模式就是结构体字面量 + `Run(ctx)`。

## 作为命令行工具

```bash
# 编译到 $GOBIN/tcpfwd
go install ./cmd/tcpfwd

# 或在仓库根目录快速跑
go run ./cmd/tcpfwd --listen :8080 --upstream 10.0.0.5:3306

# 开启上游抖动重试：每 2s 一次，最多 5 次（0 = 无限）
go run ./cmd/tcpfwd --listen :8080 --upstream 10.0.0.5:3306 \
    --retry-interval 2s --retry-attempts 5
```

参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--listen` | `:8080` | 本地监听地址 |
| `--upstream` | （必填） | 目标地址 |
| `--read-timeout` | `0` | 双向读空闲超时；`0` = 不限 |
| `--dial-timeout` | `5s` | upstream 建连超时 |
| `--log-level` | `info` | `debug` / `info` / `warn` / `error` |
| `--retry-attempts` | `0` | upstream 拨号+重连总尝试上限；`<=0` = 无限（仅 ctx 取消才停） |
| `--retry-interval` | `0` | 两次尝试间隔；`>0` 启用重试，`<=0` 不启用 |

> **注意：** `--retry-interval <= 0` 时整体保持默认行为（upstream 一断就关客户端），
> 即使 `--retry-attempts > 0` 也不会触发任何重试。

## 行为约定

- 一条连接两个 goroutine：客户端 → 上游、上游 → 客户端。
- **默认模式**：任一方向关闭 → 立即关闭对端连接。不做保活、不做重试。
  适合对端故障需要立刻被业务感知、不希望被静默重连掩盖的场景。
- **重试模式**（`--retry-interval > 0` 或 `WithUpstreamRetry(..., >0)`）：
  - upstream 拨号失败 → 等 `retry-interval` 重试；
  - upstream 建立后中途断开（网络抖动 / 服务重启）→ 等 `retry-interval` 重连；
    客户端连接保持活跃；
  - 重连期间客户端正在发的数据会被**丢弃**（不缓存，c2u 在重连期间暂停读）；
  - 客户端主动断开或 ctx 取消 → 立即退出；
  - 超出 `retry-attempts` → 关闭客户端连接并打 `upstream_give_up` 日志。
- `--read-timeout` 用于清理长时间闲置的连接（如 `300s`）。
- `Run` 的 ctx 取消 → 立刻停止接收新连接，等所有活跃连接收尾后返回。

## 日志

每条事件一行 key=value：

```
time=2026-09-05T17:00:01.234+08:00 level=INFO msg=proxy_listen listen=[::]:8080 upstream=10.0.0.5:3306 read_timeout=0s dial_timeout=5s max_attempts=5 retry_interval=2s
time=2026-09-05T17:00:03.120+08:00 level=INFO msg=conn_open conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306
time=2026-09-05T17:00:04.010+08:00 level=INFO msg=upstream_connected conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306 attempt=1
time=2026-09-05T17:00:05.500+08:00 level=WARN msg=upstream_disconnect conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306 attempt=1 err=EOF rx_so_far=128 tx_so_far=512
time=2026-09-05T17:00:07.620+08:00 level=INFO msg=upstream_connected conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306 attempt=2
time=2026-09-05T17:00:08.430+08:00 level=INFO msg=conn_close conn_id=000001 client=192.168.1.5:54321 upstream=10.0.0.5:3306 rx_bytes=128 tx_bytes=512 attempts=2 duration_ms=5410
```

`max_attempts` 取值：`off` / `unlimited` / 数字。

## 测试

```bash
go test ./...                       # 全部单元 + 示例测试
go test -race -count=10 ./...       # 跑 10 轮 race detector，验证稳定性
```

覆盖场景：

1. 端到端回环（echo）。
2. upstream 不可达时客户端连接不被挂住。
3. ctx 取消后 Run 优雅退出且无遗留 goroutine。
4. 日志字段（`proxy_listen` / `conn_open` / `conn_close` + `rx_bytes=` 等）。
5. **重试相关**：dial 重连、upstream 抖动重连、达到 MaxAttempts 后关闭客户端、
   默认模式（不传 Option）向后兼容。
6. ExampleProxy_basic / ExampleProxy_withLogger / ExampleProxy_shutdown /
   ExampleProxy_withRetry。

## 发布提示

仓库还没推到远端时，`go.mod` 里有这一对：

```
require github.com/uuxia/tcpfwd v0.0.0
replace github.com/uuxia/tcpfwd => ./
```

推到 `github.com/uuxia/tcpfwd` 后把这两行删掉即可，import 路径不变。
