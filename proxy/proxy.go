// Package proxy 提供最小可用的 TCP 端口转发（TCP forwarding / TCP proxy）能力。
//
// 一条连接两个 goroutine：client -> upstream、upstream -> client。
// 任一方向结束即立刻关闭另一端，避免死锁。
//
// 默认行为：upstream 任何一次断开 → 立即关闭客户端连接。
// 用 WithUpstreamRetry 可开启拨号重试 + 中途重连能力，对上游短暂抖动有韧性。
//
// 最简用法：
//
//	ln, _ := net.Listen("tcp", ":8080")
//	p := &proxy.Proxy{UpstreamAddr: "10.0.0.5:3306"}
//	p.UseListener(ln)
//	_ = p.Run(ctx) // ctx 取消时 Run 优雅退出
//
// 也可以让 Run 自己 Listen：
//
//	p := &proxy.Proxy{ListenAddr: ":8080", UpstreamAddr: "10.0.0.5:3306"}
//	_ = p.Run(ctx)
//
// 用 Functional Options 调整行为：
//
//	p := &proxy.Proxy{ListenAddr: ":8080", UpstreamAddr: "10.0.0.5:3306",
//	    ReadTimeout: 5 * time.Minute}
//	_ = p.Run(ctx, proxy.WithUpstreamRetry(5, 2*time.Second))
//
// 日志通过 slog.Logger 注入，nil 时回退到 slog.Default()。
package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Option 是 Proxy 的可选行为配置。Functional Options 风格。
type Option func(*Proxy)

// WithUpstreamRetry 开启上游拨号 / 中途断开后的自动重试与重连。
//
//   - maxAttempts：总尝试次数上限（含首次连接与所有重连）。
//     <=0 表示无限次，仅 ctx 取消才会停止。
//   - interval：两次尝试之间的等待时间。<=0 时默认 1 秒。
//     注意：interval > 0 是启用重试机制的开关；只设 maxAttempts 而不设
//     interval 不会触发任何重试。
//
// 行为：
//   - upstream 拨号失败 → 等待 interval 后重试；
//   - upstream 建立后中途断开（网络抖动 / 服务重启）→ 等待 interval 后重连；
//     客户端连接保持活跃；
//   - 客户端主动断开或 ctx 取消 → 立即退出；
//   - 超出 maxAttempts → 关闭客户端连接。
func WithUpstreamRetry(maxAttempts int, interval time.Duration) Option {
	return func(p *Proxy) {
		p.MaxAttempts = maxAttempts
		if interval > 0 {
			p.RetryInterval = interval
		}
	}
}

// Proxy 把 ListenAddr 上的入站 TCP 连接双向转发到 UpstreamAddr。
//
// 字段说明：
//   - ListenAddr 形如 ":8080" / "127.0.0.1:8080"；如果同时通过 UseListener
//     注入了 Listener，则 ListenAddr 留空也行（实际地址可调用 Addr() 拿）。
//   - UpstreamAddr 形如 "host:port"，支持 DNS 解析。
//   - ReadTimeout 单方向读空闲超时；0 表示不限制。
//   - DialTimeout 与上游建连超时；0 表示使用默认值（5 秒）。
//   - MaxAttempts upstream 拨号+重连总尝试上限。语义：
//     0  = 不启用重试（默认）；
//     <0 = 无限重试；
//     >0 = 最多尝试 N 次。
//     仅当 RetryInterval > 0 时生效；可用 WithUpstreamRetry 设置。
//   - RetryInterval 两次尝试间隔；<=0 = 1s 默认。
//     设置后自动启用重试模式。
//   - Log 外部注入；nil 时回退到 slog.Default()。
type Proxy struct {
	ListenAddr    string        // 本地监听地址，例如 ":8080"
	UpstreamAddr  string        // 目标地址，例如 "10.0.0.5:3306"
	ReadTimeout   time.Duration // 单方向读空闲超时；0 表示关闭
	DialTimeout   time.Duration // 与 upstream 建立 TCP 连接的超时；0 = 5s 默认
	MaxAttempts   int           // upstream 拨号+重连总尝试上限；0=关闭，<0=无限，>0=N
	RetryInterval time.Duration // 两次尝试间隔；>0 启用重试；<=0 = 1s 默认
	Log           *slog.Logger  // 外部注入；nil 时回退到 slog.Default()

	listener net.Listener // 测试用：可通过 UseListener 注入
	actualLn string       // 实际监听地址（端口监听后才有意义）
	mu       sync.Mutex   // 保护 actualLn

	wg       sync.WaitGroup
	wgCancel context.CancelFunc
}

// UseListener 注入一个已就绪的监听器（主要用于测试，也允许在外部已经 Listen
// 好端口的场景下复用已有 socket）。
func (p *Proxy) UseListener(ln net.Listener) {
	p.listener = ln
	p.setActualAddr(ln.Addr().String())
}

// Addr 返回实际监听地址。Run 未启动时返回 ListenAddr；启动后是实际绑定的端口
// （如 ":8080" → "[::]:8080" 或 "127.0.0.1:54321"）。
//
// 在 ":0"（随机端口）场景下尤其有用，可以拿到操作系统分配的端口。
func (p *Proxy) Addr() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.actualLn != "" {
		return p.actualLn
	}
	return p.ListenAddr
}

func (p *Proxy) setActualAddr(addr string) {
	p.mu.Lock()
	p.actualLn = addr
	p.mu.Unlock()
}

// effectiveRetryInterval 返回实际生效的重试间隔。<=0 时回退到 1s。
func (p *Proxy) effectiveRetryInterval() time.Duration {
	if p.RetryInterval > 0 {
		return p.RetryInterval
	}
	return time.Second
}

// Run 阻塞运行，直到 ctx 取消或监听器失败。返回时会等所有活跃连接收尾。
//
// 可选 opts 在 Run 启动时一次性应用（典型用法：proxy.WithUpstreamRetry(...)）。
//
// Run 退出语义：
//   - ctx 取消 / 监听器关闭 → 立刻停止接收新连接，已建立的连接会继续走到 EOF；
//   - 监听 Accept 出错（除 net.ErrClosed 外）→ 返回该错误；
//   - 每个连接都有独立的 ctx，runCtx 取消时会一并取消所有 handle。
func (p *Proxy) Run(ctx context.Context, opts ...Option) error {
	for _, o := range opts {
		o(p)
	}
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.DialTimeout == 0 {
		p.DialTimeout = 5 * time.Second
	}
	ln := p.listener
	if ln == nil {
		l, err := net.Listen("tcp", p.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", p.ListenAddr, err)
		}
		ln = l
	}
	p.setActualAddr(ln.Addr().String())

	// runCtx 是 Run 自己的 ctx；它取消时，所有 handle 也要跟着退出。
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	p.wgCancel = runCancel

	p.Log.Info("proxy_listen",
		"listen", ln.Addr().String(),
		"upstream", p.UpstreamAddr,
		"read_timeout", p.ReadTimeout.String(),
		"dial_timeout", p.DialTimeout.String(),
		"max_attempts", p.maxAttemptsForLog(),
		"retry_interval", p.effectiveRetryInterval().String(),
	)

	go func() {
		<-runCtx.Done()
		_ = ln.Close()
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				runCancel()
				p.wg.Wait() // 等所有 handle 收尾，避免留下孤儿 goroutine
				return nil
			}
			p.Log.Warn("accept_error", "err", err.Error())
			runCancel()
			p.wg.Wait()
			return err
		}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handle(runCtx, c)
		}()
	}
}

func (p *Proxy) maxAttemptsForLog() string {
	switch {
	case p.MaxAttempts == 0:
		return "off"
	case p.MaxAttempts < 0:
		return "unlimited"
	default:
		return fmt.Sprintf("%d", p.MaxAttempts)
	}
}

// retryEnabled 判断重试模式是否启用。
// 启用条件：RetryInterval > 0（用户明确表态要做等待/重试）。
// 此时 MaxAttempts 决定上限（<=0 = 无限）。
func (p *Proxy) retryEnabled() bool {
	return p.RetryInterval > 0
}

// handle 是单条连接的全生命周期。
//
// 行为：
//   - 默认（MaxAttempts=0 且 RetryInterval=0）：尝试 dial upstream 一次，
//     失败立即关 client；建立后 splice 直到任一端 EOF/error 立即关 client；
//   - 配置 WithUpstreamRetry 后：dial 失败会按间隔重试；upstream 中途断开
//     会按间隔重连（client 保持）；超上限 / ctx 取消 / client 主动断开才退出。
func (p *Proxy) handle(parent context.Context, c net.Conn) {
	cid := newConnID()
	start := time.Now()
	client := c.RemoteAddr().String()
	defer c.Close()

	p.Log.Info("conn_open",
		"conn_id", cid,
		"client", client,
		"upstream", p.UpstreamAddr,
	)

	resilient := p.retryEnabled()
	attempts := 0
	var totalRx, totalTx int64

	for {
		if parent.Err() != nil {
			break
		}
		if resilient && p.MaxAttempts > 0 && attempts >= p.MaxAttempts {
			p.Log.Warn("upstream_give_up",
				"conn_id", cid,
				"client", client,
				"upstream", p.UpstreamAddr,
				"attempts", attempts,
				"max", p.MaxAttempts,
			)
			break
		}
		// 等待间隔（首次连接 attempts=0 时不实际等待）
		if resilient && attempts > 0 {
			if !sleepCtx(parent, p.effectiveRetryInterval()) {
				break
			}
			if parent.Err() != nil {
				break
			}
		}

		up, err := net.DialTimeout("tcp", p.UpstreamAddr, p.DialTimeout)
		attempts++
		if err != nil {
			p.Log.Warn("upstream_dial_fail",
				"conn_id", cid,
				"client", client,
				"upstream", p.UpstreamAddr,
				"attempt", attempts,
				"err", err.Error(),
			)
			if !resilient {
				break
			}
			continue
		}
		if attempts > 1 || resilient {
			p.Log.Info("upstream_connected",
				"conn_id", cid,
				"client", client,
				"upstream", p.UpstreamAddr,
				"attempt", attempts,
			)
		}

		c2uErr, u2cErr, rx, tx := p.pipe(parent, c, up)
		totalRx += rx
		totalTx += tx
		_ = up.Close()

		// 客户端主动断 / ctx 取消 → 直接退出
		if parent.Err() != nil || isClientSide(c2uErr) {
			break
		}
		// upstream 断开
		p.Log.Warn("upstream_disconnect",
			"conn_id", cid,
			"client", client,
			"upstream", p.UpstreamAddr,
			"attempt", attempts,
			"err", errStr(u2cErr),
			"rx_so_far", totalRx,
			"tx_so_far", totalTx,
		)
		if !resilient {
			break
		}
	}

	p.Log.Info("conn_close",
		"conn_id", cid,
		"client", client,
		"upstream", p.UpstreamAddr,
		"rx_bytes", totalRx,
		"tx_bytes", totalTx,
		"attempts", attempts,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

// pipe 启动双向 splice 直到任一端退出。
//
// 返回值：
//   - c2uErr: 客户端→upstream 方向错误（client 侧问题），非 nil 时整个连接应退出；
//   - u2cErr: upstream→客户端 方向错误（upstream 侧问题），可重连；
//   - rx/tx: 各自方向的写入字节数（与方向对应）。
func (p *Proxy) pipe(parent context.Context, c, up net.Conn) (c2uErr, u2cErr error, rx, tx int64) {
	pipeCtx, cancel := context.WithCancel(parent)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				p.Log.Error("c2u_panic", "panic", fmt.Sprintf("%v", r))
			}
		}()
		// c2u: 读 client → 写 upstream
		n, err := splice(pipeCtx, p, up, c)
		atomic.AddInt64(&rx, n)
		c2uErr = err
		cancel()
	}()
	go func() {
		defer wg.Done()
		defer func() {
			if r := recover(); r != nil {
				p.Log.Error("u2c_panic", "panic", fmt.Sprintf("%v", r))
			}
		}()
		// u2c: 读 upstream → 写 client
		n, err := splice(pipeCtx, p, c, up)
		atomic.AddInt64(&tx, n)
		u2cErr = err
		cancel()
	}()
	wg.Wait()
	return
}

// splice 把 src 的字节拷到 dst，返回写入字节数与最终错误。
//
// 退出条件（按优先级）：
//   - ctx 取消：通过 src.SetReadDeadline 让阻塞中的 Read 立即返回；splice
//     自身**不关闭任何连接**，由调用方（pipe / handle）负责释放；
//   - Read 拿到 EOF / 非 timeout 错误：返回 error（告诉上游是谁的锅）；
//   - Write 失败：返回 error。
//
// timeout（p.ReadTimeout > 0）：每次 Read 限定 timeout 时间，到期当作"空闲"继续等，
// 不算错误。
//
// 关键点：每次 splice 退出时（defer）必须把 src 的 ReadDeadline 还原成 time.Time{}，
// 因为同一条 client conn c 会在 reconnect 后被新一次 splice 复用，上一轮的
// SetReadDeadline(now) 会让下一次 Read 立刻 timeout 形成 busy-loop。
func splice(ctx context.Context, p *Proxy, dst, src net.Conn) (int64, error) {
	var total int64
	buf := make([]byte, 16*1024)
	stop := make(chan struct{})
	go func() {
		<-ctx.Done()
		// 只在 src 上设 ReadDeadline 让阻塞中的 Read 立即返回。
		// 不动 dst 的 WriteDeadline（避免污染下游 splice / 客户端）。
		// 不 Close 任何连接（client c 不该被 proxy 强行关闭）。
		_ = src.SetReadDeadline(time.Now())
		close(stop)
	}()
	defer func() {
		// 清掉 src 的 ReadDeadline：下一次 pipe（重连后的 c2u）会复用同一个 client c，
		// 上一次留下的 SetReadDeadline(now) 会让 Read 立刻 timeout 形成 busy-loop。
		_ = src.SetReadDeadline(time.Time{})
	}()
	for {
		select {
		case <-stop:
			return total, nil
		default:
		}
		if p.ReadTimeout > 0 {
			_ = src.SetReadDeadline(time.Now().Add(p.ReadTimeout))
		}
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if err != nil {
			if ctx.Err() != nil {
				return total, nil
			}
			if isNetTimeout(err) {
				continue
			}
			return total, err
		}
	}
}

// sleepCtx 等 d 时长，期间若 ctx 取消立即返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// isClientSide 判定错误是否来自客户端一侧（c2u 方向）。
// client EOF / client Read 超时 / client Write 失败 → 客户端主动断，
// 不应该重连 upstream。
func isClientSide(err error) bool {
	if err == nil {
		return false
	}
	// io.EOF：远端正常关闭，正常路径
	return true
}

// errStr 把 error 转成简短字符串，nil 时给 "-"。
func errStr(err error) string {
	if err == nil {
		return "-"
	}
	return err.Error()
}

func isNetTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

var connSeq uint64

func newConnID() string {
	n := atomic.AddUint64(&connSeq, 1)
	return fmt.Sprintf("%06d", n)
}