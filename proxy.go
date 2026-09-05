package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Proxy 把 ListenAddr 上的入站连接双向转发到 UpstreamAddr。
//
// 设计要点：
//   - 一条连接两个 goroutine：client -> upstream、upstream -> client。
//   - 任一方向结束即立刻关闭另一端，避免死锁（不保活、不重连）。
//   - ReadTimeout>0 时给每个方向单独设置读空闲超时；0 表示不限制。
type Proxy struct {
	ListenAddr   string        // 本地监听地址，例如 ":8080"
	UpstreamAddr string        // 目标地址，例如 "10.0.0.5:3306"
	ReadTimeout  time.Duration // 单方向读空闲超时；0 表示关闭
	Log          *slog.Logger  // 外部注入；nil 时回退到 slog.Default()

	listener    net.Listener  // 测试用：可通过 UseListener 注入
	dialTimeout time.Duration // 测试用：默认 5s
}

// UseListener 注入一个已就绪的监听器（主要用于测试）。
func (p *Proxy) UseListener(ln net.Listener) { p.listener = ln }

// Run 阻塞运行，直到 ctx 取消或监听器失败。
func (p *Proxy) Run(ctx context.Context) error {
	if p.Log == nil {
		p.Log = slog.Default()
	}
	if p.dialTimeout == 0 {
		p.dialTimeout = 5 * time.Second
	}
	ln := p.listener
	if ln == nil {
		l, err := net.Listen("tcp", p.ListenAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", p.ListenAddr, err)
		}
		ln = l
	} else {
		// 注入模式下以实际地址回填 ListenAddr，便于日志可读
		p.ListenAddr = ln.Addr().String()
	}
	defer ln.Close()

	p.Log.Info("proxy_listen",
		"listen", p.ListenAddr,
		"upstream", p.UpstreamAddr,
		"read_timeout", p.ReadTimeout.String(),
	)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			p.Log.Warn("accept_error", "err", err.Error())
			return err
		}
		go p.handle(c)
	}
}

func (p *Proxy) handle(c net.Conn) {
	cid := newConnID()
	start := time.Now()
	client := c.RemoteAddr().String()
	p.Log.Info("conn_open",
		"conn_id", cid,
		"client", client,
		"upstream", p.UpstreamAddr,
	)

	up, err := net.DialTimeout("tcp", p.UpstreamAddr, p.dialTimeout)
	if err != nil {
		p.Log.Warn("upstream_dial_fail",
			"conn_id", cid,
			"client", client,
			"upstream", p.UpstreamAddr,
			"err", err.Error(),
		)
		c.Close()
		return
	}

	// 任一方向结束立即取消 connCtx，强制关闭 c 与 up，避免对端 goroutine 死等。
	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	var rx, tx int64
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		n := splice(connCtx, c, up, p.ReadTimeout, cid, "c2u", p.Log)
		atomic.AddInt64(&rx, n)
		connCancel() // 本侧一旦结束，强行中断另一侧
	}()
	go func() {
		defer wg.Done()
		n := splice(connCtx, up, c, p.ReadTimeout, cid, "u2c", p.Log)
		atomic.AddInt64(&tx, n)
		connCancel()
	}()
	wg.Wait()
	c.Close()
	up.Close()

	p.Log.Info("conn_close",
		"conn_id", cid,
		"client", client,
		"upstream", p.UpstreamAddr,
		"rx_bytes", atomic.LoadInt64(&rx),
		"tx_bytes", atomic.LoadInt64(&tx),
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

// splice 把 src 的字节拷到 dst，返回单向写入字节数。
// timeout>0 时若单次读超过该时长视为连接空闲，调整读截止时间继续等待。
// ctx 取消时阻塞中的 Read 会立即返回。
func splice(ctx context.Context, dst, src net.Conn, timeout time.Duration, cid, dir string, log *slog.Logger) int64 {
	if timeout <= 0 {
		// 阻塞模式：用自有缓冲做双向手写循环，避免 io.Copy 走 sendfile / ReadFrom，
		// 在某些平台上 sendfile 路径可能延迟 flush 到测试 socket。
		var total int64
		buf := make([]byte, 16*1024)
		stop := make(chan struct{})
		go func() {
			<-ctx.Done()
			_ = src.Close()
			_ = dst.Close()
			close(stop)
		}()
		for {
			select {
			case <-stop:
				return total
			default:
			}
			src.SetReadDeadline(time.Time{})
			n, err := src.Read(buf)
			if n > 0 {
				if _, werr := dst.Write(buf[:n]); werr != nil {
					return total
				}
				total += int64(n)
			}
			if err != nil {
				if ctx.Err() != nil {
					return total
				}
				if isNetTimeout(err) {
					continue
				}
				if errors.Is(err, io.EOF) {
					log.Debug("splice_eof", "conn_id", cid, "dir", dir, "bytes", total)
				} else {
					log.Debug("splice_err", "conn_id", cid, "dir", dir, "bytes", total, "err", err.Error())
				}
				return total
			}
		}
	}
	buf := make([]byte, 32*1024)
	var total int64
	for {
		select {
		case <-ctx.Done():
			return total
		default:
		}
		_ = src.SetReadDeadline(time.Now().Add(timeout))
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return total
			}
			total += int64(n)
		}
		if err != nil {
			if isNetTimeout(err) {
				continue
			}
			return total
		}
	}
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
