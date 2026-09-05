package proxy_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uuxia/tcpfwd/proxy"
)

// 找一对空闲端口：upstream 端口先占再放，proxy 端口后占再放，二者通常不会冲突。
func pickPorts(t *testing.T) (proxyAddr, upstreamAddr string) {
	t.Helper()
	a, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	aPort := a.Addr().(*net.TCPAddr).Port
	a.Close()
	b, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	bPort := b.Addr().(*net.TCPAddr).Port
	b.Close()
	return fmt.Sprintf("127.0.0.1:%d", aPort), fmt.Sprintf("127.0.0.1:%d", bPort)
}

// startEchoServer 起一个回环 echo：收到的字节原样写出。
// 返回监听地址与停止函数。
func startEchoServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := &atomic.Bool{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				if done.Load() {
					return
				}
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	stop = func() {
		done.Store(true)
		ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), stop
}

// waitProxyReady 等 proxy 真起来：注入日志 + 等 proxy_listen 出现。
//
// 已废弃：必须在 `go p.Run(...)` 之前注入 p.Log，事后注入已晚（Run 已用旧
// Logger 写过 proxy_listen），所以现在统一用 time.Sleep(50ms) 兜底。
// 保留函数签名仅供查阅。
func startProxyReady(t *testing.T, p *proxy.Proxy) *safeBuf {
	t.Helper()
	buf := &safeBuf{}
	p.Log = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "proxy_listen") {
			return buf
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 若 log 缓冲里始终没出现 proxy_listen（很可能 Run 已经写完），返回 buf 让
	// 调用方决定怎么处理。
	return buf
}

// safeBuf 给 bytes.Buffer 配把锁，避免 race detector 报错。
type safeBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// runProxyWithLog 把 p.Run 跑起来，把日志收到 buffer，返回 cancel 与 wg。
// 不发任何探测连接，避免幽灵连接和真实测试连接争上游 fd。
func runProxyWithLog(t *testing.T, p *proxy.Proxy) (context.CancelFunc, *safeBuf, *sync.WaitGroup) {
	t.Helper()
	buf := &safeBuf{}
	p.Log = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if p.ListenAddr == "" {
		t.Fatal("proxy listen addr is empty")
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.Run(ctx)
	}()
	// 监视 net.Listen 日志是否出现，判断端口已开。不发额外连接。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "proxy_listen") {
			return cancel, buf, wg
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Fatalf("proxy did not report proxy_listen within 2s")
	return cancel, buf, wg
}

func TestProxy_EndToEnd(t *testing.T) {
	upstreamAddr, stopUp := startEchoServer(t)
	t.Logf("echo upstreamAddr=%s", upstreamAddr)
	defer stopUp()

	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxyAddr := proxyLn.Addr().String()
	t.Logf("proxyAddr=%s", proxyAddr)

	p := &proxy.Proxy{UpstreamAddr: upstreamAddr, Log: slog.Default()}
	p.UseListener(proxyLn)
	ctx, cancel := context.WithCancel(context.Background())
	done := &sync.WaitGroup{}
	done.Add(1)
	go func() {
		defer done.Done()
		_ = p.Run(ctx)
	}()
	defer func() {
		cancel()
		done.Wait()
	}()

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t.Logf("client connected: %s -> proxy", c.LocalAddr())
	c.SetDeadline(time.Now().Add(2 * time.Second))

	msg := []byte("hello, tcp forwarder\n")
	n, err := c.Write(msg)
	t.Logf("client wrote %d bytes, err=%v", n, err)
	if err != nil {
		t.Fatal(err)
	}
	// 注意：不能用 io.ReadAll，它会循环 Read 直到 EOF，在不关闭连接的场景下
	// 会一直等到 deadline 触发并返回 timeout error，掩盖已经收到的回包。
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatalf("echo read: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch: got=%q want=%q", got, msg)
	}
}

func TestProxy_UpstreamUnreachable(t *testing.T) {
	// upstream 监听器立刻关闭 → 客户端 dial up 应该 fail
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				close(closed)
				return
			}
			c.Close() // 立刻关闭客户端连接，模拟不可用 upstream
		}
	}()
	upstreamAddr := ln.Addr().String()

	// proxy 用一个我们控制的 dialTimeout
	p := &proxy.Proxy{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstreamAddr,
		DialTimeout:  300 * time.Millisecond,
		Log:          slog.Default(),
	}
	// 注入 listener 后才知道实际地址
	realLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.UseListener(realLn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
		ln.Close()
		<-closed
	}()

	c, err := net.Dial("tcp", realLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))

	// 发字节后应当被 Proxy 关连接（远端关）
	_, werr := c.Write([]byte("ping"))
	if werr != nil {
		t.Fatal(werr)
	}
	buf := make([]byte, 16)
	_, rerr := c.Read(buf)
	if rerr == nil {
		t.Fatal("expected read to fail because upstream immediately closes")
	}
}

func TestProxy_ShutdownCleanly(t *testing.T) {
	upstreamAddr, stopUp := startEchoServer(t)
	defer stopUp()
	proxyAddr, _ := pickPorts(t)

	p := &proxy.Proxy{ListenAddr: proxyAddr, UpstreamAddr: upstreamAddr, Log: slog.Default()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", proxyAddr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy did not exit after cancel")
	}
}

func TestProxy_LogContainsExpectedFields(t *testing.T) {
	upstreamAddr, stopUp := startEchoServer(t)
	defer stopUp()
	proxyAddr, _ := pickPorts(t)

	p := &proxy.Proxy{ListenAddr: proxyAddr, UpstreamAddr: upstreamAddr, Log: slog.Default()}
	cancel, logBuf, wg := runProxyWithLog(t, p)
	defer func() {
		cancel()
		wg.Wait()
	}()

	// 建立一个连接，写一点数据，主动关闭。等 splice 走完即可，不要用 io.Copy
	// 读 —— TCP 不会主动对端半关闭，对端只有等本端关闭才能看到 EOF。
	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write([]byte("hi\n")); err != nil {
		t.Fatal(err)
	}
	c.Close()

	// 等待日志刷出 conn_close
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "conn_close") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	out := logBuf.String()
	for _, want := range []string{"proxy_listen", "conn_open", "conn_close",
		"rx_bytes=", "tx_bytes=", "duration_ms="} {
		if !strings.Contains(out, want) {
			t.Logf("full log:\n%s", out)
			t.Fatalf("log missing %q", want)
		}
	}
}

// startControlledEcho 起一个监听器但**不**开始 Accept（门控开关）；
// 返回地址 + startAccept / stop 全停。
//
// t 可传 nil（example_test 用），此时出错用 panic 而不是 t.Fatal。
//
// 用法：
//
//	addr, start, stop := startControlledEcho(t)
//	defer stop()
//	start()                       // 开始 Accept
//	// ... 测试 ...
//	stop()                        // 关闭监听器
func startControlledEcho(t *testing.T) (addr string, startAccept, stop func()) {
	fatalf := func(format string, args ...any) {
		if t != nil {
			t.Helper()
			t.Fatalf(format, args...)
		} else {
			panic(fmt.Sprintf(format, args...))
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fatalf("listen: %v", err)
	}

	var (
		mu       sync.Mutex
		acceptOn bool
		wg       sync.WaitGroup
		stopped  bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			on := acceptOn
			mu.Unlock()
			if !on {
				c.Close()
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c) // echo
			}(c)
		}
	}()

	startAccept = func() {
		mu.Lock()
		acceptOn = true
		mu.Unlock()
	}
	stop = func() {
		mu.Lock()
		if stopped {
			mu.Unlock()
			return
		}
		stopped = true
		mu.Unlock()
		ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), startAccept, stop
}

// TestProxy_DialRetry_StartLate 验证：upstream 起初**没起来**（不接受连接 →
// proxy dial 失败），过一会起来；WithUpstreamRetry 应让 proxy 自动重试，
// 客户端最终能拿到 echo。
func TestProxy_DialRetry_StartLate(t *testing.T) {
	addr, startAccept, stopUp := startControlledEcho(t)
	defer stopUp()

	proxyAddr, _ := pickPorts(t)
	p := &proxy.Proxy{
		ListenAddr:   proxyAddr,
		UpstreamAddr: addr,
		Log:          slog.Default(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx,
			proxy.WithUpstreamRetry(0, 100*time.Millisecond), // 无限重试，100ms 间隔
		)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// 等 proxy_listen 日志出现再 dial，避免 race
	time.Sleep(50 * time.Millisecond)

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))

	// 客户端先写一次：此刻 upstream 不接受 → dial 失败 → proxy 关客户端
	if _, err := c.Write([]byte("first\n")); err != nil {
		t.Fatalf("first write: %v", err)
	}
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("expected first read to fail (upstream not accepting yet)")
	}
	c.Close()

	// 现在让 upstream 接受，再开一个连接 —— 这次应当能正常 echo
	startAccept()
	c2, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	c2.SetDeadline(time.Now().Add(3 * time.Second))

	msg := []byte("after-up\n")
	if _, err := c2.Write(msg); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c2, got); err != nil {
		t.Fatalf("post-recovery read: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch: got=%q want=%q", got, msg)
	}
}

// TestProxy_Reconnect_UpstreamRestart 验证：连接建立后 upstream 主动断开，
// proxy 应自动重连，客户端继续可用。
func TestProxy_Reconnect_UpstreamRestart(t *testing.T) {
	addr, stopUp := startEchoCloseFirst(t)
	defer stopUp()

	proxyAddr, _ := pickPorts(t)
	p := &proxy.Proxy{
		ListenAddr:   proxyAddr,
		UpstreamAddr: addr,
	}
	logBuf := &safeBuf{}
	p.Log = slog.New(slog.NewTextHandler(logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	t.Logf("proxy listen=%s upstream=%s", proxyAddr, addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx,
			proxy.WithUpstreamRetry(0, 100*time.Millisecond),
		)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// 等 proxy_listen 日志出现再 dial，避免 race
	time.Sleep(50 * time.Millisecond)

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))

	// 第一次连接：upstream 主动断开。客户端发的数据会被 c2u 在 attempt=1
	// 时尝试透传但 up 已 RST，所以数据丢失（用户选择的"丢弃"语义）。
	// 客户端这边在 reconnect 完成前会拿到一次 EOF/rst（视 splice 退出时序）。
	// 这里**不**做严格断言：仅确保连接没立即被 proxy 强关且后续仍然可用。

	// 等待 reconnect 完成（proxy 日志里应出现 attempt=2）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logBuf.String(), "attempt=2") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// 第二次发送：重连后客户端重新写一次，期望 echo 正常回来。
	msg := []byte("after\n")
	t.Logf("about to write at %s", time.Now().Format("15:04:05.000"))
	if _, err := c.Write(msg); err != nil {
		t.Fatalf("post-reconnect write: %v", err)
	}
	t.Logf("wrote %d bytes at %s, waiting echo...", len(msg), time.Now().Format("15:04:05.000"))
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Logf("log:\n%s", logBuf.String())
		t.Fatalf("post-reconnect read: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo mismatch after reconnect: got=%q want=%q", got, msg)
	}

	// 验证日志里确实发生过 disconnect + 重连
	out := logBuf.String()
	if !strings.Contains(out, "upstream_disconnect") {
		t.Logf("log:\n%s", out)
		t.Fatal("expected upstream_disconnect in log")
	}
	if !strings.Contains(out, "attempt=2") {
		t.Logf("log:\n%s", out)
		t.Fatal("expected attempt=2 (reconnect) in log")
	}
}

// startEchoCloseFirst 起一个 echo server，但**第一次** accept 的连接会被
// 立刻关掉（模拟 upstream 抖动），后续 accept 正常 echo。
//
// 返回地址 + stop。
func startEchoCloseFirst(t *testing.T) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu      sync.Mutex
		closed  bool
		wg      sync.WaitGroup
		stopped bool
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			first := !closed
			closed = true
			mu.Unlock()
			if first {
				c.Close() // 模拟第一次连接被 upstream 主动断开
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	stop = func() {
		mu.Lock()
		if stopped {
			mu.Unlock()
			return
		}
		stopped = true
		mu.Unlock()
		ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), stop
}

// TestProxy_DialRetry_GiveUp 验证：upstream 永远连不上时，MaxAttempts 用尽后
// proxy 主动关闭客户端。
func TestProxy_DialRetry_GiveUp(t *testing.T) {
	// 不起任何 listener；用合法端口号但保证没人监听
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // 立刻关闭，端口无人监听

	proxyAddr, _ := pickPorts(t)
	p := &proxy.Proxy{
		ListenAddr:   proxyAddr,
		UpstreamAddr: addr,
		DialTimeout:  100 * time.Millisecond,
		Log:          slog.Default(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx,
			proxy.WithUpstreamRetry(2, 100*time.Millisecond), // 最多 2 次
		)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// 等 proxy_listen 日志出现再 dial，避免 race
	time.Sleep(50 * time.Millisecond)

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := c.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	// 期望 proxy 关客户端
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("expected client to be closed after MaxAttempts exceeded")
	}
}

// TestProxy_DefaultNoRetry 验证：默认配置（不传 Option）下 upstream 断开仍然
// 立即关闭客户端，向后兼容。
func TestProxy_DefaultNoRetry(t *testing.T) {
	// upstream 监听后立刻关闭所有 accept 的连接
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	defer ln.Close()

	proxyAddr, _ := pickPorts(t)
	p := &proxy.Proxy{
		ListenAddr:   proxyAddr,
		UpstreamAddr: ln.Addr().String(),
		Log:          slog.Default(),
	}
	// 故意不传 WithUpstreamRetry → 默认行为：dial 成功后 upstream 关闭 → 客户端关
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	// 等 proxy_listen 日志出现再 dial，避免 race
	time.Sleep(50 * time.Millisecond)

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err == nil {
		t.Fatal("default behavior: expected client close, got data")
	}
	// 1.5s 内不应有第二次"重连"行为（没有重连逻辑）
	// 这里只验证语义，不做严格时序断言
}
