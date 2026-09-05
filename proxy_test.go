package main

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

func runProxyWithLog(t *testing.T, p *Proxy) (context.CancelFunc, *bytes.Buffer, *sync.WaitGroup) {
	t.Helper()
	buf := &bytes.Buffer{}
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
	// 等监听端口生效
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", p.ListenAddr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			return cancel, buf, wg
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	t.Fatalf("proxy did not start listening on %s", p.ListenAddr)
	return cancel, buf, wg
}

func TestProxy_EndToEnd(t *testing.T) {
	upstreamAddr, stopUp := startEchoServer(t)
	t.Logf("echo upstreamAddr=%s", upstreamAddr)
	defer stopUp()
	proxyAddr, _ := pickPorts(t)
	t.Logf("proxyAddr=%s", proxyAddr)

	p := &Proxy{ListenAddr: proxyAddr, UpstreamAddr: upstreamAddr, Log: slog.Default()}
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

	// 等待 proxy 监听
	for i := 0; i < 50; i++ {
		c, err := net.DialTimeout("tcp", proxyAddr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// 等前一个探测连接彻底被 handle 收尾，避免 backlog 残留
	time.Sleep(50 * time.Millisecond)

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
	got, err := io.ReadAll(c)
	if err != nil {
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
	p := &Proxy{
		ListenAddr:   "127.0.0.1:0",
		UpstreamAddr: upstreamAddr,
		dialTimeout:  300 * time.Millisecond,
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

	p := &Proxy{ListenAddr: proxyAddr, UpstreamAddr: upstreamAddr, Log: slog.Default()}
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

	p := &Proxy{ListenAddr: proxyAddr, UpstreamAddr: upstreamAddr, Log: slog.Default()}
	cancel, logBuf, wg := runProxyWithLog(t, p)
	defer func() {
		cancel()
		wg.Wait()
	}()

	c, err := net.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("hi"))
	io.Copy(io.Discard, c)
	c.Close()

	// 等待日志刷出
	time.Sleep(500 * time.Millisecond)
	out := logBuf.String()
	for _, want := range []string{"proxy_listen", "conn_open", "conn_close",
		"rx_bytes=", "tx_bytes=", "duration_ms="} {
		if !strings.Contains(out, want) {
			t.Logf("full log:\n%s", out)
			t.Fatalf("log missing %q", want)
		}
	}
}
