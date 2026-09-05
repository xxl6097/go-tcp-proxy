package proxy_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/uuxia/tcpfwd/proxy"
)

// ExampleProxy_basic 演示最简用法：注入 Listener、转发到远端、收完数据
// 主动退出 Run。
func ExampleProxy_basic() {
	upstream, stop := startFakeUpstream()
	defer stop()

	ln := mustListen("127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := &proxy.Proxy{
		UpstreamAddr: upstream,
	}
	p.UseListener(ln)

	// 用 done channel 等 Run goroutine 退出，避免 race detector 报 leaked goroutine。
	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()

	// 等 Run 把端口挂上
	waitListen(p.Addr())

	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Println("dial fail:", err)
		return
	}
	defer c.Close()
	c.Write([]byte("ping\n"))
	buf := make([]byte, 16)
	c.SetReadDeadline(time.Now().Add(time.Second))
	n, _ := c.Read(buf)
	fmt.Printf("echo=%q", buf[:n])

	cancel()
	<-done
	// Output: echo="ping\n"
}

// ExampleProxy_withLogger 演示如何注入自定义 logger，并复用已有 Listener。
func ExampleProxy_withLogger() {
	log := slog.New(slog.NewTextHandler(io.Discard, nil)) // 静默日志
	p := &proxy.Proxy{
		UpstreamAddr: "upstream.internal:6379",
		Log:          log,
	}

	// 复用已有 socket（如被 systemd / k8s 接管）
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Println("listen fail:", err)
		return
	}
	p.UseListener(ln)

	// 主程序：用 ctx 统一控制 Run 的生命周期
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx)
		close(done)
	}()
	<-done // ctx 超时后 Run 优雅退出

	fmt.Println("proxy exited cleanly")
	// Output: proxy exited cleanly
}

// ExampleProxy_shutdown 演示同一 ctx 下挂多个 Proxy（端口复用、灰度切换场景）。
func ExampleProxy_shutdown() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	var wg sync.WaitGroup
	for _, upstream := range []string{"primary:80", "fallback:80"} {
		upstream := upstream
		wg.Add(1)
		go func() {
			defer wg.Done()
			p := &proxy.Proxy{
				ListenAddr:   "127.0.0.1:0",
				UpstreamAddr: upstream,
				Log:          log,
			}
			_ = p.Run(ctx)
		}()
	}

	// 让两个 Proxy 都跑一会儿，然后一次性关闭
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()

	fmt.Println("all proxies stopped")
	// Output: all proxies stopped
}

// ExampleProxy_withRetry 演示上游抖动场景：先不开 accept → proxy dial 失败
// 重试；300ms 后开 accept → 重连成功，客户端拿到 echo。
func ExampleProxy_withRetry() {
	addr, startAccept, stopUp := startControlledEcho(nil)
	defer stopUp()

	ln := mustListen("127.0.0.1:0")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	p := &proxy.Proxy{
		UpstreamAddr: addr,
		Log:          log,
	}
	p.UseListener(ln)

	done := make(chan struct{})
	go func() {
		_ = p.Run(ctx,
			proxy.WithUpstreamRetry(0, 100*time.Millisecond), // 无限重试，100ms 间隔
		)
		close(done)
	}()

	c, err := net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Println("dial fail:", err)
		return
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))

	// 此刻 upstream 不接受 → 客户端写完后等会得到错误（连接被关）
	if _, err := c.Write([]byte("first\n")); err != nil {
		fmt.Println("first write:", err)
		return
	}
	buf := make([]byte, 16)
	if _, err := c.Read(buf); err == nil {
		fmt.Println("expected first read to fail")
		return
	}
	c.Close()

	// 让 upstream 开始接受，新建连接 → 这次能正常 echo
	startAccept()
	c2, err := net.Dial("tcp", p.Addr())
	if err != nil {
		fmt.Println("dial 2:", err)
		return
	}
	defer c2.Close()
	c2.SetDeadline(time.Now().Add(2 * time.Second))

	msg := []byte("after-up\n")
	if _, err := c2.Write(msg); err != nil {
		fmt.Println("write:", err)
		return
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c2, got); err != nil {
		fmt.Println("read:", err)
		return
	}
	fmt.Printf("recovered=%q", got)

	cancel()
	<-done
	// Output: recovered="after-up\n"
}

// ---- helpers ----

func mustListen(addr string) net.Listener {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	return ln
}

// startFakeUpstream 返回一个测试用 echo 服务器的监听地址，外加 stop 函数。
// echo 行为：原样回写。
func startFakeUpstream() (string, func()) {
	ln := mustListen("127.0.0.1:0")
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
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	stop := func() {
		ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), stop
}

// waitListen 等待监听端口生效，最多 1s。
func waitListen(addr string) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
