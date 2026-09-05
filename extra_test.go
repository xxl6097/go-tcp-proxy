package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func launchEchoServer(t *testing.T, addr string) (string, func()) {
	ln, err := net.Listen("tcp", addr)
	if err != nil { t.Fatal(err) }
	done := &atomic.Bool{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil { return }
			go func(c net.Conn) {
				defer c.Close()
				if done.Load() { return }
				_, _ = io.Copy(c, c)
			}(c)
		}
	}()
	return ln.Addr().String(), func() {
		done.Store(true)
		ln.Close()
		wg.Wait()
	}
}

func TestDirectForward(t *testing.T) {
	echoAddr, stopEcho := launchEchoServer(t, "127.0.0.1:0")
	defer stopEcho()
	
	proxyLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil { t.Fatal(err) }
	proxyAddr := proxyLn.Addr().String()
	
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	p := &Proxy{UpstreamAddr: echoAddr, Log: log}
	p.UseListener(proxyLn)
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	defer cancel()
	
	c, err := net.Dial("tcp", proxyAddr)
	if err != nil { t.Fatal(err) }
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	
	c.Write([]byte("hello\n"))
	
	buf := make([]byte, 100)
	for {
		c.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, err := c.Read(buf)
		if err != nil { fmt.Printf("err %v bytes=%d\n", err, n); return }
		fmt.Printf("got %d bytes: %q\n", n, buf[:n])
		if n >= 6 { return }
	}
}
