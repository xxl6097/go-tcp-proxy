// tcpfwd 是一个极简 TCP 端口转发命令行工具，基于 github.com/uuxia/tcpfwd/proxy 库。
//
// 自身只是 CLI 壳子：解析 flag、装配 slog、把信号转 ctx、调用 proxy.Proxy.Run。
// 真正可复用的能力在根目录的 proxy 包。
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/xxl6097/go-tcp-proxy/proxy"
)

func main() {
	listen := flag.String("listen", ":8080", "本地监听地址，例如 :8080 或 127.0.0.1:8080")
	upstream := flag.String("upstream", "", "目标地址，例如 10.0.0.5:3306")
	readTimeout := flag.Duration("read-timeout", 0, "双向读空闲超时；0 表示不限制")
	dialTimeout := flag.Duration("dial-timeout", 5*time.Second, "upstream 建连超时；默认 5s")
	logLevel := flag.String("log-level", "info", "日志级别：debug|info|warn|error")
	retryAttempts := flag.String("retry-attempts", "0", "upstream 拨号+重连总尝试上限；0 或负数 = 无限（仅 ctx 取消才停）")
	retryInterval := flag.Duration("retry-interval", 0, "两次尝试间隔；>0 启用重试，<=0 不启用")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `tcpfwd - 极简 TCP 端口转发器

Usage:
  %s --listen :8080 --upstream 10.0.0.5:3306
  %s --listen :8080 --upstream 10.0.0.5:3306 --retry-interval 2s --retry-attempts 5

Flags:
`, os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *upstream == "" {
		flag.Usage()
		os.Exit(2)
	}

	attempts, err := strconv.Atoi(*retryAttempts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --retry-attempts %q: %v\n", *retryAttempts, err)
		os.Exit(2)
	}

	level := parseLevel(*logLevel)
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	p := &proxy.Proxy{
		ListenAddr:    *listen,
		UpstreamAddr:  *upstream,
		ReadTimeout:   *readTimeout,
		DialTimeout:   *dialTimeout,
		MaxAttempts:   attempts,
		RetryInterval: *retryInterval,
		Log:           slog.Default(),
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var opts []proxy.Option
	if *retryInterval > 0 {
		opts = append(opts, proxy.WithUpstreamRetry(attempts, *retryInterval))
	}
	if err := p.Run(ctx, opts...); err != nil {
		slog.Error("proxy_exit", "err", err.Error())
		os.Exit(1)
	}
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	return slog.LevelInfo
}
