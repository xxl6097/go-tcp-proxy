package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	listen := flag.String("listen", ":8080", "本地监听地址，例如 :8080 或 127.0.0.1:8080")
	upstream := flag.String("upstream", "", "目标地址，例如 10.0.0.5:3306")
	readTimeout := flag.Duration("read-timeout", 0, "双向读空闲超时；0 表示不限制")
	logLevel := flag.String("log-level", "info", "日志级别：debug|info|warn|error")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `tcpfwd - 极简 TCP 端口转发器

Usage:
  %s --listen :8080 --upstream 10.0.0.5:3306

Flags:
`, os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	if *upstream == "" {
		flag.Usage()
		os.Exit(2)
	}

	level := parseLevel(*logLevel)
	h := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	p := Proxy{
		ListenAddr:   *listen,
		UpstreamAddr: *upstream,
		ReadTimeout:  *readTimeout,
		Log:          slog.Default(),
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := p.Run(ctx); err != nil {
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
