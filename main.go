package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"collector/internal/aggregate"
	"collector/internal/collector"
	"collector/internal/config"
	"collector/internal/exchange"
	"collector/internal/model"
	"collector/internal/warehouse"
	"collector/internal/watchdog"
	"collector/internal/writer"
)

// Approximate 256 bytes per queued output, including referenced values and headroom.
// This budgets the queue, not the in-flight batch, serialization, or client buffers.
const saveQueueCapacity = (16 << 20) / 256

type Store interface {
	Checkpoints(context.Context) (model.Checkpoints, error)
	Append(context.Context, int64, []model.Row) error
}

func execute(ctx context.Context, w *watchdog.Watchdog) error {
	offline := flag.Bool("replay", false, "aggregate closed candle JSON lines from stdin without external access")
	flag.Parse()
	if *offline {
		return replay(os.Stdin, os.Stdout)
	}
	c, err := config.Read()
	if err != nil {
		return err
	}
	w.SetTimeout(config.SaveTimeout(c.Intervals))
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: c.LogLevel})))
	initCtx, done := context.WithTimeout(ctx, 5*time.Minute)
	store, err := warehouse.New(initCtx, warehouse.Options{Project: c.Project, Dataset: c.Dataset, Symbols: c.Symbols, Tables: c.Tables, Intervals: c.Intervals})
	done()
	if err != nil {
		return err
	}
	ex := exchange.New(exchange.Options{REST: c.REST, WebSocket: c.WebSocket, Pace: 250 * time.Millisecond})
	return run(ctx, c, ex, store, w.Ping)
}

func main() {
	w := watchdog.New(config.SaveTimeout(nil), hardExit)
	defer w.Close()
	// Leave SIGINT/SIGTERM to the OS: no drain or client cleanup on exit.
	err := execute(context.Background(), w)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c config.Config, ex collector.Source, store Store, onSaved func()) error {
	startup, cancelStartup := context.WithTimeout(ctx, 5*time.Minute)
	checkpoints, err := store.Checkpoints(startup)
	cancelStartup()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	queue := make(chan model.Output, saveQueueCapacity)
	go func() { cancel(writer.Run(ctx, store.Append, queue, c.FlushEvery, onSaved)) }()
	go func() {
		cancel(collector.Run(ctx, c.Symbols, ex, aggregate.New(c.Intervals, checkpoints, c.Start), queue))
	}()
	<-ctx.Done()
	return context.Cause(ctx)
}

func hardExit() { os.Exit(2) }
