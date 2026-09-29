package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"collector/internal/exchange"
	"collector/internal/model"
	"collector/internal/watchdog"
	"collector/internal/writer"
)

func TestOnlySuccessfulUploadPings(t *testing.T) {
	for _, mode := range []string{"empty", "failed", "saved"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := watchdog.New(20*time.Minute, func() { t.Error("unexpected exit") })
				defer w.Close()
				ctx := context.Background()
				pings := 0
				queue := make(chan model.Output, 1)
				if mode != "empty" {
					queue <- model.Output{Interval: 300}
				}
				time.Sleep(time.Second)
				err := flushBatch(ctx, (&memoryStore{fail: mode == "failed"}).Append, queue, func() { pings++; w.Ping() })
				if (err != nil) != (mode == "failed") {
					t.Fatal("unexpected save result")
				}
				if (pings > 0) != (mode == "saved") {
					t.Fatal("only a successful nonempty upload may ping")
				}
			})
		})
	}
}

func TestEmptyQueueCannotKeepWatchdogAlive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fired := make(chan struct{}, 1)
		w := watchdog.New(20*time.Minute, func() { fired <- struct{}{} })
		defer w.Close()
		w.SetTimeout(3 * time.Second)
		w.Ping()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = writer.Run(ctx, (&memoryStore{}).Append, make(chan model.Output), time.Second, w.Ping)
		}()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if len(fired) != 1 {
			t.Fatal("empty flushes concealed missing data")
		}
		cancel()
		<-done
	})
}

type stuckWriter struct{}

func (stuckWriter) Write([]byte) (int, error) { select {} }

type stuckBody struct{ closeOnly bool }

func (b stuckBody) Read([]byte) (int, error) {
	if b.closeOnly {
		return 0, io.EOF
	}
	select {}
}
func (b stuckBody) Close() error {
	if b.closeOnly {
		select {}
	}
	return nil
}

type stuckStore struct{}

func (*stuckStore) Append(context.Context, int64, []model.Row) error { select {} }

// Each child uses virtual time, but calls the real os.Exit. Neither a custom
// transport, a store nor an model.Output writer below cooperates with cancellation.
func TestWatchdogHardExit(t *testing.T) {
	if mode := os.Getenv("COLLECTOR_TEST_HANG"); mode != "" {
		synctest.Test(t, func(t *testing.T) {
			w := watchdog.New(3*time.Second, hardExit)
			defer w.Close()
			ctx := context.Background()
			switch mode {
			case "bare":
				// No timeout, operation wrapper, context check or cancellation.
				select {}
			case "retry":
				ex := exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"999999999"}}, Body: io.NopCloser(strings.NewReader("{}"))}, nil
				})}})
				_ = ex.History(ctx, "TEST", 0, -1, func(model.Range) error { return nil })
			case "transport":
				ex := exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { select {} })}})
				_ = ex.History(ctx, "TEST", 0, -1, func(model.Range) error { return nil })
			case "body", "close":
				ex := exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Body: stuckBody{closeOnly: mode == "close"}}, nil
				})}})
				_ = ex.History(ctx, "TEST", 0, -1, func(model.Range) error { return nil })
			case "save":
				queue := make(chan model.Output, 1)
				queue <- model.Output{Interval: 300}
				_ = flushBatch(ctx, (&stuckStore{}).Append, queue, w.Ping)
			case "log":
				logger := slog.New(slog.NewTextHandler(stuckWriter{}, nil))
				logger.Info("synthetic event")
			}
			t.Fatal("hung operation returned")
		})
		return
	}
	for _, mode := range []string{"bare", "retry", "transport", "body", "close", "save", "log"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWatchdogHardExit$")
			cmd.Env = append(os.Environ(), "COLLECTOR_TEST_HANG="+mode)
			cmd.Stdout = io.Discard
			cmd.Stderr = io.Discard
			err := cmd.Run()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 2 {
				t.Fatal("watchdog failed to terminate hung child")
			}
		})
	}
}
