package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"collector/internal/aggregate"
	"collector/internal/collector"
	"collector/internal/config"
	"collector/internal/exchange"
	"collector/internal/model"

	"github.com/coder/websocket"
)

// Simulate the external supervisor with two runs using persisted checkpoints.
// All traffic stays on an in-process test server.
func TestRunDiscardsQueueOnDisconnectAndRefetchesAfterRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now().Unix()/300*300 - 600
	var connections, invalidRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") == "websocket" {
			if strings.Contains(r.URL.Query().Get("streams"), "invalid") {
				t.Error("invalid symbol subscribed")
			}
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			connections.Add(1) // Force a disconnect during catch-up.
			return
		}
		if r.URL.Query().Get("symbol") == "INVALID" {
			invalidRequests.Add(1)
			w.WriteHeader(400)
			fmt.Fprint(w, `{"code":-1121}`)
			return
		}
		from, err := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		if err != nil {
			t.Error("missing history boundary")
		}
		var times []int64
		for timestamp := from / 1000; timestamp <= start+600; timestamp += 60 {
			times = append(times, timestamp*1000)
		}
		fmt.Fprint(w, restPage(times...))
	}))
	defer server.Close()
	ex := exchange.New(exchange.Options{REST: server.URL, WebSocket: strings.Replace(server.URL, "http", "ws", 1), HTTPClient: server.Client()})
	c := config.Config{Symbols: []string{"INVALID", "TEST"}, Intervals: []int64{300}, Start: start,
		FlushEvery: time.Hour}
	store := &restartStore{}
	for attempt := 1; attempt <= 2; attempt++ {
		err := run(ctx, c, ex, store, nil)
		if err == nil || ctx.Err() != nil {
			t.Fatal("disconnect did not return an error promptly")
		}
		if connections.Load() != int64(attempt) || invalidRequests.Load() != int64(attempt) {
			t.Fatal("internal reconnect or invalid-symbol handling changed")
		}
		if len(store.batches) != 0 {
			t.Fatal("disconnect unexpectedly drained queued rows")
		}

	}

}

// This test store supports one interval and retains only successfully appended rows.
type restartStore struct{ memoryStore }

func (s *restartStore) Checkpoints(context.Context) (model.Checkpoints, error) {
	rows := make(map[string]int64)
	for _, batch := range s.batches {
		for _, row := range batch {
			if previous, ok := rows[row.Symbol]; !ok || row.Time > previous {
				rows[row.Symbol] = row.Time
			}
		}
	}
	return map[int64]map[string]int64{300: rows}, nil
}

func TestSessionBuffersEventsDuringPerSymbolHandoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	restStarted, releaseREST := make(chan struct{}), make(chan struct{})
	var connected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") == "websocket" {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			connected.Store(true)
			send := func(symbol string, timestamp int64) {
				body := strings.Replace(streamFixture, `"TEST"`, `"`+symbol+`"`, 1)
				body = strings.Replace(body, `"t":0`, fmt.Sprintf(`"t":%d`, timestamp*1000), 1)
				if err := conn.Write(ctx, websocket.MessageText, []byte(body)); err != nil {
					t.Error("writing synthetic event failed")
				}
			}
			send("FIRST", 240)
			select {
			case <-restStarted:
			case <-ctx.Done():
				return
			}
			send("SECOND", 240)
			send("FIRST", 540)
			send("SECOND", 540)
			close(releaseREST)
			_, _, _ = conn.Read(ctx)
			return
		}
		if !connected.Load() {
			t.Error("REST requested before stream connection")
		}
		from, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("endTime"), 10, 64)
		if r.URL.Query().Get("symbol") == "FIRST" && from == 0 {
			close(restStarted)
			select {
			case <-releaseREST:
			case <-ctx.Done():
				return
			}
		}
		var times []int64
		for timestamp := from; timestamp <= end; timestamp += 60000 {
			times = append(times, timestamp)
		}
		fmt.Fprint(w, restPage(times...))
	}))
	defer server.Close()
	out := make(chan model.Output, 8)
	ex := exchange.New(exchange.Options{REST: server.URL, WebSocket: strings.Replace(server.URL, "http", "ws", 1), HTTPClient: server.Client()})
	symbols := []string{"FIRST", "SECOND"}
	source := sessionSource{ex}
	done := make(chan error, 1)
	go func() { done <- collector.Run(ctx, symbols, source, aggregate.New([]int64{300}, nil, 0), out) }()
	seen := make(map[string][]int64)
	for range 4 {
		select {
		case row := <-out:
			if row.Row.Volume != 25 {
				t.Error("handoff or queued live event lost a minute")
			}
			seen[row.Row.Symbol] = append(seen[row.Row.Symbol], row.Row.Time)
		case err := <-done:
			t.Fatalf("session failed: %v", err)
		case <-ctx.Done():
			t.Fatal("session stalled")
		}
	}
	cancel()
	<-done
	for _, symbol := range symbols {
		if !reflect.DeepEqual(seen[symbol], []int64{0, 300}) {
			t.Fatal("per-symbol ordering or handoff failed")
		}
	}
}

func TestSessionPreservesReaderFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"e":"error"}`))
		_, _, _ = conn.Read(ctx)
	}))
	defer server.Close()
	ex := exchange.New(exchange.Options{WebSocket: strings.Replace(server.URL, "http", "ws", 1)})
	if err := collector.Run(ctx, []string{"TEST"}, sessionSource{ex}, testEngine(), make(chan model.Output)); err == nil || err.Error() != "exchange stream error" {
		t.Fatalf("reader failure was hidden: %v", err)
	}
}
