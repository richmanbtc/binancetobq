package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"collector/internal/aggregate"
	"collector/internal/collector"
	"collector/internal/config"
	"collector/internal/exchange"
	"collector/internal/model"
)

type memoryStore struct {
	batches [][]model.Row
	fail    bool
}

func TestSaveQueueOverflowStopsCollection(t *testing.T) {
	queue := make(chan model.Output, 1)
	source := sourceFuncs{history: func(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
		for i := 0; i < 10; i++ {
			c := sampleCandle(i)
			if err := consume(model.Range{Symbol: symbol, From: int64(i) * 60, Until: int64(i+1) * 60, Candles: []model.Candle{c}}); err != nil {
				return err
			}
		}
		return nil
	}}
	err := collector.Run(context.Background(), []string{"TEST"}, source, testEngine(), queue)
	if err == nil || err.Error() != "save queue full; restart and backfill required" {
		t.Fatal("full save queue did not stop collection")
	}
	if len(queue) != 1 || (<-queue).Row.Time != 0 {
		t.Fatal("overflow discarded an already queued row")
	}
}

func (s *memoryStore) Checkpoints(context.Context) (model.Checkpoints, error) {
	return nil, nil
}
func (s *memoryStore) Append(ctx context.Context, interval int64, rows []model.Row) error {
	if s.fail {
		return errors.New("synthetic save failure")
	}
	s.batches = append(s.batches, append([]model.Row(nil), rows...))
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestWriterFailureCancelsInFlightBackfill(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		requests := 0
		ex := exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			requests++
			if requests > 1 {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			body := restPage(0, 60000, 120000, 180000, 240000, 300000)
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}})
		c := config.Config{Symbols: []string{"TEST"}, Intervals: []int64{300}, FlushEvery: 5 * time.Second}
		start := time.Now()
		err := run(context.Background(), c, ex, &memoryStore{fail: true}, nil)
		if err == nil || err.Error() != "synthetic save failure" || requests != 2 || time.Since(start) != c.FlushEvery {
			t.Fatalf("writer failure did not stop backfill: %v", err)
		}
	})
}

// A failed hourly load must not replay committed five-minute rows on restart.
type partialStore struct {
	saved      map[int64]map[int64]model.Row
	failHourly bool
}

func (s *partialStore) Checkpoints(context.Context) (model.Checkpoints, error) {
	result := make(map[int64]map[string]int64)
	for interval, rows := range s.saved {
		last := int64(-1)
		for timestamp := range rows {
			last = max(last, timestamp)
		}
		result[interval] = map[string]int64{"TEST": last}
	}
	return result, nil
}
func (s *partialStore) Append(_ context.Context, interval int64, rows []model.Row) error {
	if interval == 3600 && s.failHourly {
		return errors.New("synthetic hourly failure")
	}
	if s.saved[interval] == nil {
		s.saved[interval] = make(map[int64]model.Row)
	}
	for _, row := range rows {
		if _, exists := s.saved[interval][row.Time]; exists {
			return errors.New("duplicate committed row")
		}
		s.saved[interval][row.Time] = row
	}
	return nil
}

func TestRestartAfterOnlyOneIntervalWasSaved(t *testing.T) {
	ex := exchange.New(exchange.Options{REST: "http://example.invalid", WebSocket: ":", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		var times []int64
		for timestamp := start; timestamp <= 720*60000 && len(times) < 499; timestamp += 60000 {
			times = append(times, timestamp)
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(restPage(times...))), Header: make(http.Header)}, nil
	})}})
	c := config.Config{Symbols: []string{"TEST"}, Intervals: []int64{300, 3600}, FlushEvery: time.Hour}
	store := &partialStore{saved: make(map[int64]map[int64]model.Row), failHourly: true}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := recoverAndSave(ctx, c, ex, store); err == nil || err.Error() != "synthetic hourly failure" {
		t.Fatalf("unexpected first-run result: %v", err)
	}
	if len(store.saved[300]) != 144 || len(store.saved[3600]) != 0 {
		t.Fatal("partial commit was not reproduced")
	}
	store.failHourly = false
	for range 2 {
		// Recover from persisted checkpoints, then perform the next periodic batch.
		if err := recoverAndSave(ctx, c, ex, store); err != nil {
			t.Fatalf("restart failed: %v", err)
		}
		if len(store.saved[300]) != 144 || len(store.saved[3600]) != 12 {
			t.Fatal("restart lost or duplicated completed buckets")
		}
		for interval, rows := range store.saved {
			for timestamp := int64(0); timestamp < 720*60; timestamp += interval {
				row, ok := rows[timestamp]
				if !ok || row.Volume != float64(interval/60)*5 {
					t.Fatal("restarted aggregate is incomplete")
				}
			}
		}
	}
}

func recoverAndSave(ctx context.Context, c config.Config, ex *exchange.Client, store Store) error {
	checkpoints, err := store.Checkpoints(ctx)
	if err != nil {
		return err
	}
	queue := make(chan model.Output, saveQueueCapacity)
	source := sourceFuncs{history: ex.History}
	if err := collector.Run(ctx, c.Symbols, source, aggregate.New(c.Intervals, checkpoints, c.Start), queue); !errors.Is(err, io.EOF) {
		return err
	}
	return flushBatch(ctx, store.Append, queue, nil)
}

// Deliberately ignores cancellation until the test releases it.
type uncooperativeStore struct {
	memoryStore
	started, release chan struct{}
}

func (s *uncooperativeStore) Append(context.Context, int64, []model.Row) error {
	close(s.started)
	<-s.release
	return nil
}
func TestRunDoesNotWaitForUncooperativeWriter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &uncooperativeStore{started: make(chan struct{}), release: make(chan struct{})}
		ex := exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Query().Get("startTime") == "0" {
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(restPage(0, 60000, 120000, 180000, 240000, 300000)))}, nil
			}
			<-store.started
			return &http.Response{StatusCode: 400, Body: io.NopCloser(strings.NewReader("{}"))}, nil
		})}})
		started := time.Now()
		err := run(context.Background(), config.Config{Symbols: []string{"TEST"}, Intervals: []int64{300}, FlushEvery: 5 * time.Second}, ex, store, nil)
		close(store.release)
		if err == nil || err.Error() != "exchange HTTP status 400" || time.Since(started) != 5*time.Second {
			t.Fatal("collection failure waited for the stuck writer")
		}
	})
}
