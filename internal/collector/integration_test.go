package collector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"

	"collector/internal/aggregate"
	"collector/internal/exchange"
	"collector/internal/model"
)

func TestLiveGapRepairAndDuplicates(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Get("endTime") != "240000" {
			t.Error("incorrect gap boundary")
		}
		start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
		fmt.Fprint(w, restPage(start, start+60000))
	}))
	defer server.Close()
	rows := []model.Candle{sampleCandle(0)}
	out := make(chan model.Output, 2)
	c := testCollector(exchange.New(exchange.Options{REST: server.URL, HTTPClient: server.Client()}), out)
	if err := c.consume(model.Range{Symbol: rows[0].Symbol, From: 0, Until: 60, Candles: rows}); err != nil {
		t.Fatal(err)
	}
	candle := rows[0]
	candle.Time = 240
	err := c.accept(context.Background(), model.Event{Candle: candle, Closed: true}, false)
	if err != nil || requests != 3 || len(out) != 1 {
		t.Fatal("live gap was not repaired before aggregation")
	}
	if row := <-out; row.Row.Volume != 25 {
		t.Fatal("repaired minutes missing from aggregate")
	}
	for _, timestamp := range []int64{240, 60} {
		candle.Time = timestamp
		err = c.accept(context.Background(), model.Event{Candle: candle, Closed: true}, false)
		if err != nil || requests != 3 || len(out) != 0 {
			t.Fatal("duplicate or late candle was processed again")
		}
	}
}

func TestHandoffRequiresRESTOverlap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		times      []int64
		wantErr    bool
		wantVolume float64
	}{
		{"fills_missing_minutes", []int64{60000, 120000, 180000, 240000}, false, 25},
		{"ignores_boundary_and_later_rows", []int64{60000, 120000, 180000, 240000, 300000}, false, 25},
		{"genuine_gap", []int64{240000}, false, 10},
		{"empty_is_not_proof", nil, true, 0},
		{"behind_is_not_proof", []int64{60000}, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Query().Get("startTime") != "60000" || r.URL.Query().Get("endTime") != "240000" {
						t.Error("incorrect handoff range")
					}
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(restPage(tc.times...))), Header: make(http.Header)}, nil
				})}
				rows := []model.Candle{sampleCandle(0)}
				out := make(chan model.Output, 2)
				c := testCollector(exchange.New(exchange.Options{REST: "http://example.invalid", HTTPClient: client}), out)
				if err := c.consume(model.Range{Symbol: rows[0].Symbol, From: 0, Until: 60, Candles: rows}); err != nil {
					t.Fatal(err)
				}
				anchor := rows[0]
				anchor.Time = 240
				err := c.accept(context.Background(), model.Event{Candle: anchor, Closed: true}, true)
				if (err != nil) != tc.wantErr {
					t.Fatalf("unexpected handoff result: %v", err)
				}
				if tc.wantErr {
					if c.engine.Resume("TEST") != 60 || len(out) != 0 {
						t.Fatal("unverified stream candle advanced state")
					}
					return
				}
				if len(out) != 1 || (<-out).Row.Volume != tc.wantVolume {
					t.Fatal("handoff lost or duplicated candles")
				}
				if err := c.accept(context.Background(), model.Event{Candle: anchor, Closed: true}, false); err != nil || len(out) != 0 {
					t.Fatal("duplicate anchor was aggregated")
				}
			})
		})
	}
}

func TestOpenCandleRepairsMissedCloseWithoutUsingOpenValues(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch calls {
		case 1:
			if r.URL.Query().Get("startTime") != "0" || r.URL.Query().Get("endTime") != "0" {
				t.Error("first open event did not require overlap")
			}
			fmt.Fprint(w, restPage(0))
		case 2:
			if r.URL.Query().Get("startTime") != "240000" || r.URL.Query().Get("endTime") != "300000" {
				t.Error("missed close was not requested")
			}
			fmt.Fprint(w, restPage(240000, 300000))
		default:
			t.Error("unnecessary REST request")
			fmt.Fprint(w, "[]")
		}
	}))
	defer server.Close()
	ctx := context.Background()
	out := make(chan model.Output, 2)
	c := testCollector(exchange.New(exchange.Options{REST: server.URL, HTTPClient: server.Client()}), out)
	open := model.Event{Candle: model.Candle{Symbol: "TEST"}}
	if err := c.accept(ctx, open, true); err != nil {
		t.Fatal(err)
	}
	if c.engine.Resume("TEST") != 0 {
		t.Fatal("open candle entered aggregation")
	}
	rows := []model.Candle{sampleCandle(0), sampleCandle(1), sampleCandle(2), sampleCandle(3)}
	for _, row := range rows {
		if err := c.accept(ctx, model.Event{Candle: row, Closed: true}, false); err != nil {
			t.Fatal(err)
		}
	}
	open.Time = 300
	if err := c.accept(ctx, open, false); err != nil {
		t.Fatal(err)
	}
	if c.engine.Resume("TEST") != 300 || len(out) != 1 {
		t.Fatal("missed close was not recovered")
	}
	if row := <-out; row.Row.Volume != 25 || row.Row.Close != 101 {
		t.Fatal("open values polluted the completed bucket")
	}
	// A delayed close for the already repaired minute is harmless.
	closed := rows[0]
	closed.Time = 240
	if err := c.accept(ctx, model.Event{Candle: closed, Closed: true}, false); err != nil || len(out) != 0 || calls != 2 {
		t.Fatal("delayed close was counted twice")
	}
}

func testCollector(source Source, out chan<- model.Output) *collector {
	return &collector{ctx: context.Background(), source: source, engine: aggregate.New([]int64{300, 3600}, nil, 0), out: out}
}

func sampleCandle(i int) model.Candle {
	return model.Candle{Symbol: "TEST", Time: int64(i) * 60, Open: 100, High: 102, Low: 99, Close: 101, Volume: 5, Amount: 500, Trades: 7, BuyVolume: 2, BuyAmount: 200}
}

func restPage(times ...int64) string {
	rows := make([]string, len(times))
	for i, ms := range times {
		rows[i] = fmt.Sprintf(`[%d,"100","102","99","101","5",59999,"500",7,"2","200","0"]`, ms)
	}
	return "[" + strings.Join(rows, ",") + "]"
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
