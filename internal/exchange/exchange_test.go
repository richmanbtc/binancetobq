package exchange

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"collector/internal/model"
	"collector/internal/retry"

	"github.com/coder/websocket"
)

const restFixture = `[[0,"100","102","99","101","5",59999,"500",7,"2","200","0"]]`
const streamFixture = `{"data":{"e":"kline","s":"TEST","k":{"t":0,"i":"1m","x":true,"o":"100","h":"102","l":"99","c":"101","v":"5","q":"500","n":7,"V":"2","Q":"200"}}}`

func TestBothTransportsRejectInvalidCandleValues(t *testing.T) {
	for _, bad := range []string{`"NaN"`, `"+Inf"`, `"-Inf"`, `"1e999"`, `"\x31\x30\x30"`, `null`, `"0"`, `"103"`} {
		t.Run(bad, func(t *testing.T) {
			body := strings.Replace(restFixture, `"100"`, bad, 1)
			if _, err := decodeREST([]byte(body), "TEST"); err == nil {
				t.Fatal("REST accepted invalid open price")
			}
			body = strings.Replace(streamFixture, `"100"`, bad, 1)
			if _, _, err := decodeStream([]byte(body)); err == nil {
				t.Fatal("stream accepted invalid open price")
			}
		})
	}
}

func TestDecodeCandles(t *testing.T) {
	r, err := decodeREST([]byte(restFixture), "TEST")
	if err != nil {
		t.Fatal(err)
	}
	c, closed, err := decodeStream([]byte(streamFixture))
	if err != nil || !closed || len(r) != 1 || c != r[0] {
		t.Fatal("REST and stream disagree")
	}
	_, closed, err = decodeStream([]byte(strings.Replace(streamFixture, `"x":true`, `"x":false`, 1)))
	if err != nil || closed {
		t.Fatal("open stream candle accepted")
	}
	if _, err = decodeREST([]byte(`[[0,"NaN"]]`), "TEST"); err == nil {
		t.Fatal("malformed candle accepted")
	}
	if _, _, err = decodeStream([]byte(strings.Replace(streamFixture, `"100"`, `"NaN"`, 1))); err == nil {
		t.Fatal("nonfinite value accepted")
	}
}

func TestDecodeNumericRepresentations(t *testing.T) {
	for _, raw := range []string{`100`, `100.0`, `1e2`, `"100"`, `"1e2"`, `"\u0031\u0030\u0030"`} {
		t.Run(raw, func(t *testing.T) {
			rows, err := decodeREST([]byte(strings.Replace(restFixture, `"100"`, raw, 1)), "TEST")
			if err != nil || len(rows) != 1 || rows[0].Open != 100 {
				t.Fatal("REST numeric representation rejected")
			}
			c, closed, err := decodeStream([]byte(strings.Replace(streamFixture, `"100"`, raw, 1)))
			if err != nil || !closed || c.Open != 100 {
				t.Fatal("stream numeric representation rejected")
			}
		})
	}
}

func TestHistoryPaginationAndRateLimit(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(429)
			return
		}
		switch r.URL.Query().Get("startTime") {
		case "0":
			fmt.Fprint(w, restPage(0, 60000))
		case "60000":
			fmt.Fprint(w, restPage(60000, 120000))
		default:
			t.Error("unexpected pagination boundary")
			fmt.Fprint(w, "[]")
		}
	}))
	defer server.Close()
	ex := Client{rest: server.URL, client: server.Client()}
	count := 0
	err := ex.candleHistory(context.Background(), "TEST", 0, 120, func(c model.Candle) error { count++; return nil })
	if err != nil || count != 2 || calls != 3 {
		t.Fatalf("pagination or retry failed: count=%d calls=%d", count, calls)
	}
}

func TestInvalidSymbolIsDistinct(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(400); fmt.Fprint(w, `{"code":-1121}`) }))
	defer server.Close()
	ex := Client{rest: server.URL, client: server.Client()}
	_, err := ex.page(context.Background(), "TEST", 0, 60)
	if !errors.Is(err, model.ErrInvalidSymbol) {
		t.Fatal("invalid symbol not recognized")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if retry.Wait(ctx, time.Hour) == nil {
		t.Fatal("cancellation ignored")
	}
}

func TestStreamReaderAndSubscription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("streams") != "test@kline_1m" {
			t.Error("incorrect subscription")
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(streamFixture))
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ex := Client{stream: strings.Replace(server.URL, "http", "ws", 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := ex.connect(ctx, []string{"TEST"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	queue := make(chan model.Event, 1)
	done := make(chan error, 1)
	go func() { done <- readStream(ctx, conn, queue) }()
	select {
	case c := <-queue:
		if c.Close != 101 {
			t.Fatal("bad stream candle")
		}
	case <-ctx.Done():
		t.Fatal("stream timed out")
	}
	cancel()
	<-done
}

// Synthetic exchange rows, including a possibly open final row.
func restPage(times ...int64) string {
	rows := make([]string, len(times))
	for i, ms := range times {
		rows[i] = strings.Replace(strings.TrimSuffix(strings.TrimPrefix(restFixture, "["), "]"), "[0,", fmt.Sprintf("[%d,", ms), 1)
	}
	return "[" + strings.Join(rows, ",") + "]"
}

func TestHistoryRefetchesPageTailWithoutLocalClock(t *testing.T) {
	for _, base := range []int64{0, 7258118400000} {
		t.Run(strconv.FormatInt(base, 10), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Query().Has("endTime") {
					t.Error("catch-up depends on a clock cutoff")
				}
				start, _ := strconv.ParseInt(r.URL.Query().Get("startTime"), 10, 64)
				switch start {
				case base:
					times := make([]int64, 499)
					for i := range times {
						times[i] = base + int64(i)*60000
					}
					fmt.Fprint(w, restPage(times...))
				case base + 498*60000:
					// The previous page's partial tail has changed before its successor appeared.
					fmt.Fprint(w, strings.Replace(restPage(start, start+60000, start+120000), `"101"`, `"102"`, 1))
				case base + 500*60000:
					fmt.Fprint(w, restPage(start))
				default:
					t.Error("page tail was skipped")
					fmt.Fprint(w, "[]")
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ex := Client{rest: server.URL, client: server.Client()}
			var rows []model.Candle
			err := ex.candleHistory(ctx, "TEST", base/1000, -1, func(c model.Candle) error { rows = append(rows, c); return nil })
			if err != nil || len(rows) != 500 || calls != 3 {
				t.Fatal("incorrect pagination or finality")
			}
			for i, c := range rows {
				if c.Time != base/1000+int64(i)*60 {
					t.Fatal("missing or duplicate candle")
				}
			}
			if rows[498].Close != 102 {
				t.Fatal("stale partial page tail was consumed")
			}
		})
	}
}

func TestHistoryRejectsUnprovenTailAndUnorderedPage(t *testing.T) {
	for _, body := range []string{"[]", restPage(60000), restPage(60000, 0), restPage(0, 0)} {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			ex := Client{rest: "http://example.invalid", client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
			})}}
			consumed := 0
			before := time.Now()
			err := ex.candleHistory(context.Background(), "TEST", 0, 120, func(model.Candle) error { consumed++; return nil })
			if err == nil || consumed != 0 {
				t.Fatal("unproven or unordered candle advanced state")
			}
			if body == "[]" || body == restPage(60000) {
				if calls != 4 || time.Since(before) != 7*time.Second {
					t.Fatal("REST publication retries were not bounded")
				}
			} else if calls != 1 {
				t.Fatal("malformed history was retried")
			}
		})
	}
}

func TestStreamReaderCoalescesOpenUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		open := strings.Replace(streamFixture, `"x":true`, `"x":false`, 1)
		for range 20 {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(open)); err != nil {
				return
			}
		}
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(streamFixture))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(strings.Replace(open, `"t":0`, `"t":60000`, 1)))
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ex := Client{stream: strings.Replace(server.URL, "http", "ws", 1)}
	conn, err := ex.connect(ctx, []string{"TEST"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	queue := make(chan model.Event, 3)
	done := make(chan error, 1)
	go func() { done <- readStream(ctx, conn, queue) }()
	for _, want := range []struct {
		time   int64
		closed bool
	}{{0, false}, {0, true}, {60, false}} {
		select {
		case got, ok := <-queue:
			if !ok || got.Time != want.time || got.Closed != want.closed {
				t.Fatal("open updates filled queue or displaced a close")
			}
		case <-ctx.Done():
			t.Fatal("reader stalled")
		}
	}
	cancel()
	<-done
}

func BenchmarkDecodeStream(b *testing.B) {
	for _, closed := range []bool{false, true} {
		b.Run(fmt.Sprint(closed), func(b *testing.B) {
			body := []byte(strings.Replace(streamFixture, `"x":true`, fmt.Sprintf(`"x":%t`, closed), 1))
			b.ReportAllocs()
			for b.Loop() {
				if _, _, err := decodeStream(body); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestDecodersRejectMissingMetadata(t *testing.T) {
	for _, body := range []string{
		"null",
		strings.Replace(streamFixture, `"t":0,`, "", 1),
		strings.Replace(streamFixture, `"t":0`, `"t":null`, 1),
		strings.Replace(streamFixture, `"x":true,`, "", 1),
		strings.Replace(streamFixture, `"x":true`, `"x":null`, 1),
	} {
		if _, _, err := decodeStream([]byte(body)); err == nil {
			t.Fatal("missing stream metadata was accepted")
		}
	}
	for _, body := range []string{"null", strings.Replace(restFixture, "[[0,", "[[null,", 1)} {
		if _, err := decodeREST([]byte(body), "TEST"); err == nil {
			t.Fatal("null history or timestamp was accepted")
		}
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(streamFixture, `{"data":`), "}")
	a, closed, err := decodeStream([]byte(raw))
	b, _, _ := decodeStream([]byte(streamFixture))
	if err != nil || !closed || a != b {
		t.Fatal("raw and combined streams disagree")
	}
}

func FuzzExchangeDecoders(f *testing.F) {
	for _, seed := range []string{streamFixture, restFixture, "[]", "null", `{"e":"error"}`, `{"result":null,"id":1}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, body string) {
		c, closed, err := decodeStream([]byte(body))
		if err == nil && closed && (!model.ValidCandle(c) || c.Symbol == "") {
			t.Fatal("invalid closed candle accepted")
		}
		rows, err := decodeREST([]byte(body), "TEST")
		if err == nil {
			for _, c := range rows {
				if !model.ValidCandle(c) {
					t.Fatal("invalid historical candle accepted")
				}
			}
		}
	})
}

func TestRetryAfterUsesServerClockAndRejectsOverflow(t *testing.T) {
	fallback := 7 * time.Second
	for _, tc := range []struct {
		value string
		want  time.Duration
	}{
		{"0", 0}, {"3", 3 * time.Second}, {"-1", fallback}, {"9223372036854775807", fallback},
		{"bad", fallback}, {"Thu, 01 Jan 1970 00:00:30 GMT", 30 * time.Second},
		{"Wed, 31 Dec 1969 23:59:59 GMT", 0},
	} {
		h := make(http.Header)
		h.Set("Date", "Thu, 01 Jan 1970 00:00:00 GMT")
		h.Set("Retry-After", tc.value)
		if got := retryAfter(h, fallback); got != tc.want {
			t.Fatalf("unexpected retry delay: %v", got)
		}
	}
}

func TestHistoryWaitsForRESTPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		ex := Client{rest: "http://example.invalid", client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			calls++
			body := restPage(0)
			if calls > 1 {
				body = restPage(0, 60000)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
		})}}
		var rows []model.Candle
		before := time.Now()
		err := ex.candleHistory(context.Background(), "TEST", 0, 60, func(c model.Candle) error { rows = append(rows, c); return nil })
		if err != nil || calls != 2 || len(rows) != 1 || rows[0].Time != 0 || time.Since(before) != time.Second {
			t.Fatal("temporary REST lag was not recovered")
		}
	})
}
func TestStreamQueueOverflowReturnsForRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(streamFixture))
		_, _, _ = conn.Read(r.Context())
	}))
	defer server.Close()
	ex := Client{stream: strings.Replace(server.URL, "http", "ws", 1)}
	conn, err := ex.connect(ctx, []string{"TEST"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if readStream(ctx, conn, make(chan model.Event)) == nil {
		t.Fatal("overflow was silently discarded")
	}
	if ctx.Err() != nil {
		t.Fatal("overflow waited for timeout")
	}
}

func TestRESTSingleTailVerifiesAbsenceWithoutUsingPrices(t *testing.T) {
	ex := &Client{rest: "http://example.invalid", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(restPage(300000)))}, nil
	})}}
	var ranges []model.Range
	err := ex.History(context.Background(), "TEST", 0, 300, func(r model.Range) error { ranges = append(ranges, r); return nil })
	if err != nil || len(ranges) != 1 || ranges[0].From != 0 || ranges[0].Until != 300 || len(ranges[0].Candles) != 0 {
		t.Fatal("tail did not prove the empty preceding range")
	}
}

func (e *Client) candleHistory(ctx context.Context, symbol string, start, end int64, consume func(model.Candle) error) error {
	return e.History(ctx, symbol, start, end, func(r model.Range) error {
		for _, c := range r.Candles {
			if err := consume(c); err != nil {
				return err
			}
		}
		return nil
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
