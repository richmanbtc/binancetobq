package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"time"

	"collector/internal/model"
	"collector/internal/retry"
)

func retryAfter(header http.Header, fallback time.Duration) time.Duration {
	value := header.Get("Retry-After")
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 && seconds <= math.MaxInt64/int64(time.Second) {
		return time.Duration(seconds) * time.Second
	}
	if date, err := http.ParseTime(value); err == nil {
		// Prefer the server clock, so local clock skew does not change the wait.
		now, err := http.ParseTime(header.Get("Date"))
		if err != nil {
			now = time.Now()
		}
		return max(0, date.Sub(now))
	}
	return fallback
}

func (e *Client) page(ctx context.Context, symbol string, start, end int64) ([]model.Candle, error) {
	u, err := url.Parse(e.rest)
	if err != nil {
		return nil, errors.New("invalid exchange URL")
	}
	q := u.Query()
	q.Set("symbol", symbol)
	q.Set("interval", "1m")
	q.Set("limit", "499")
	q.Set("startTime", strconv.FormatInt(start*1000, 10))
	// Include the boundary candle as evidence that the preceding candle closed.
	if end >= 0 {
		q.Set("endTime", strconv.FormatInt(end*1000, 10))
	}
	u.RawQuery = q.Encode()
	for attempt := 0; attempt < 8; attempt++ {
		if err := retry.Wait(ctx, e.pace); err != nil {
			return nil, err
		}
		status, header, body, readErr := e.request(ctx, u.String())
		delay := retry.Delay(attempt)
		if status == http.StatusOK && readErr == nil {
			return decodeREST(body, symbol)
		}
		var api struct {
			Code int `json:"code"`
		}
		_ = json.Unmarshal(body, &api)
		if api.Code == -1121 {
			return nil, model.ErrInvalidSymbol
		}
		if readErr == nil && status != 418 && status != 429 && status < 500 {
			return nil, fmt.Errorf("exchange HTTP status %d", status)
		}
		delay = retryAfter(header, delay)
		if attempt < 7 {
			slog.Warn("exchange request retry", "symbol", symbol, "status", status, "attempt", attempt+1, "wait_seconds", delay.Seconds())
		}
		if err := retry.Wait(ctx, delay); err != nil {
			return nil, err
		}
	}
	return nil, errors.New("exchange retry budget exhausted")
}

func decodeREST(body []byte, symbol string) ([]model.Candle, error) {
	var raw [][]json.RawMessage
	if json.Unmarshal(body, &raw) != nil || raw == nil {
		return nil, errors.New("invalid exchange response")
	}
	var result []model.Candle
	for _, r := range raw {
		if len(r) < 11 {
			return nil, errors.New("short candle response")
		}
		var ms *int64
		if json.Unmarshal(r[0], &ms) != nil || ms == nil {
			return nil, errors.New("invalid candle timestamp")
		}
		c, err := decodeCandle(symbol, *ms, [9]json.RawMessage{r[1], r[2], r[3], r[4], r[5], r[7], r[8], r[9], r[10]})
		if err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, nil
}

// end is an exclusive candle boundary, or negative for unbounded catch-up.
// A bounded request must see a candle at or beyond end, even if that tail is open.
func (e *Client) History(ctx context.Context, symbol string, start, end int64, consume func(model.Range) error) error {
	bounded := end >= 0
	pending := 0
	for {
		started := time.Now()
		rows, err := e.page(ctx, symbol, start, end)
		if err != nil {
			return err
		}
		slog.Debug("historical page received", "symbol", symbol, "rows", len(rows), "from", time.Unix(start, 0).UTC(), "duration_ms", time.Since(started).Milliseconds())
		previous := start - 1
		for _, c := range rows {
			if c.Time < start || c.Time <= previous {
				return errors.New("unordered historical candles")
			}
			previous = c.Time
		}
		until := start
		if len(rows) > 0 {
			until = rows[len(rows)-1].Time
		}
		if bounded {
			until = min(until, end)
		}
		if until > start {
			closed := rows[:sort.Search(len(rows), func(i int) bool { return rows[i].Time >= until })]
			if err := consume(model.Range{Symbol: symbol, From: start, Until: until, Candles: closed}); err != nil {
				return err
			}
			start, pending = until, 0
		}
		if (!bounded && len(rows) < 2) || (bounded && start >= end && len(rows) > 0) {
			return nil
		}
		if len(rows) >= 2 {
			continue
		}
		if pending == 3 {
			return errors.New("historical candle finality pending; restart required")
		}
		if err := retry.Wait(ctx, retry.Delay(pending)); err != nil {
			return err
		}
		pending++
	}
}

// Bound each external call, including a stalled body or Close. Deliberate
// Retry-After waits stay outside this deadline so restart cannot bypass them.
func (e *Client) request(ctx context.Context, target string) (int, http.Header, []byte, error) {
	ctx, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, nil, nil, errors.New("invalid exchange request")
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	return resp.StatusCode, resp.Header, body, err
}
