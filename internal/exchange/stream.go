package exchange

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"collector/internal/model"

	"github.com/coder/websocket"
)

func decodeStream(body []byte) (model.Candle, bool, error) {
	// Explicit uppercase tags prevent case-insensitive matches to e, t and l.
	type streamMessage struct {
		Data      *streamMessage  `json:"data"`
		Event     string          `json:"e"`
		EventTime json.RawMessage `json:"E"`
		Symbol    string          `json:"s"`
		K         *struct {
			Time      *int64          `json:"t"`
			CloseTime json.RawMessage `json:"T"`
			LastTrade json.RawMessage `json:"L"`
			Closed    *bool           `json:"x"`
			Interval  string          `json:"i"`
			Open      json.RawMessage `json:"o"`
			High      json.RawMessage `json:"h"`
			Low       json.RawMessage `json:"l"`
			Close     json.RawMessage `json:"c"`
			Volume    json.RawMessage `json:"v"`
			Amount    json.RawMessage `json:"q"`
			Trades    json.RawMessage `json:"n"`
			BuyVolume json.RawMessage `json:"V"`
			BuyAmount json.RawMessage `json:"Q"`
		} `json:"k"`
	}
	var message *streamMessage
	if json.Unmarshal(body, &message) != nil || message == nil {
		return model.Candle{}, false, errors.New("invalid stream message")
	}
	if message.Data != nil {
		message = message.Data
	}
	if message.Event == "error" {
		return model.Candle{}, false, errors.New("exchange stream error")
	}
	if message.Event != "kline" {
		return model.Candle{}, false, nil
	}
	k := message.K
	if k == nil || k.Time == nil || k.Closed == nil || k.Interval != "1m" || message.Symbol == "" || *k.Time < 0 || *k.Time%60000 != 0 {
		return model.Candle{}, false, errors.New("invalid stream candle metadata")
	}
	if !*k.Closed {
		// Open prices are never consumed; the timestamp can reveal a missed close.
		return model.Candle{Symbol: message.Symbol, Time: *k.Time / 1000}, false, nil
	}
	c, err := decodeCandle(message.Symbol, *k.Time, [9]json.RawMessage{k.Open, k.High, k.Low, k.Close, k.Volume, k.Amount, k.Trades, k.BuyVolume, k.BuyAmount})
	return c, err == nil, err
}

// Connect before the per-symbol REST handoff so incoming events can queue up.
func (e *Client) connect(ctx context.Context, symbols []string) (*websocket.Conn, error) {
	u, err := url.Parse(e.stream)
	if err != nil {
		return nil, errors.New("invalid stream URL")
	}
	streams := make([]string, len(symbols))
	for i, s := range symbols {
		streams[i] = strings.ToLower(s) + "@kline_1m"
	}
	q := u.Query()
	q.Set("streams", strings.Join(streams, "/"))
	u.RawQuery = q.Encode()
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, u.String(), nil)
	if err != nil {
		return nil, errors.New("stream connection failed")
	}
	conn.SetReadLimit(1 << 20)
	return conn, nil
}

// Stream owns the connection and leaves out open; cancellation ends the reader.
func (e *Client) Stream(ctx context.Context, symbols []string, out chan<- model.Event) error {
	conn, err := e.connect(ctx, symbols)
	if err != nil {
		return err
	}
	defer func() { go conn.CloseNow() }()
	return readStream(ctx, conn, out)
}

func readStream(ctx context.Context, conn *websocket.Conn, out chan<- model.Event) error {
	opened := make(map[string]int64)
	for {
		c, closed, err := readCandle(ctx, conn)
		if err != nil {
			return err
		}

		last, seen := opened[c.Symbol]
		if c.Symbol == "" || (!closed && seen && c.Time <= last) {
			continue
		}
		if !closed {
			opened[c.Symbol] = c.Time
		}
		select {
		case out <- model.Event{Candle: c, Closed: closed}:
		case <-ctx.Done():
			return ctx.Err()
		default:
			return errors.New("stream queue full; restart and backfill required")
		}
	}
}

func readCandle(ctx context.Context, conn *websocket.Conn) (model.Candle, bool, error) {
	ctx, done := context.WithTimeout(ctx, 90*time.Second)
	defer done()
	_, body, err := conn.Read(ctx)
	if err != nil {
		return model.Candle{}, false, errors.New("stream read failed or timed out")
	}
	return decodeStream(body)
}
