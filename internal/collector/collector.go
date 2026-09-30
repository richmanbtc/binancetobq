package collector

import (
	"cmp"
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"collector/internal/model"
)

// Sources must not close out. Stream returns on cancellation or failure.
type Source interface {
	History(context.Context, string, int64, int64, func(model.Range) error) error
	Stream(context.Context, []string, chan<- model.Event) error
}

type Aggregator interface {
	Resume(string) int64
	Advance(model.Range) ([]model.Output, error)
}

// collector owns collection state; only the socket reader and writer run separately.
type collector struct {
	ctx    context.Context
	source Source
	engine Aggregator
	out    chan<- model.Output
	active []string
}

func (c *collector) consume(r model.Range) error {
	rows, err := c.engine.Advance(r)
	if err != nil {
		return err
	}
	for _, row := range rows {
		select {
		case c.out <- row:
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
			return errors.New("save queue full; restart and backfill required")
		}
	}
	return nil
}

func (c *collector) backfill(ctx context.Context) error {
	kept := c.active[:0]
	for index, symbol := range c.active {
		from, started := c.engine.Resume(symbol), time.Now()
		slog.Info("historical symbol recovery started", "symbol", symbol, "position", index+1, "total", len(c.active), "from", time.Unix(from, 0).UTC())
		err := c.source.History(ctx, symbol, from, -1, c.consume)
		if errors.Is(err, model.ErrInvalidSymbol) {
			slog.Warn("skipping invalid symbol", "symbol", symbol, "position", index+1)
			continue
		}
		if err != nil {
			slog.Error("historical recovery failed", "symbol", symbol)
			return err
		}
		kept = append(kept, symbol)
		slog.Info("historical symbol recovered", "symbol", symbol, "position", index+1, "total", len(c.active), "from", time.Unix(from, 0).UTC(), "processed_until", time.Unix(c.engine.Resume(symbol), 0).UTC(), "duration_ms", time.Since(started).Milliseconds())
	}
	c.active = kept
	if len(c.active) == 0 {
		return errors.New("no valid symbols remain")
	}
	return nil
}

// The first event requires REST overlap. Later open events repair missed closes
// without aggregating unconfirmed prices or relying on a local clock.
func (c *collector) accept(ctx context.Context, event model.Event, initial bool) error {
	start := c.engine.Resume(event.Symbol)
	if initial || event.Time > start {
		if err := c.source.History(ctx, event.Symbol, min(start, event.Time), event.Time, c.consume); err != nil {
			return err
		}
	}
	if event.Closed && event.Time >= start {
		return c.consume(model.Range{Symbol: event.Symbol, From: c.engine.Resume(event.Symbol), Until: event.Time + 60, Candles: []model.Candle{event.Candle}})
	}
	return nil
}

// Errors return immediately; process exit closes remaining connections.
func (c *collector) session() error {
	ctx, cancel := context.WithCancelCause(c.ctx)
	defer cancel(nil)
	incoming := make(chan model.Event, 4096)
	go func() {
		err := c.source.Stream(ctx, c.active, incoming)
		if err != nil {
			slog.Error("realtime stream failed")
		}
		cancel(err)
	}()
	synced := make(map[string]bool)
	watch := time.NewTicker(time.Minute)
	defer watch.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-watch.C:
			slog.Info("collector running", "active_symbols", len(c.active), "queued_rows", len(c.out))
		case candle := <-incoming:
			if !slices.Contains(c.active, candle.Symbol) {
				continue
			}
			if err := c.accept(ctx, candle, !synced[candle.Symbol]); err != nil {
				slog.Error("realtime processing failed", "symbol", candle.Symbol)
				return cmp.Or(context.Cause(ctx), err)
			}
			synced[candle.Symbol] = true
		}
	}
}

func Run(ctx context.Context, symbols []string, source Source, engine Aggregator, out chan<- model.Output) error {
	c := &collector{ctx: ctx, source: source, engine: engine, out: out, active: slices.Clone(symbols)}
	if err := c.backfill(ctx); err != nil {
		return err
	}
	slog.Info("historical catch-up complete", "active_symbols", len(c.active))
	return c.session()
}
