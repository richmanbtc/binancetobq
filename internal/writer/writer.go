package writer

import (
	"context"
	"log/slog"
	"time"

	"collector/internal/model"
)

type Save func(context.Context, int64, []model.Row) error

func flushBatch(ctx context.Context, save Save, in <-chan model.Output, onSaved func()) error {
	pending := make(map[int64][]model.Row)
	// Snapshot the queue so a busy producer cannot postpone the next upload.
	for range len(in) {
		row := <-in
		pending[row.Interval] = append(pending[row.Interval], row.Row)
	}
	for _, interval := range []int64{300, 3600} {
		rows := pending[interval]
		if len(rows) == 0 {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		oldest, newest := rows[0].Time, rows[0].Time
		symbols := make(map[string]struct{})
		for _, row := range rows {
			oldest, newest = min(oldest, row.Time), max(newest, row.Time)
			symbols[row.Symbol] = struct{}{}
		}
		started := time.Now()
		jobCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		err := save(jobCtx, interval, rows)
		cancel()
		if err != nil {
			slog.Error("warehouse save failed", "interval_seconds", interval, "rows", len(rows))
			return err
		}
		if onSaved != nil {
			onSaved()
		}
		slog.Info("warehouse batch saved", "rows", len(rows), "interval_seconds", interval, "symbol_count", len(symbols), "oldest_time", time.Unix(oldest, 0).UTC(), "newest_time", time.Unix(newest, 0).UTC(), "duration_ms", time.Since(started).Milliseconds())
	}
	return nil
}

func Run(ctx context.Context, save Save, in <-chan model.Output, flushEvery time.Duration, onSaved func()) error {
	ticker := time.NewTicker(flushEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := flushBatch(ctx, save, in, onSaved); err != nil {
				return err
			}
		}
	}
}
