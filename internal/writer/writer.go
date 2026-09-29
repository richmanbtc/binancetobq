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
		jobCtx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		err := save(jobCtx, interval, rows)
		cancel()
		if err != nil {
			return err
		}
		if onSaved != nil {
			onSaved()
		}
		slog.Info("warehouse batch saved", "rows", len(rows), "interval_seconds", interval)
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
