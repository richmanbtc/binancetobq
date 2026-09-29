package retry

import (
	"context"
	"time"
)

func Delay(attempt int) time.Duration {
	return time.Second * time.Duration(1<<min(attempt, 5))
}

func Wait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
