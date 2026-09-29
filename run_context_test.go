package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"collector/internal/config"
	"collector/internal/model"
)

func TestRunPreservesParentCancellationCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cause := errors.New("synthetic cancellation cause")
	source := sourceFuncs{history: func(ctx context.Context, _ string, _, _ int64, _ func(model.Range) error) error {
		cancel(cause)
		<-ctx.Done()
		return ctx.Err()
	}}
	c := config.Config{Symbols: []string{"TEST"}, Intervals: []int64{300}, FlushEvery: time.Hour}
	if err := run(ctx, c, source, &memoryStore{}, nil); !errors.Is(err, cause) {
		t.Fatalf("parent cancellation cause was lost: %v", err)
	}
}
