package writer

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"collector/internal/model"
)

type memoryStore struct {
	batches [][]model.Row
	fail    bool
}

func (s *memoryStore) Append(ctx context.Context, interval int64, rows []model.Row) error {
	if s.fail {
		return errors.New("synthetic save failure")
	}
	s.batches = append(s.batches, append([]model.Row(nil), rows...))
	return nil
}

type notifyingStore struct {
	batchSizes chan int
}

func (s *notifyingStore) Append(_ context.Context, _ int64, rows []model.Row) error {
	s.batchSizes <- len(rows)
	return nil
}

func TestWriterUsesTimerAndDiscardsQueueOnCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		store := &notifyingStore{batchSizes: make(chan int, 2)}
		queue := make(chan model.Output, 3000)
		for range 2001 {
			queue <- model.Output{Interval: 300}
		}
		done := make(chan error, 1)
		go func() { done <- Run(ctx, store.Append, queue, 5*time.Second, nil) }()
		synctest.Wait()
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if len(store.batchSizes) != 0 || len(queue) != 2001 {
			t.Fatal("writer drained the queue before the timer")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(store.batchSizes) != 1 || <-store.batchSizes != 2001 {
			t.Fatal("timer did not save all queued rows")
		}
		queue <- model.Output{Interval: 300}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || len(store.batchSizes) != 0 || len(queue) != 1 {
			t.Fatal("cancellation drained queued rows")
		}
	})
}

func TestPeriodicBatchSavesBothIntervals(t *testing.T) {
	store := &memoryStore{}
	queue := make(chan model.Output, 2)
	queue <- model.Output{Interval: 300, Row: model.Row{Time: 0}}
	queue <- model.Output{Interval: 3600, Row: model.Row{Time: 0}}
	close(queue)
	saved := 0
	if err := flushBatch(context.Background(), store.Append, queue, func() { saved++ }); err != nil {
		t.Fatal(err)
	}
	if len(store.batches) != 2 || saved != 2 {
		t.Fatal("batch lost queued rows or save notifications")
	}
}

func TestWriterPropagatesFailure(t *testing.T) {
	store := &memoryStore{fail: true}
	queue := make(chan model.Output, 1)
	queue <- model.Output{Interval: 300}
	close(queue)
	if flushBatch(context.Background(), store.Append, queue, nil) == nil {
		t.Fatal("save failure was swallowed")
	}
}
