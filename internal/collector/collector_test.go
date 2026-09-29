package collector

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"collector/internal/model"
)

type sourceFuncs struct {
	history func(context.Context, string, int64, int64, func(model.Range) error) error
	stream  func(context.Context, []string, chan<- model.Event) error
}

func (s sourceFuncs) History(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
	return s.history(ctx, symbol, from, until, consume)
}
func (s sourceFuncs) Stream(ctx context.Context, symbols []string, out chan<- model.Event) error {
	return s.stream(ctx, symbols, out)
}

type aggregatorFuncs struct {
	advance func(model.Range) ([]model.Output, error)
}

func (a aggregatorFuncs) Resume(string) int64                           { return 0 }
func (a aggregatorFuncs) Advance(r model.Range) ([]model.Output, error) { return a.advance(r) }

func TestRunUsesSuppliedSourcesAndAggregation(t *testing.T) {
	symbols := []string{"INVALID", "TEST"}
	var received []model.Range
	var subscribed []string
	source := sourceFuncs{
		history: func(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
			if symbol == "INVALID" {
				return model.ErrInvalidSymbol
			}
			return consume(model.Range{Symbol: symbol, From: from, Until: 60})
		},
		stream: func(ctx context.Context, symbols []string, out chan<- model.Event) error {
			subscribed = append([]string(nil), symbols...)
			return io.EOF
		},
	}
	engine := aggregatorFuncs{advance: func(r model.Range) ([]model.Output, error) {
		received = append(received, r)
		return []model.Output{{Interval: 300, Row: model.Row{Symbol: r.Symbol}}}, nil
	}}
	out := make(chan model.Output, 1)
	if err := Run(context.Background(), symbols, source, engine, out); !errors.Is(err, io.EOF) {
		t.Fatalf("unexpected result: %v", err)
	}
	if !reflect.DeepEqual(subscribed, []string{"TEST"}) || !reflect.DeepEqual(symbols, []string{"INVALID", "TEST"}) {
		t.Fatal("invalid-symbol filtering changed the caller's symbols")
	}
	if len(received) != 1 || received[0].Symbol != "TEST" || len(out) != 1 || (<-out).Row.Symbol != "TEST" {
		t.Fatal("verified range or aggregate output was lost")
	}
}

func TestSourceFailureCancelsBlockedHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan struct{})
	failure := errors.New("synthetic source failure")
	source := sourceFuncs{
		history: func(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
			if until < 0 {
				return nil
			}
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
		stream: func(ctx context.Context, symbols []string, out chan<- model.Event) error {
			out <- model.Event{Candle: model.Candle{Symbol: "TEST", Time: 60}}
			select {
			case <-started:
				return failure
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	engine := aggregatorFuncs{advance: func(model.Range) ([]model.Output, error) {
		t.Error("unverified event reached aggregation")
		return nil, nil
	}}
	if err := Run(ctx, []string{"TEST"}, source, engine, make(chan model.Output)); !errors.Is(err, failure) {
		t.Fatalf("source failure was lost: %v", err)
	}
}
