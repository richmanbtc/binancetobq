package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"collector/internal/aggregate"
	"collector/internal/exchange"
	"collector/internal/model"
	"collector/internal/writer"
)

const restFixture = `[[0,"100","102","99","101","5",59999,"500",7,"2","200","0"]]`
const streamFixture = `{"data":{"e":"kline","s":"TEST","k":{"t":0,"i":"1m","x":true,"o":"100","h":"102","l":"99","c":"101","v":"5","q":"500","n":7,"V":"2","Q":"200"}}}`

func restPage(times ...int64) string {
	rows := make([]string, len(times))
	for i, ms := range times {
		rows[i] = strings.Replace(strings.TrimSuffix(strings.TrimPrefix(restFixture, "["), "]"), "[0,", fmt.Sprintf("[%d,", ms), 1)
	}
	return "[" + strings.Join(rows, ",") + "]"
}
func fixtureCandle() model.Candle {
	return model.Candle{Symbol: "TEST", Open: 100, High: 102, Low: 99, Close: 101, Volume: 5, Amount: 500, Trades: 7, BuyVolume: 2, BuyAmount: 200}
}
func sampleCandle(i int) model.Candle { c := fixtureCandle(); c.Time = int64(i) * 60; return c }
func testEngine() *aggregate.Engine   { return aggregate.New([]int64{300, 3600}, nil, 0) }

type sourceFuncs struct {
	history func(context.Context, string, int64, int64, func(model.Range) error) error
	stream  func(context.Context, []string, chan<- model.Event) error
}

func (s sourceFuncs) History(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
	if s.history == nil {
		return nil
	}
	return s.history(ctx, symbol, from, until, consume)
}
func (s sourceFuncs) Stream(ctx context.Context, symbols []string, out chan<- model.Event) error {
	if s.stream == nil {
		return io.EOF
	}
	return s.stream(ctx, symbols, out)
}

type sessionSource struct{ *exchange.Client }

func (s sessionSource) History(ctx context.Context, symbol string, from, until int64, consume func(model.Range) error) error {
	if until < 0 {
		return nil
	}
	return s.Client.History(ctx, symbol, from, until, consume)
}

// Exercise a single periodic flush through writer.Run without exposing its helper.
func flushBatch(ctx context.Context, save writer.Save, in chan model.Output, onSaved func()) error {
	intervals := make(map[int64]bool)
	queue := make(chan model.Output, len(in))
	for range cap(queue) {
		row := <-in
		intervals[row.Interval] = true
		queue <- row
	}
	if len(queue) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	saved := 0
	err := writer.Run(ctx, save, queue, time.Nanosecond, func() {
		saved++
		if onSaved != nil {
			onSaved()
		}
		if saved == len(intervals) {
			cancel()
		}
	})
	if saved == len(intervals) && errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
