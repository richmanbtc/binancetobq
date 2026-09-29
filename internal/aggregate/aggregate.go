package aggregate

import (
	"errors"
	"math"

	"collector/internal/model"
)

type moments struct {
	n        int
	mean, m2 float64
}

func (m *moments) add(x float64) {
	m.n++
	d := x - m.mean
	m.mean += d / float64(m.n)
	m.m2 += d * (x - m.mean)
}

func (m moments) std() float64 {
	if m.n < 2 {
		return 0
	}
	return math.Sqrt(math.Max(0, m.m2/float64(m.n-1)))
}

type bucket struct {
	model.Row
	closes, diffs    moments
	fiveMinuteCloses moments
	fiveMinute       int64
}

func (b *bucket) add(c model.Candle, interval int64) {
	previous := b.Close
	if b.closes.n == 0 {
		b.Row = model.Row{Symbol: c.Symbol, Time: c.Time / interval * interval, Open: c.Open, High: c.High, Low: c.Low}
		previous = c.Open
	}
	if interval > 300 && b.closes.n > 0 && b.fiveMinute != c.Time/300 {
		b.fiveMinuteCloses.add(previous)
	}
	b.fiveMinute = c.Time / 300
	b.High = math.Max(b.High, c.High)
	b.Low = math.Min(b.Low, c.Low)
	b.Close = c.Close
	b.Volume += c.Volume
	b.Amount += c.Amount
	b.Trades += c.Trades
	b.BuyVolume += c.BuyVolume
	b.BuyAmount += c.BuyAmount
	b.HighTWAP += c.High
	b.LowTWAP += c.Low
	b.HighOpenMean += c.High - c.Open
	b.LowOpenMean += c.Low - c.Open
	lr := math.Log(c.High / c.Low)
	b.LogRangeMean += lr
	b.LogRangeSquareMean += lr * lr
	b.closes.add(c.Close)
	b.diffs.add(c.Close - previous)
}

func (b *bucket) finish(interval int64) model.Row {
	r := b.Row
	n := float64(b.closes.n)
	r.TWAP, r.CloseStd, r.DiffStd = b.closes.mean, b.closes.std(), b.diffs.std()
	r.HighTWAP /= n
	r.LowTWAP /= n
	r.HighOpenMean /= n
	r.LowOpenMean /= n
	r.LogRangeMean /= n
	r.LogRangeSquareMean /= n
	if interval > 300 {
		closes := b.fiveMinuteCloses
		closes.add(r.Close)
		value := closes.mean
		r.TWAP5M = &value
	}
	return r
}

// Warehouse checkpoints stay fixed; series.next tracks live coverage.
type intervalState struct {
	interval, checkpoint int64
	bucket               bucket
}

type series struct {
	next      int64
	intervals []intervalState
}

// Engine has one owner. No locks or shared mutable aggregation state are needed.
type Engine struct {
	start     int64
	intervals []int64
	series    map[string]*series
}

func New(intervals []int64, checkpoints model.Checkpoints, start int64) *Engine {
	e := &Engine{start: start, intervals: intervals, series: make(map[string]*series)}
	for i, interval := range intervals {
		for symbol, checkpoint := range checkpoints[interval] {
			e.symbol(symbol).intervals[i].checkpoint = checkpoint
		}
	}
	return e
}

func (e *Engine) symbol(symbol string) *series {
	s := e.series[symbol]
	if s == nil {
		s = &series{next: -1, intervals: make([]intervalState, len(e.intervals))}
		for i, interval := range e.intervals {
			s.intervals[i] = intervalState{interval: interval, checkpoint: -1}
		}
		e.series[symbol] = s
	}
	return s
}

func (e *Engine) Resume(symbol string) int64 {
	s := e.symbol(symbol)
	if s.next >= 0 {
		return s.next
	}
	resume := int64(math.MaxInt64)
	for _, state := range s.intervals {
		candidate := e.start
		if checkpoint := state.checkpoint; checkpoint >= e.start {
			candidate = checkpoint + state.interval
		}
		resume = min(resume, candidate)
	}
	return resume
}

// Advance is the sole live aggregation ingress. Validate the entire range before
// changing state; an unobserved interval must never become an implicit gap.
func (e *Engine) Advance(r model.Range) ([]model.Output, error) {
	if r.From != e.Resume(r.Symbol) || r.From < 0 || r.From%60 != 0 || r.Until <= r.From || r.Until%60 != 0 {
		return nil, errors.New("noncontiguous verified range")
	}
	previous := r.From - 1
	for _, c := range r.Candles {
		if c.Symbol != r.Symbol || c.Time <= previous || c.Time < r.From || c.Time >= r.Until || !model.ValidCandle(c) {
			return nil, errors.New("invalid verified candle range")
		}
		previous = c.Time
	}
	var out []model.Output
	s := e.symbol(r.Symbol)
	for _, c := range r.Candles {
		for i := range s.intervals {
			state := &s.intervals[i]
			if state.bucket.closes.n > 0 && state.bucket.Time != c.Time/state.interval*state.interval {
				out = state.finish(out)
			}
			state.bucket.add(c, state.interval)
			if (c.Time+60)%state.interval == 0 {
				out = state.finish(out)
			}
		}
	}
	for i := range s.intervals {
		state := &s.intervals[i]
		if state.bucket.closes.n > 0 && state.bucket.Time+state.interval <= r.Until {
			out = state.finish(out)
		}
	}
	s.next = r.Until
	return out, nil
}

// Both price transitions and verified empty tails use the same emission gate.
func (s *intervalState) finish(out []model.Output) []model.Output {
	if s.bucket.Time > s.checkpoint {
		out = append(out, model.Output{Interval: s.interval, Row: s.bucket.finish(s.interval)})
	}
	s.bucket = bucket{}
	return out
}
