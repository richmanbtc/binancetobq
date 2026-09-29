package aggregate

import (
	"testing"

	"collector/internal/model"
)

func TestVerifiedRangePreservesConfiguredIntervalOrder(t *testing.T) {
	for _, intervals := range [][]int64{{300, 3600}, {3600, 300}} {
		e := New(intervals, nil, 0)
		rows, err := e.Advance(model.Range{Symbol: "TEST", From: 0, Until: 3600, Candles: []model.Candle{sampleCandle(59)}})
		if err != nil || len(rows) != len(intervals) {
			t.Fatal("coincident interval boundaries did not emit both rows")
		}
		for i, row := range rows {
			if row.Interval != intervals[i] {
				t.Fatal("configured interval order changed")
			}
		}
	}
}

func TestVerifiedRangeClosesBucketAcrossMissingMinutes(t *testing.T) {
	e := New([]int64{300}, nil, 0)
	first := sampleCandle(0)
	rows, err := e.Advance(model.Range{Symbol: first.Symbol, From: 0, Until: 60, Candles: []model.Candle{first}})
	if err != nil || len(rows) != 0 {
		t.Fatal("first minute closed an incomplete bucket")
	}
	rows, err = e.Advance(model.Range{Symbol: first.Symbol, From: 60, Until: 600, Candles: nil})
	if err != nil || len(rows) != 1 || rows[0].Row.Time != 0 || rows[0].Row.Close != first.Close {
		t.Fatal("verified absence did not close the existing bucket")
	}
	if e.Resume(first.Symbol) != 600 {
		t.Fatal("empty range did not advance coverage")
	}
	rows, err = e.Advance(model.Range{Symbol: first.Symbol, From: 600, Until: 900, Candles: nil})
	if err != nil || len(rows) != 0 {
		t.Fatal("empty bucket was synthesized")
	}
	rows, err = e.Advance(model.Range{Symbol: first.Symbol, From: 900, Until: 1200, Candles: []model.Candle{sampleCandle(15)}})
	if err != nil || len(rows) != 1 || rows[0].Row.Time != 900 || rows[0].Row.Volume != sampleCandle(15).Volume || e.Resume(first.Symbol) != 1200 {
		t.Fatal("first candle after verified absence was lost or misaggregated")
	}
}

func TestVerifiedRangeRejectsInvalidInputBeforeMutation(t *testing.T) {
	first, second := sampleCandle(0), sampleCandle(1)
	for _, r := range []model.Range{
		{Symbol: first.Symbol, From: 60, Until: 120, Candles: []model.Candle{second}},
		{Symbol: first.Symbol, From: 0, Until: 60, Candles: []model.Candle{first, second}},
		{Symbol: first.Symbol, From: 0, Until: 120, Candles: []model.Candle{second, first}},
		{Symbol: first.Symbol, From: 0, Until: 120, Candles: []model.Candle{first, first}},
		{Symbol: first.Symbol, From: 0, Until: 60, Candles: []model.Candle{{Symbol: first.Symbol, Time: 0}}},
		{Symbol: first.Symbol},
	} {
		e := New([]int64{300}, nil, 0)
		if _, err := e.Advance(r); err == nil {
			t.Fatal("invalid range accepted")
		}
		if e.Resume(first.Symbol) != 0 {
			t.Fatal("rejected range mutated progress")
		}
		rows, err := e.Advance(model.Range{Symbol: first.Symbol, From: 0, Until: 300, Candles: []model.Candle{first}})
		if err != nil || len(rows) != 1 || rows[0].Row.Volume != first.Volume {
			t.Fatal("rejected range changed subsequent aggregation")
		}
	}
}
