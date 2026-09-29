package aggregate

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"

	"collector/internal/model"
)

func sampleCandle(i int) model.Candle {
	p := 100 + float64(i%17)/5
	return model.Candle{Symbol: "TEST", Time: int64(i) * 60, Open: p, High: p + 2, Low: p - 1, Close: p + float64(i%3)/4, Volume: float64(i%7) + 1, Amount: float64(i%11) + 20, Trades: float64(i%5) + 2, BuyVolume: 0.5, BuyAmount: 3}
}

func testEngine() *Engine {
	return New([]int64{300, 3600}, nil, 0)
}

func TestPythonCompatibility(t *testing.T) {
	data, err := os.ReadFile("../../testdata/python_expected.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected []model.Output
	if err = json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	engine := testEngine()
	actual := make(map[[2]int64]model.Row)
	for i := 0; i <= 120; i++ {
		for _, row := range engine.advanceCandle(sampleCandle(i)) {
			actual[[2]int64{row.Interval, row.Row.Time}] = row.Row
		}
	}
	if len(actual) != len(expected) {
		t.Fatalf("row count: got %d, want %d", len(actual), len(expected))
	}
	for _, want := range expected {
		got, ok := actual[[2]int64{want.Interval, want.Row.Time}]
		if !ok {
			t.Fatal("missing aggregate")
		}
		a, b := reflect.ValueOf(got), reflect.ValueOf(want.Row)
		for i := 0; i < a.NumField(); i++ {
			x, y := reflect.Indirect(a.Field(i)), reflect.Indirect(b.Field(i))
			if x.IsValid() != y.IsValid() {
				t.Fatal("optional field mismatch")
			}
			if !x.IsValid() {
				continue
			}
			if x.Kind() == reflect.Float64 {
				if math.Abs(x.Float()-y.Float()) > 1e-10*math.Max(1, math.Abs(y.Float())) {
					t.Fatalf("field %s differs from Python", a.Type().Field(i).Name)
				}
			} else if !reflect.DeepEqual(x.Interface(), y.Interface()) {
				t.Fatalf("field %s mismatch", a.Type().Field(i).Name)
			}
		}
	}
}

func TestReplayAndCheckpoints(t *testing.T) {
	e := testEngine()
	var out []model.Output
	for i := 0; i < 5; i++ {
		out = append(out, e.advanceCandle(sampleCandle(i))...)
		out = append(out, e.advanceCandle(sampleCandle(i))...)
	}
	if len(out) != 1 || out[0].Row.Time != 0 {
		t.Fatal("duplicate or premature emission")
	}
	if len(e.advanceCandle(sampleCandle(1))) != 0 {
		t.Fatal("late candle emitted")
	}
	fresh := New([]int64{300, 3600}, map[int64]map[string]int64{300: {"TEST": 0}, 3600: {"TEST": -1}}, 0)
	for i := 0; i < 5; i++ {
		if len(fresh.advanceCandle(sampleCandle(i))) != 0 {
			t.Fatal("stored bucket replayed")
		}
	}
	rows, err := fresh.Advance(model.Range{Symbol: "TEST", From: 300, Until: 3600})
	if err != nil || len(rows) != 1 || rows[0].Interval != 3600 || rows[0].Row.Volume != 15 {
		t.Fatal("hourly state was lost")
	}
}

func TestGapsAndSingleSample(t *testing.T) {
	e := testEngine()
	if len(e.advanceCandle(sampleCandle(0))) != 0 {
		t.Fatal("partial bucket emitted early")
	}
	rows := e.advanceCandle(sampleCandle(5))
	if len(rows) != 1 || rows[0].Row.CloseStd != 0 || rows[0].Row.DiffStd != 0 {
		t.Fatal("single-sample partial bucket differs from Python")
	}
	if rows[0].Row.HighOpenMean != 2 || rows[0].Row.LowOpenMean != -1 {
		t.Fatal("legacy mean semantics changed")
	}
}

func TestResumeAtEarliestUnstoredBucket(t *testing.T) {
	e := New([]int64{300, 3600}, map[int64]map[string]int64{300: {"TEST": 3300}, 3600: {"TEST": 0}}, 0)
	if e.Resume("TEST") != 3600 {
		t.Fatal("wrong checkpoint resume boundary")
	}
	e.advanceCandle(sampleCandle(70))
	if e.Resume("TEST") != 4260 {
		t.Fatal("wrong live resume boundary")
	}
}

func BenchmarkAggregate(b *testing.B) {
	e := testEngine()
	c := sampleCandle(0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		c.Time = int64(i) * 60
		if _, err := e.Advance(model.Range{Symbol: c.Symbol, From: c.Time, Until: c.Time + 60, Candles: []model.Candle{c}}); err != nil {
			b.Fatal(err)
		}
	}
}

func TestCheckpointsRemainSeparateFromEmittedRows(t *testing.T) {
	checkpoints := map[int64]map[string]int64{300: {"TEST": -1}}
	e := New([]int64{300}, checkpoints, 0)
	for i := 0; i < 5; i++ {
		e.advanceCandle(sampleCandle(i))
	}
	if checkpoints[300]["TEST"] != -1 {
		t.Fatal("aggregation changed the warehouse snapshot")
	}
	fresh := New([]int64{300}, checkpoints, 0)
	if fresh.Resume("TEST") != 0 || e.Resume("TEST") != 300 {
		t.Fatal("restart and live recovery boundaries disagree")
	}
	other := sampleCandle(4)
	other.Symbol = "OTHER"
	if len(e.advanceCandle(other)) != 1 {
		t.Fatal("one symbol suppressed another symbol's model.Output")
	}
}

func TestSparseCheckpointsResumeFromConfiguredStart(t *testing.T) {
	const start = int64(3600)
	for _, checkpoints := range []map[int64]map[string]int64{
		nil,
		{300: {"TEST": 3900}},
		{300: {"TEST": 0}, 3600: {"TEST": 0}},
	} {
		e := New([]int64{300, 3600}, checkpoints, start)
		if e.Resume("TEST") != start {
			t.Fatal("missing or old checkpoint changed the configured start")
		}
	}
	e := New([]int64{300}, nil, 0)
	var rows []model.Output
	for i := 60; i < 65; i++ {
		rows = append(rows, e.advanceCandle(sampleCandle(i))...)
	}
	if len(rows) != 1 || rows[0].Row.Time != start {
		t.Fatal("missing checkpoint suppressed the first completed bucket")
	}
}

func TestHourlyFiveMinuteMeanWithGaps(t *testing.T) {
	e := New([]int64{3600}, nil, 0)
	// Multiple updates in one five-minute group, entirely absent groups, and
	// a missing final minute must still use only each present group's last close.
	var out []model.Output
	for _, minute := range []int{0, 2, 5, 19, 57, 60} {
		out = append(out, e.advanceCandle(sampleCandle(minute))...)
	}
	want := (sampleCandle(2).Close + sampleCandle(5).Close + sampleCandle(19).Close + sampleCandle(57).Close) / 4
	if len(out) != 1 || out[0].Row.TWAP5M == nil || math.Abs(*out[0].Row.TWAP5M-want) > 1e-12 {
		t.Fatal("absent five-minute groups affected hourly mean")
	}
}

// Test fixtures prove coverage through each candle, including absent minutes.
func (e *Engine) advanceCandle(c model.Candle) []model.Output {
	from := e.Resume(c.Symbol)
	if c.Time < from {
		return nil
	}
	rows, err := e.Advance(model.Range{Symbol: c.Symbol, From: from, Until: c.Time + 60, Candles: []model.Candle{c}})
	if err != nil {
		panic(err)
	}
	return rows
}
