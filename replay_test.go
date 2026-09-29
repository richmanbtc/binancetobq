package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"os"
	"reflect"
	"testing"

	"collector/internal/model"
)

func TestReplayMatchesPythonGoldenWithDuplicates(t *testing.T) {
	var input, output bytes.Buffer
	encoder := json.NewEncoder(&input)
	for i := 0; i <= 120; i++ {
		p := 100 + float64(i%17)/5
		c := model.Candle{Symbol: "TEST", Time: int64(i) * 60, Open: p, High: p + 2, Low: p - 1, Close: p + float64(i%3)/4, Volume: float64(i%7) + 1, Amount: float64(i%11) + 20, Trades: float64(i%5) + 2, BuyVolume: 0.5, BuyAmount: 3}
		for range 2 {
			if err := encoder.Encode(c); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := replay(&input, &output); err != nil {
		t.Fatal(err)
	}
	actual := make(map[[2]int64]model.Row)
	decoder := json.NewDecoder(&output)
	for {
		var row model.Output
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		key := [2]int64{row.Interval, row.Row.Time}
		if _, exists := actual[key]; exists {
			t.Fatal("duplicate replay aggregate")
		}
		actual[key] = row.Row
	}
	data, err := os.ReadFile("testdata/python_expected.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected []model.Output
	if err = json.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	if len(actual) != len(expected) {
		t.Fatal("replay emitted an incomplete bucket or lost a complete bucket")
	}
	for _, want := range expected {
		got, ok := actual[[2]int64{want.Interval, want.Row.Time}]
		if !ok {
			t.Fatal("missing replay aggregate")
		}
		a, b := reflect.ValueOf(got), reflect.ValueOf(want.Row)
		for i := 0; i < a.NumField(); i++ {
			x, y := reflect.Indirect(a.Field(i)), reflect.Indirect(b.Field(i))
			if x.IsValid() != y.IsValid() {
				t.Fatal("optional replay field mismatch")
			}
			if !x.IsValid() {
				continue
			}
			if x.Kind() == reflect.Float64 {
				if math.Abs(x.Float()-y.Float()) > 1e-10*math.Max(1, math.Abs(y.Float())) {
					t.Fatalf("replay field %s differs from Python", a.Type().Field(i).Name)
				}
			} else if !reflect.DeepEqual(x.Interface(), y.Interface()) {
				t.Fatalf("replay field %s differs from Python", a.Type().Field(i).Name)
			}
		}
	}
}
