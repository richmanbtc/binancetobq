package model

import (
	"math"
	"testing"
)

func TestValidCandleNumericBounds(t *testing.T) {
	c := Candle{Open: 2, High: 3, Low: 1, Close: 2}
	if !ValidCandle(c) {
		t.Fatal("valid candle with zero quantities rejected")
	}
	for i, field := range []*float64{&c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Amount, &c.Trades, &c.BuyVolume, &c.BuyAmount} {
		original := *field
		for _, bad := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
			*field = bad
			if ValidCandle(c) {
				t.Errorf("invalid value accepted in field %d", i)
			}
		}
		if i < 4 {
			*field = 0
			if ValidCandle(c) {
				t.Errorf("zero price accepted in field %d", i)
			}
		}
		*field = original
	}
	c.High = 1
	if ValidCandle(c) {
		t.Fatal("high below open and close accepted")
	}
	c.High, c.Low = 3, 2.5
	if ValidCandle(c) {
		t.Fatal("low above open and close accepted")
	}
}
