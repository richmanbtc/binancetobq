package model

import (
	"errors"
	"math"
)

var ErrInvalidSymbol = errors.New("exchange rejected a symbol")

type Checkpoints map[int64]map[string]int64

// Candle contains one-minute values; acquisition establishes finality.
// All timestamps are UTC seconds.
type Candle struct {
	Symbol                                       string
	Time                                         int64
	Open, High, Low, Close                       float64
	Volume, Amount, Trades, BuyVolume, BuyAmount float64
}

type Row struct {
	Symbol             string   `json:"symbol"`
	Time               int64    `json:"timestamp"`
	Open               float64  `json:"op"`
	High               float64  `json:"hi"`
	Low                float64  `json:"lo"`
	Close              float64  `json:"cl"`
	Volume             float64  `json:"volume"`
	Amount             float64  `json:"amount"`
	Trades             float64  `json:"trades"`
	BuyVolume          float64  `json:"buy_volume"`
	BuyAmount          float64  `json:"buy_amount"`
	TWAP               float64  `json:"twap"`
	TWAP5M             *float64 `json:"twap_5m,omitempty"`
	CloseStd           float64  `json:"cl_std"`
	DiffStd            float64  `json:"cl_diff_std"`
	HighTWAP           float64  `json:"hi_twap"`
	LowTWAP            float64  `json:"lo_twap"`
	HighOpenMean       float64  `json:"hi_op_max"`
	LowOpenMean        float64  `json:"lo_op_min"`
	LogRangeMean       float64  `json:"ln_hi_lo_mean"`
	LogRangeSquareMean float64  `json:"ln_hi_lo_sqr_mean"`
}

type Output struct {
	Interval int64
	Row      Row
}

// Range covers [From, Until), including source minutes proven absent.
// Acquisition establishes finality; aggregation checks coverage before mutation.
type Range struct {
	Symbol      string
	From, Until int64
	Candles     []Candle
}

type Event struct {
	Candle
	Closed bool
}

func ValidCandle(c Candle) bool {
	for _, value := range [...]float64{c.Open, c.High, c.Low, c.Close, c.Volume, c.Amount, c.Trades, c.BuyVolume, c.BuyAmount} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return c.Time >= 0 && c.Time%60 == 0 && c.Low > 0 && c.Low <= math.Min(c.Open, c.Close) && c.High >= math.Max(c.Open, c.Close)
}
