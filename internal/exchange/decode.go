package exchange

import (
	"encoding/json"
	"errors"
	"strconv"

	"collector/internal/model"
)

// Both transports validate JSON before passing encoded numbers here.
func decodeCandle(symbol string, ms int64, values [9]json.RawMessage) (model.Candle, error) {
	if ms%60000 != 0 {
		return model.Candle{}, errors.New("invalid candle timestamp")
	}
	c := model.Candle{Symbol: symbol, Time: ms / 1000}
	fields := []*float64{&c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Amount, &c.Trades, &c.BuyVolume, &c.BuyAmount}
	for i, field := range fields {
		text := string(values[i])
		if len(text) > 0 && text[0] == '"' {
			var err error
			text, err = strconv.Unquote(text)
			if err != nil {
				return model.Candle{}, errors.New("invalid candle number")
			}
		}
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return model.Candle{}, errors.New("nonfinite or invalid candle number")
		}
		*field = value
	}
	if !model.ValidCandle(c) {
		return model.Candle{}, errors.New("invalid candle values")
	}
	return c, nil
}
