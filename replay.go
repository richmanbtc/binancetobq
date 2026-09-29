package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"

	"collector/internal/aggregate"
	"collector/internal/model"
)

// Replay has no network or credential access. Input and output are JSON lines.
func replay(input io.Reader, output io.Writer) error {
	e := aggregate.New([]int64{300, 3600}, nil, 0)
	scanner := bufio.NewScanner(input)
	encoder := json.NewEncoder(output)
	for scanner.Scan() {
		var candle model.Candle
		if json.Unmarshal(scanner.Bytes(), &candle) != nil || !model.ValidCandle(candle) || candle.Symbol == "" {
			return errors.New("invalid replay candle")
		}
		from := e.Resume(candle.Symbol)
		if candle.Time < from {
			continue
		}
		rows, err := e.Advance(model.Range{Symbol: candle.Symbol, From: from, Until: candle.Time + 60, Candles: []model.Candle{candle}})
		if err != nil {
			return err
		}
		for _, row := range rows {
			if encoder.Encode(row) != nil {
				return errors.New("writing replay result failed")
			}
		}
	}
	if scanner.Err() != nil {
		return errors.New("reading replay input failed")
	}
	return nil
}
