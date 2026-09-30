package config

import (
	"errors"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
)

type Config struct {
	Project, Dataset string
	REST, WebSocket  string
	Symbols          []string
	Tables           map[int64]string
	Intervals        []int64
	Start            int64
	LogLevel         slog.Level
	FlushEvery       time.Duration
}

func Read() (Config, error) {
	c := Config{Project: os.Getenv("GC_PROJECT_ID"), Dataset: os.Getenv("BINANCETOBQ_DATASET"), Tables: make(map[int64]string), FlushEvery: 5 * time.Second}
	// Allow legacy project IDs; BigQuery validates existence and validity.
	// Restrict characters because warehouse queries interpolate quoted paths.
	if !regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`).MatchString(c.Project) {
		return c, errors.New("set GC_PROJECT_ID using letters, digits, underscores, hyphens, dots or colons")
	}
	dataset := c.Dataset
	if i := strings.LastIndexByte(dataset, '.'); i >= 0 {
		if !regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`).MatchString(dataset[:i]) {
			return c, errors.New("invalid project in BINANCETOBQ_DATASET")
		}
		dataset = dataset[i+1:]
	}
	if len(dataset) > 1024 || !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(dataset) {
		return c, errors.New("set BINANCETOBQ_DATASET to dataset_id or project_id.dataset_id; dataset ID must use 1-1024 letters, digits or underscores")
	}
	switch strings.ToUpper(os.Getenv("BINANCETOBQ_LOG_LEVEL")) {
	case "DEBUG", "NOTSET":
		c.LogLevel = slog.LevelDebug
	case "", "INFO":
		c.LogLevel = slog.LevelInfo
	case "WARNING", "WARN":
		c.LogLevel = slog.LevelWarn
	case "ERROR":
		c.LogLevel = slog.LevelError
	case "CRITICAL", "FATAL":
		c.LogLevel = slog.LevelError + 4
	default:
		return c, errors.New("invalid BINANCETOBQ_LOG_LEVEL")
	}
	var table string
	switch os.Getenv("BINANCETOBQ_MARKET_TYPE") {
	case "spot":
		table = "binance_ohlcv_spot"
		c.REST = "https://api.binance.com/api/v3/klines"
		c.WebSocket = "wss://stream.binance.com:9443/stream"
	case "perp":
		table = "binance_ohlcv"
		c.REST = "https://fapi.binance.com/fapi/v1/klines"
		c.WebSocket = "wss://fstream.binance.com/market/stream"
	default:
		return c, errors.New("BINANCETOBQ_MARKET_TYPE must be spot or perp")
	}
	for _, symbol := range strings.Split(os.Getenv("BINANCETOBQ_SYMBOLS"), ",") {
		symbol = strings.ToUpper(strings.TrimSpace(symbol))
		if !regexp.MustCompile(`^[A-Z0-9]{1,26}$`).MatchString(symbol) {
			return c, errors.New("BINANCETOBQ_SYMBOLS must list base assets without USDT")
		}
		symbol += "USDT"
		if !slices.Contains(c.Symbols, symbol) {
			c.Symbols = append(c.Symbols, symbol)
		}
	}
	// Leave headroom below the exchange's per-connection subscription limit.
	if len(c.Symbols) > 200 {
		return c, errors.New("at most 200 symbols per process are supported")
	}
	for _, interval := range strings.Split(os.Getenv("BINANCETOBQ_INTERVALS"), ",") {
		var seconds int64
		name := table
		switch strings.TrimSpace(interval) {
		case "5m":
			seconds, name = 300, table+"_5m"
		case "1h":
			seconds = 3600
		default:
			return c, errors.New("BINANCETOBQ_INTERVALS must list 5m and/or 1h")
		}
		if !slices.Contains(c.Intervals, seconds) {
			c.Intervals = append(c.Intervals, seconds)
			c.Tables[seconds] = name
		}
	}
	return c, nil
}

// Until configuration is read, allow the longest supported interval.
func SaveTimeout(intervals []int64) time.Duration {
	interval := int64(3600)
	for _, value := range intervals {
		interval = min(interval, value)
	}
	return time.Duration(interval)*time.Second + 15*time.Minute
}
