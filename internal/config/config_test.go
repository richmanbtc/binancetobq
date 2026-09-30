package config

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPythonCompatibleConfig(t *testing.T) {
	t.Setenv("BINANCETOBQ_LOG_LEVEL", "INFO")
	t.Setenv("GC_PROJECT_ID", "test-project")
	t.Setenv("BINANCETOBQ_DATASET", "test_dataset")
	t.Setenv("BINANCETOBQ_SYMBOLS", "AAA,BBB,AAA")
	for _, tc := range []struct {
		market, rest, stream, table string
	}{
		{"spot", "https://api.binance.com/api/v3/klines", "wss://stream.binance.com:9443/stream", "binance_ohlcv_spot"},
		{"perp", "https://fapi.binance.com/fapi/v1/klines", "wss://fstream.binance.com/market/stream", "binance_ohlcv"},
	} {
		t.Run(tc.market, func(t *testing.T) {
			t.Setenv("BINANCETOBQ_MARKET_TYPE", tc.market)
			for _, intervals := range []string{"5m", "1h", "5m,1h"} {
				t.Setenv("BINANCETOBQ_INTERVALS", intervals)
				c, err := Read()
				if err != nil {
					t.Fatal(err)
				}
				if c.Project != "test-project" || c.Dataset != "test_dataset" || !reflect.DeepEqual(c.Symbols, []string{"AAAUSDT", "BBBUSDT"}) {
					t.Fatal("Python environment settings were not honored")
				}
				if c.REST != tc.rest || c.WebSocket != tc.stream {
					t.Fatal("incorrect market endpoints")
				}
				want := map[int64]string{}
				if intervals != "1h" {
					want[300] = tc.table + "_5m"
				}
				if intervals != "5m" {
					want[3600] = tc.table
				}
				if !reflect.DeepEqual(c.Tables, want) || len(c.Intervals) != len(want) {
					t.Fatal("incorrect table selection")
				}
			}
		})
	}
	t.Setenv("BINANCETOBQ_MARKET_TYPE", "spot")
	t.Setenv("BINANCETOBQ_INTERVALS", "5m,1h")
	for name, level := range map[string]slog.Level{"debug": slog.LevelDebug, "INFO": slog.LevelInfo, "WARNING": slog.LevelWarn, "ERROR": slog.LevelError, "CRITICAL": slog.LevelError + 4, "NOTSET": slog.LevelDebug} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("BINANCETOBQ_LOG_LEVEL", name)
			c, err := Read()
			if err != nil || c.LogLevel != level {
				t.Fatal("Python log level was not honored")
			}
		})
	}
	for _, tc := range []struct{ key, value string }{
		{"BINANCETOBQ_MARKET_TYPE", ""}, {"BINANCETOBQ_MARKET_TYPE", "futures"},
		{"BINANCETOBQ_INTERVALS", ""}, {"BINANCETOBQ_INTERVALS", "1m"},
		{"BINANCETOBQ_SYMBOLS", ""},
		{"BINANCETOBQ_LOG_LEVEL", "invalid"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Read(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}
func TestSaveTimeout(t *testing.T) {
	for _, tt := range []struct {
		intervals []int64
		want      time.Duration
	}{
		{nil, 75 * time.Minute},
		{[]int64{300}, 20 * time.Minute},
		{[]int64{300, 3600}, 20 * time.Minute},
		{[]int64{3600, 300}, 20 * time.Minute},
		{[]int64{3600}, 75 * time.Minute},
	} {
		if SaveTimeout(tt.intervals) != tt.want {
			t.Fatal("incorrect save deadline")
		}
	}
}

func TestBigQueryIdentifiers(t *testing.T) {
	t.Setenv("BINANCETOBQ_MARKET_TYPE", "spot")
	t.Setenv("BINANCETOBQ_INTERVALS", "5m")
	t.Setenv("BINANCETOBQ_SYMBOLS", "AAA")
	t.Setenv("BINANCETOBQ_LOG_LEVEL", "INFO")
	for _, tc := range []struct {
		name, project, dataset, wantError string
	}{
		{"standard", "test-project", "test_dataset", ""},
		{"numeric dataset", "test-project", "2024", ""},
		{"numeric prefix", "test-project", "2024_market", ""},
		{"hidden dataset", "test-project", "_hidden", ""},
		{"maximum dataset", "test-project", strings.Repeat("a", 1024), ""},
		{"domain project", "example.com:test-project", "test_dataset", ""},
		{"legacy project", "legacy_project", "test_dataset", ""},
		{"short project", "old", "test_dataset", ""},
		{"empty project", "", "test_dataset", "GC_PROJECT_ID"},
		{"empty dataset", "test-project", "", "BINANCETOBQ_DATASET"},
		{"long dataset", "test-project", strings.Repeat("a", 1025), "BINANCETOBQ_DATASET"},
		{"dataset hyphen", "test-project", "test-dataset", "BINANCETOBQ_DATASET"},
		{"qualified dataset", "test-project", "other-project.dataset", ""},
		{"qualified domain dataset", "test-project", "example.com:other-project.dataset", ""},
		{"qualified maximum dataset", "test-project", "other-project." + strings.Repeat("a", 1024), ""},
		{"qualified long dataset", "test-project", "other-project." + strings.Repeat("a", 1025), "BINANCETOBQ_DATASET"},
		{"empty qualified project", "test-project", ".dataset", "BINANCETOBQ_DATASET"},
		{"empty qualified dataset", "test-project", "other-project.", "BINANCETOBQ_DATASET"},
		{"qualified project quote", "test-project", "other`project.dataset", "BINANCETOBQ_DATASET"},
		{"project space", "test project", "test_dataset", "GC_PROJECT_ID"},
		{"project newline", "test-project\n", "test_dataset", "GC_PROJECT_ID"},
		{"project quote", "test`project", "test_dataset", "GC_PROJECT_ID"},
		{"project escape", "test\\project", "test_dataset", "GC_PROJECT_ID"},
		{"dataset quote", "test-project", "test`dataset", "BINANCETOBQ_DATASET"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GC_PROJECT_ID", tc.project)
			t.Setenv("BINANCETOBQ_DATASET", tc.dataset)
			c, err := Read()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("expected error naming %s, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.Project != tc.project || c.Dataset != tc.dataset {
				t.Fatal("identifiers were changed")
			}
		})
	}
}
