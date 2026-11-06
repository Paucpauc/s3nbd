package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestLogLevelFiltering(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  []string
	}{
		{"debug", []string{"DEBUG", "INFO", "WARN", "ERROR"}},
		{"info", []string{"INFO", "WARN", "ERROR"}},
		{"warn", []string{"WARN", "ERROR"}},
		{"error", []string{"ERROR"}},
		{"", []string{"INFO", "WARN", "ERROR"}},
		{" DeBuG ", []string{"DEBUG", "INFO", "WARN", "ERROR"}},
	} {
		t.Run(tc.level, func(t *testing.T) {
			var out bytes.Buffer
			logger, err := newLogger(&out, tc.level)
			if err != nil {
				t.Fatal(err)
			}
			logger.Debug("debug message")
			logger.Info("info message")
			logger.Warn("warn message")
			logger.Error("error message")
			lines := strings.Split(strings.TrimSpace(out.String()), "\n")
			if len(lines) != len(tc.want) {
				t.Fatalf("got %d records, want %d: %s", len(lines), len(tc.want), out.String())
			}
			for i, level := range tc.want {
				if !strings.Contains(lines[i], "level="+level+" ") {
					t.Errorf("record %d: got %q, want level %s", i, lines[i], level)
				}
			}
		})
	}
}

func TestInvalidLogLevel(t *testing.T) {
	for _, level := range []string{"trace", "off", "inf", "INFO+1"} {
		if _, err := newLogger(&bytes.Buffer{}, level); err == nil {
			t.Errorf("expected error for LogLevel %q", level)
		}
	}
}
