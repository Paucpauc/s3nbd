package main

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// newLogger defaults to info so configurations without LogLevel remain valid.
func newLogger(w io.Writer, level string) (*slog.Logger, error) {
	var threshold slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		threshold = slog.LevelInfo
	case "debug":
		threshold = slog.LevelDebug
	case "warn":
		threshold = slog.LevelWarn
	case "error":
		threshold = slog.LevelError
	default:
		return nil, fmt.Errorf("invalid LogLevel %q: expected debug, info, warn, or error", level)
	}
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: threshold})), nil
}
