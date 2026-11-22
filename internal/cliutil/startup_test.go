package cliutil

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestOpenWithRetry(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	var slept time.Duration
	sleep := func(d time.Duration) { slept += d }

	calls := 0
	v, err := openWithRetry(log, "db", 30*time.Second, func(context.Context) (int, error) {
		if calls++; calls < 3 {
			return 0, errors.New("connection refused")
		}
		return 7, nil
	}, sleep)
	if err != nil || v != 7 || calls != 3 || slept != 1500*time.Millisecond {
		t.Fatalf("v=%d err=%v calls=%d slept=%s", v, err, calls, slept)
	}

	// Never reachable: gives up within the window with the last error.
	slept, calls = 0, 0
	_, err = openWithRetry(log, "db", 30*time.Second, func(context.Context) (int, error) {
		calls++
		return 0, errors.New("no")
	}, sleep)
	if err == nil || slept > 30*time.Second || calls < 5 {
		t.Fatalf("err=%v calls=%d slept=%s", err, calls, slept)
	}
}
