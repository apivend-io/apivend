package cliutil

import (
	"context"
	"log/slog"
	"time"
)

// OpenWithRetry calls open until it succeeds or about window has passed,
// backing off from half a second to five. A new pod's first connection can
// be refused for a few seconds (a NetworkPolicy admits its address with a
// delay); exiting at once turns that into a restart.
func OpenWithRetry[T any](log *slog.Logger, what string, window time.Duration, open func(context.Context) (T, error)) (T, error) {
	return openWithRetry(log, what, window, open, time.Sleep)
}

func openWithRetry[T any](log *slog.Logger, what string, window time.Duration, open func(context.Context) (T, error), sleep func(time.Duration)) (T, error) {
	wait, waited := 500*time.Millisecond, time.Duration(0)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		v, err := open(ctx)
		cancel()
		if err == nil || waited+wait > window {
			return v, err
		}
		log.Warn(what+": not reachable yet, retrying", "in", wait.String(), "err", err)
		sleep(wait)
		waited += wait
		wait = min(wait*2, 5*time.Second)
	}
}
