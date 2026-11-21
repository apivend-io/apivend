package settle

import (
	"sync"
	"time"
)

// health remembers which settlement backends are failing, so that during an
// outage the settler stops waiting out a dead backend's timeout on every job
// and goes straight to the next one (a circuit breaker per backend).
//
//   - closed: the backend is tried as usual.
//   - open: after tripAfter consecutive failures it is skipped for a
//     cool-down, which doubles on each failed probe (minCooldown..maxCooldown).
//   - half-open: once the cool-down has passed, one job probes it; success
//     closes it, failure opens it again.
//
// A refusal is not a failure: the backend answered. State is per settler
// process and keyed by backend name, so it survives provider reloads.
type health struct {
	mu       sync.Mutex
	backends map[string]*backendHealth
}

type backendHealth struct {
	fails     int
	cooldown  time.Duration
	openUntil time.Time
	probing   bool
}

const (
	tripAfter   = 2
	minCooldown = 10 * time.Second
	maxCooldown = 2 * time.Minute
)

func (h *health) get(name string) *backendHealth {
	if h.backends == nil {
		h.backends = map[string]*backendHealth{}
	}
	b := h.backends[name]
	if b == nil {
		b = &backendHealth{}
		h.backends[name] = b
	}
	return b
}

// allow reports whether a job may try the backend now. When it lets a job
// probe an open backend, no other job probes it until result is called.
func (h *health) allow(name string, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.get(name)
	if b.openUntil.IsZero() {
		return true
	}
	if now.Before(b.openUntil) || b.probing {
		return false
	}
	b.probing = true
	return true
}

// result records an attempt. It reports whether the backend is now up, and
// whether that changed.
func (h *health) result(name string, ok bool, now time.Time) (up, changed bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.get(name)
	wasUp := b.openUntil.IsZero()
	b.probing = false
	if ok {
		*b = backendHealth{}
		return true, !wasUp
	}
	b.fails++
	if b.fails < tripAfter {
		return wasUp, false
	}
	switch {
	case b.cooldown == 0:
		b.cooldown = minCooldown
	case b.cooldown < maxCooldown:
		b.cooldown = min(2*b.cooldown, maxCooldown)
	}
	b.openUntil = now.Add(b.cooldown)
	return false, wasUp
}
