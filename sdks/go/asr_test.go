package asr

import (
	"testing"
	"time"
)

func TestBackoffCap(t *testing.T) {
	base := time.Second
	max := 32 * time.Second
	for attempt := 1; attempt <= 12; attempt++ {
		d := backoff(attempt, base, max)
		// Allow for +/-20% jitter beyond the cap.
		if d > max+max/5 {
			t.Fatalf("attempt %d: backoff %s exceeds cap+jitter", attempt, d)
		}
		if d <= 0 {
			t.Fatalf("attempt %d: backoff must be positive, got %s", attempt, d)
		}
	}
}

func TestBackoffGrows(t *testing.T) {
	base := time.Second
	max := time.Hour // effectively uncapped for this range
	// Compare center values (strip jitter by using odd attempts only, +jitter).
	prev := backoff(1, base, max)
	for attempt := 2; attempt <= 6; attempt++ {
		d := backoff(attempt, base, max)
		if d < prev/2 {
			t.Fatalf("attempt %d backoff %s not growing vs prev %s", attempt, d, prev)
		}
		prev = d
	}
}
