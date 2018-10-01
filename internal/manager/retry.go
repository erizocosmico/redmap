package manager

import (
	"time"

	"github.com/cenkalti/backoff"
)

// Retry retries a function until the timeout is reached using an exponential
// backoff.
func Retry(timeout time.Duration, fn func() error) error {
	b := &backoff.ExponentialBackOff{
		InitialInterval:     10 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          backoff.DefaultMultiplier,
		MaxInterval:         timeout / 6,
		MaxElapsedTime:      timeout,
		Clock:               backoff.SystemClock,
	}
	b.Reset()
	return backoff.Retry(fn, b)
}
