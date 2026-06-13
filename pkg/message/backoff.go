package message

import (
	"math"
	"math/rand"
	"time"
)

// BackoffStrategy defines how to calculate backoff delays
type BackoffStrategy interface {
	NextDelay(attempt int) time.Duration
	Reset()
}

// ExponentialBackoff implements exponential backoff with jitter
type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    float64 // 0.0 to 1.0
}

// NewExponentialBackoff creates a new exponential backoff strategy
func NewExponentialBackoff(baseDelay, maxDelay time.Duration) *ExponentialBackoff {
	return &ExponentialBackoff{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Jitter:    0.3, // 30% jitter by default
	}
}

// NextDelay calculates the next backoff delay
func (e *ExponentialBackoff) NextDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}

	// Calculate exponential delay: baseDelay * 2^attempt
	delay := float64(e.BaseDelay) * math.Pow(2, float64(attempt))

	// Cap at max delay
	if delay > float64(e.MaxDelay) {
		delay = float64(e.MaxDelay)
	}

	// Add jitter: random value between (1-jitter)*delay and (1+jitter)*delay
	jitterRange := delay * e.Jitter
	jitter := (rand.Float64() * 2 * jitterRange) - jitterRange

	finalDelay := time.Duration(delay + jitter)

	// Ensure we don't go below base delay or above max delay
	if finalDelay < e.BaseDelay {
		finalDelay = e.BaseDelay
	}
	if finalDelay > e.MaxDelay {
		finalDelay = e.MaxDelay
	}

	return finalDelay
}

// Reset resets the backoff state (for ExponentialBackoff, this is a no-op)
func (e *ExponentialBackoff) Reset() {
	// Nothing to reset for stateless backoff
}
