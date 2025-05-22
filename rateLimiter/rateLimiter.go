package ratelimiter

import (
	"sync"
	"time"
)

type RateLimiter struct {
	tokens     chan struct{}
	interval   time.Duration
	maxTokens  int
	tokenCount int
	mu         sync.Mutex
}

func NewRateLimiter(rps int) *RateLimiter {
	maxTokens := rps
	rl := &RateLimiter{
		tokens:     make(chan struct{}, maxTokens),
		interval:   time.Second / time.Duration(rps),
		maxTokens:  maxTokens,
		tokenCount: maxTokens,
	}

	for range maxTokens {
		rl.tokens <- struct{}{}
	}

	go rl.refillTokens()

	return rl
}

func (rl *RateLimiter) refillTokens() {
	ticker := time.NewTicker(rl.interval)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		if rl.tokenCount < rl.maxTokens {
			select {
			case rl.tokens <- struct{}{}:
				rl.tokenCount++
			default:
				// Bucket is full
			}
		}
		rl.mu.Unlock()
	}
}

func (rl *RateLimiter) Wait() {
	<-rl.tokens
	rl.mu.Lock()
	rl.tokenCount--
	rl.mu.Unlock()
}
