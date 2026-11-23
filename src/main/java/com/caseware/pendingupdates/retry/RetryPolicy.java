package com.caseware.pendingupdates.retry;

import com.caseware.pendingupdates.ports.DownstreamException;

import java.time.Duration;

/**
 * Simple exponential backoff retry policy.
 */
public record RetryPolicy(int maxAttempts, Duration initialDelay, double multiplier, Duration maxDelay) {
    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts >= 1");
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(4, Duration.ofMillis(50), 2.0, Duration.ofSeconds(2));
    }

    public boolean isRetriable(Throwable t) {
        return t instanceof DownstreamException de && de.isRetriable();
    }
}
