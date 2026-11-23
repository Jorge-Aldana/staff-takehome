package com.caseware.pendingupdates.concurrency;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Caps in-flight calls to the downstream engagement system, which has limited capacity.
 * Shared across worker instances in production (e.g. backed by a distributed semaphore
 * or simply sized per-instance with a global capacity budget).
 */
public class DownstreamBulkhead {
    private final Semaphore semaphore;
    private final int maxPermits;

    public DownstreamBulkhead(int maxConcurrent) {
        if (maxConcurrent <= 0) throw new IllegalArgumentException("maxConcurrent must be > 0");
        this.maxPermits = maxConcurrent;
        this.semaphore = new Semaphore(maxConcurrent);
    }

    public int maxPermits() {
        return maxPermits;
    }

    public void acquire() throws InterruptedException {
        semaphore.acquire();
    }

    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return semaphore.tryAcquire(timeout, unit);
    }

    public void release() {
        semaphore.release();
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }
}
