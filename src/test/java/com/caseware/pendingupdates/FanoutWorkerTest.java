package com.caseware.pendingupdates;

import com.caseware.pendingupdates.concurrency.DownstreamBulkhead;
import com.caseware.pendingupdates.model.EngagementRef;
import com.caseware.pendingupdates.model.Outcome;
import com.caseware.pendingupdates.model.TemplatePublishEvent;
import com.caseware.pendingupdates.ports.DownstreamException;
import com.caseware.pendingupdates.ports.EngagementSource;
import com.caseware.pendingupdates.ports.EngagementUpdateClient;
import com.caseware.pendingupdates.retry.RetryPolicy;
import com.caseware.pendingupdates.worker.InMemoryIdempotencyLedger;
import com.caseware.pendingupdates.worker.TemplatePublishFanoutWorker;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class FanoutWorkerTest {

    private static TemplatePublishEvent event() {
        return new TemplatePublishEvent("pub-1", "tmpl-A", "v2", "US", List.of("v1"), false, Instant.now());
    }

    private static List<EngagementRef> engagements(int n) {
        List<EngagementRef> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            out.add(new EngagementRef("eng-" + i, "firm-1", "US", "US", "tmpl-A", "v1", Set.of()));
        }
        return out;
    }

    @Test
    void processesAllEngagementsAcrossPages() {
        List<EngagementRef> all = engagements(25);
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(8), RetryPolicy.defaults(), 10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(25, res.processed());
        assertEquals(25, res.applied());
        assertEquals(25, calls.get());
    }

    @Test
    void respectsDownstreamCapacity() throws Exception {
        List<EngagementRef> all = engagements(20);
        EngagementSource source = pagedSource(all, 20);
        AtomicInteger maxConcurrent = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        EngagementUpdateClient client = (eng, pending) -> {
            int c = current.incrementAndGet();
            maxConcurrent.updateAndGet(m -> Math.max(m, c));
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DownstreamException("interrupted", true, e);
            }
            current.decrementAndGet();
            return Outcome.APPLIED;
        };

        int cap = 4;
        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(cap), RetryPolicy.defaults(), 20);

        // Run in a thread so we can release after observing cap.
        Thread t = new Thread(() -> worker.process(event()));
        t.start();
        Thread.sleep(200);
        // At most `cap` should be in flight.
        assertTrue(current.get() <= cap, "in-flight " + current.get() + " > cap " + cap);
        release.countDown();
        t.join(10_000);
        worker.shutdown();
        assertTrue(maxConcurrent.get() <= cap, "max concurrent " + maxConcurrent.get() + " > cap " + cap);
    }

    @Test
    void retriesTransientFailures() {
        List<EngagementRef> all = engagements(3);
        EngagementSource source = pagedSource(all, 10);
        ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        EngagementUpdateClient client = (eng, pending) -> {
            int a = attempts.computeIfAbsent(eng.engagementId(), k -> new AtomicInteger()).incrementAndGet();
            if (a == 1) throw new DownstreamException("transient", true);
            return Outcome.APPLIED;
        };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(3, res.processed());
        assertEquals(3, res.applied());
        assertEquals(0, res.failed());
    }

    @Test
    void idempotentOnRedelivery() {
        List<EngagementRef> all = engagements(5);
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };

        var ledger = new InMemoryIdempotencyLedger();
        var worker1 = new TemplatePublishFanoutWorker(source, client, ledger, new DownstreamBulkhead(4),
                RetryPolicy.defaults(), 10);
        worker1.process(event());
        worker1.shutdown();

        // Redeliver same publish: source returns same engagements, but ledger blocks reprocessing.
        var worker2 = new TemplatePublishFanoutWorker(source, client, ledger, new DownstreamBulkhead(4),
                RetryPolicy.defaults(), 10);
        var res2 = worker2.process(event());
        worker2.shutdown();

        assertEquals(5, res2.processed());
        assertEquals(5, res2.skipped());
        assertEquals(0, res2.applied());
        // downstream not called again
        assertEquals(5, calls.get());
    }

    @Test
    void recordsPartialFailures() {
        List<EngagementRef> all = engagements(4);
        EngagementSource source = pagedSource(all, 10);
        EngagementUpdateClient client = (eng, pending) -> {
            if (eng.engagementId().equals("eng-1")) throw new DownstreamException("boom", false);
            return Outcome.APPLIED;
        };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), new RetryPolicy(1, java.time.Duration.ofMillis(1), 1.0, java.time.Duration.ofMillis(1)),
                10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(4, res.processed());
        assertEquals(3, res.applied());
        assertEquals(1, res.failed());
        assertTrue(res.failedEngagementIds().contains("eng-1"));
    }

    @Test
    void skipsDeclinedCandidate() {
        List<EngagementRef> all = List.of(
                new EngagementRef("eng-d", "firm-1", "US", "US", "tmpl-A", "v1", Set.of("v2"))
        );
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(1, res.processed());
        assertEquals(1, res.blocked());
        assertEquals(0, calls.get());
    }

    @Test
    void skipsWhenAlreadyOnVersion() {
        List<EngagementRef> all = List.of(
                new EngagementRef("eng-s", "firm-1", "US", "US", "tmpl-A", "v2", Set.of())
        );
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(1, res.processed());
        assertEquals(1, res.skipped());
        assertEquals(0, calls.get());
    }

    @Test
    void ignoresPublishForOtherMarket() {
        List<EngagementRef> all = List.of(
                new EngagementRef("eng-eu", "firm-eu", "EU", "EU", "tmpl-A", "v1", Set.of())
        );
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(event());
        worker.shutdown();

        assertEquals(1, res.processed());
        assertEquals(1, res.skipped());
        assertEquals(0, calls.get());
    }

    @Test
    void withdrawalClearsOnlyTheMatchingPendingVersion() {
        Instant earlier = Instant.parse("2026-01-01T00:00:00Z");
        Instant later = Instant.parse("2026-01-02T00:00:00Z");
        List<EngagementRef> all = List.of(
                new EngagementRef("eng-v2", "firm-1", "US", "US", "tmpl-A", "v1", null,
                        "v2", earlier, Set.of()),
                new EngagementRef("eng-v3", "firm-1", "US", "US", "tmpl-A", "v1", null,
                        "v3", later, Set.of())
        );
        EngagementSource source = pagedSource(all, 10);
        List<String> cleared = new ArrayList<>();
        EngagementUpdateClient client = (eng, pending) -> {
            if (pending.candidateVersion() == null) cleared.add(eng.engagementId());
            return Outcome.APPLIED;
        };
        TemplatePublishEvent withdrawal = new TemplatePublishEvent("pub-w", "tmpl-A", "v2", "US",
                List.of("v1"), true, Instant.parse("2026-01-03T00:00:00Z"));

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(withdrawal);
        worker.shutdown();

        assertEquals(2, res.processed());
        assertEquals(1, res.applied());
        assertEquals(1, res.skipped());
        assertEquals(List.of("eng-v2"), cleared);
    }

    @Test
    void stalePublishDoesNotDowngradeNewerPending() {
        Instant pendingAt = Instant.parse("2026-01-10T00:00:00Z");
        List<EngagementRef> all = List.of(
                new EngagementRef("eng-1", "firm-1", "US", "US", "tmpl-A", "v1", null,
                        "v9", pendingAt, Set.of())
        );
        EngagementSource source = pagedSource(all, 10);
        AtomicInteger calls = new AtomicInteger();
        EngagementUpdateClient client = (eng, pending) -> { calls.incrementAndGet(); return Outcome.APPLIED; };
        TemplatePublishEvent older = new TemplatePublishEvent("pub-old", "tmpl-A", "v2", "US",
                List.of("v1"), false, Instant.parse("2026-01-05T00:00:00Z"));

        var worker = new TemplatePublishFanoutWorker(source, client, new InMemoryIdempotencyLedger(),
                new DownstreamBulkhead(4), RetryPolicy.defaults(), 10);
        var res = worker.process(older);
        worker.shutdown();

        assertEquals(1, res.processed());
        assertEquals(1, res.skipped());
        assertEquals(0, calls.get());
    }

    private static EngagementSource pagedSource(List<EngagementRef> all, int pageSize) {
        return new EngagementSource() {
            @Override
            public Page findEngagements(String templateId, String market, String cursor, int ps) {
                int start = (cursor == null) ? 0 : Integer.parseInt(cursor);
                int end = Math.min(start + ps, all.size());
                if (start >= all.size()) return new Page(List.of(), null);
                List<EngagementRef> slice = new ArrayList<>(all.subList(start, end));
                String next = (end >= all.size()) ? null : String.valueOf(end);
                return new Page(slice, next);
            }
        };
    }
}
