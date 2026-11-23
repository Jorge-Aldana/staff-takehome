package com.caseware.pendingupdates.worker;

import com.caseware.pendingupdates.concurrency.DownstreamBulkhead;
import com.caseware.pendingupdates.model.EngagementRef;
import com.caseware.pendingupdates.model.FanoutResult;
import com.caseware.pendingupdates.model.Outcome;
import com.caseware.pendingupdates.model.PendingUpdate;
import com.caseware.pendingupdates.model.TemplatePublishEvent;
import com.caseware.pendingupdates.ports.DownstreamException;
import com.caseware.pendingupdates.ports.EngagementSource;
import com.caseware.pendingupdates.ports.EngagementUpdateClient;
import com.caseware.pendingupdates.ports.IdempotencyLedger;
import com.caseware.pendingupdates.retry.RetryPolicy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TemplatePublishFanoutWorker {

    private static final System.Logger LOG = System.getLogger(TemplatePublishFanoutWorker.class.getName());

    private final EngagementSource source;
    private final EngagementUpdateClient client;
    private final IdempotencyLedger ledger;
    private final DownstreamBulkhead bulkhead;
    private final RetryPolicy retryPolicy;
    private final int pageSize;
    private final ExecutorService executor;

    public TemplatePublishFanoutWorker(EngagementSource source,
                                      EngagementUpdateClient client,
                                      IdempotencyLedger ledger,
                                      DownstreamBulkhead bulkhead,
                                      RetryPolicy retryPolicy,
                                      int pageSize) {
        this.source = source;
        this.client = client;
        this.ledger = ledger;
        this.bulkhead = bulkhead;
        this.retryPolicy = retryPolicy;
        this.pageSize = pageSize <= 0 ? 100 : pageSize;
        this.executor = Executors.newFixedThreadPool(bulkhead.maxPermits());
    }

    public FanoutResult process(TemplatePublishEvent event) {
        int processed = 0, applied = 0, skipped = 0, failed = 0, blocked = 0;
        List<String> failedIds = new ArrayList<>();

        String cursor = null;
        while (true) {
            EngagementSource.Page page = source.findEngagements(event.templateId(), event.market(), cursor, pageSize);
            if (page == null || page.isEmpty()) break;

            List<PendingFuture> futures = new ArrayList<>();
            for (EngagementRef e : page.items()) {
                futures.add(new PendingFuture(e,
                        CompletableFuture.supplyAsync(() -> processOne(event, e), executor)));
            }
            for (PendingFuture pf : futures) {
                EngagementOutcome o;
                try {
                    o = pf.future.join();
                } catch (CompletionException ce) {
                    // Defensive: a bug in one item must not abort the whole publish.
                    LOG.log(System.Logger.Level.ERROR, "Unhandled fan-out failure for engagement "
                            + pf.engagement.engagementId() + " publish " + event.publishId(), ce);
                    processed++;
                    failed++;
                    failedIds.add(pf.engagement.engagementId());
                    continue;
                }
                processed++;
                switch (o.outcome) {
                    case APPLIED -> applied++;
                    case SKIPPED -> skipped++;
                    case FAILED -> { failed++; failedIds.add(o.engagementId); }
                    case DECLINED_BLOCKED -> blocked++;
                    case IN_PROGRESS -> skipped++;
                }
            }

            cursor = page.nextCursor();
            if (cursor == null || cursor.isBlank()) break;
        }

        return new FanoutResult(event.publishId(), processed, applied, skipped, failed, blocked, failedIds);
    }

    private EngagementOutcome processOne(TemplatePublishEvent event, EngagementRef engagement) {
        String candidate = event.withdrawn() ? null : event.versionId();

        // Idempotency claim. Already-claimed (non-failed) items are skipped on redelivery.
        if (!ledger.tryClaim(event.publishId(), engagement.engagementId())) {
            return new EngagementOutcome(engagement.engagementId(), Outcome.SKIPPED);
        }

        try {
            // Market/branch applicability: a US publish must not touch EU branches.
            if (event.market() != null && engagement.market() != null
                    && !event.market().equalsIgnoreCase(engagement.market())) {
                return finish(event, engagement, Outcome.SKIPPED);
            }

            // Ignore events older than the version the engagement already uses.
            if (engagement.basePublishedAt() != null
                    && event.publishedAt().isBefore(engagement.basePublishedAt())) {
                return finish(event, engagement, Outcome.SKIPPED);
            }

            if (event.withdrawn()) {
                // A withdrawal only clears the pending state it actually refers to.
                if (!event.versionId().equals(engagement.pendingVersion())) {
                    return finish(event, engagement, Outcome.SKIPPED);
                }
                // Do not let a stale withdrawal erase a newer pending update.
                if (engagement.pendingUpdatedAt() != null
                        && event.publishedAt().isBefore(engagement.pendingUpdatedAt())) {
                    return finish(event, engagement, Outcome.SKIPPED);
                }
            } else {
                // Out-of-order delivery: never downgrade a newer pending candidate.
                if (engagement.pendingUpdatedAt() != null
                        && event.publishedAt().isBefore(engagement.pendingUpdatedAt())) {
                    return finish(event, engagement, Outcome.SKIPPED);
                }
                // Already on this version: no pending update.
                if (candidate.equals(engagement.baseVersion())) {
                    return finish(event, engagement, Outcome.SKIPPED);
                }
                // The user explicitly declined this exact candidate.
                if (engagement.declinedVersions().contains(candidate)) {
                    return finish(event, engagement, Outcome.DECLINED_BLOCKED);
                }
            }

            PendingUpdate pending = new PendingUpdate(
                    event.templateId(),
                    engagement.baseVersion(),
                    candidate,
                    summaryKey(event, engagement),
                    event.withdrawn()
            );

            Outcome outcome = callDownstream(event, engagement, pending);
            return new EngagementOutcome(engagement.engagementId(), outcome);
        } catch (RuntimeException ex) {
            LOG.log(System.Logger.Level.ERROR, "Unexpected error for engagement "
                    + engagement.engagementId() + " publish " + event.publishId(), ex);
            safeComplete(event, engagement, Outcome.FAILED);
            return new EngagementOutcome(engagement.engagementId(), Outcome.FAILED);
        }
    }

    private Outcome callDownstream(TemplatePublishEvent event, EngagementRef engagement, PendingUpdate pending) {
        Outcome result = null;
        int attempts = 0;

        while (attempts < retryPolicy.maxAttempts()) {
            attempts++;
            try {
                bulkhead.acquire();
                try {
                    result = client.updatePending(engagement, pending);
                } finally {
                    bulkhead.release();
                }
                if (result == null) {
                    throw new IllegalStateException("downstream returned null outcome");
                }
                break;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.log(System.Logger.Level.WARNING, "Interrupted while waiting for downstream capacity, engagement "
                        + engagement.engagementId());
                break;
            } catch (DownstreamException ex) {
                if (!retryPolicy.isRetriable(ex) || attempts >= retryPolicy.maxAttempts()) {
                    LOG.log(System.Logger.Level.WARNING, "Downstream failed for engagement "
                            + engagement.engagementId() + " after " + attempts + " attempt(s)", ex);
                    break;
                }
                sleep(backoff(attempts));
            }
        }

        Outcome finalOutcome = result == null ? Outcome.FAILED : result;
        safeComplete(event, engagement, finalOutcome);
        return finalOutcome;
    }

    private EngagementOutcome finish(TemplatePublishEvent event, EngagementRef engagement, Outcome outcome) {
        safeComplete(event, engagement, outcome);
        return new EngagementOutcome(engagement.engagementId(), outcome);
    }

    private void safeComplete(TemplatePublishEvent event, EngagementRef engagement, Outcome outcome) {
        try {
            ledger.complete(event.publishId(), engagement.engagementId(), outcome);
        } catch (RuntimeException ex) {
            LOG.log(System.Logger.Level.ERROR, "Could not persist idempotency outcome for engagement "
                    + engagement.engagementId() + " publish " + event.publishId(), ex);
        }
    }

    private String summaryKey(TemplatePublishEvent event, EngagementRef engagement) {
        return event.templateId() + ":" + engagement.baseVersion() + "->"
                + (event.withdrawn() ? "withdrawn" : event.versionId());
    }

    private Duration backoff(int attempt) {
        long delay = (long) (retryPolicy.initialDelay().toMillis() * Math.pow(retryPolicy.multiplier(), attempt - 1));
        long capped = Math.min(delay, retryPolicy.maxDelay().toMillis());
        return Duration.ofMillis(capped);
    }

    private void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    private record PendingFuture(EngagementRef engagement, CompletableFuture<EngagementOutcome> future) {}

    private record EngagementOutcome(String engagementId, Outcome outcome) {}
}
