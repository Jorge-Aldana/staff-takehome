# Template Publish Fan-Out Worker (Part 2)

Narrow component for the Staff take-home: processes a `TemplatePublishEvent` by
fan-ing out to every engagement that references the published template, updating
each engagement's pending-update state via a downstream engagement-management
system.

## Build & test

```bash
mvn test
```

Java 17+ and Maven 3.8+.

## Diagrams

`diagrams/architecture.drawio` is a four-page draw.io file:
high-level architecture, fan-out control flow, version DAG / declined
semantics, and regional deployment / data residency. See
`diagrams/README.md` for details.

## Key design points

- **Bounded downstream capacity**: `DownstreamBulkhead` (semaphore) caps in-flight calls
  to the ~1 min downstream system. The worker processes page-by-page so memory and
  in-flight work stay bounded.
- **Concurrency**: a fixed thread pool sized to the bulkhead capacity drives
  `CompletableFuture` per engagement.
- **Idempotency**: `IdempotencyLedger` claims `(publishId, engagementId)`; redelivered
  events skip already-claimed items. Failed items can be reclaimed for retry.
- **Failure handling**: per-engagement retry with exponential backoff; retriable vs
  non-retriable classification; partial failures are recorded (not fatal to the job)
  and returned in `FanoutResult.failedEngagementIds` for replay/DLQ.
- **Declined / already-current**: engagements that declined the exact candidate, or
  already use it, are short-circuited without a downstream call.
- **Withdrawn versions**: a withdrawn publish clears pending state **only** when the
  engagement's `pendingVersion` equals the withdrawn version (`candidate = null`).
- **Out-of-order events**: `basePublishedAt` / `pendingUpdatedAt` guards prevent an
  older publish from overwriting a newer pending offer; withdrawals are guarded too.
- **Market isolation**: a publish for one market does not touch engagements in another.

## Production notes (out of scope of this slice)

- Replace `InMemoryIdempotencyLedger` with a DB table keyed on
  `(publish_id, engagement_id)` with a unique constraint and a **lease/expiry** so a
  crashed worker's `IN_PROGRESS` claim can be reclaimed.
- Checkpoint the page cursor in a `fanout_job` table to resume after a worker crash
  without re-scanning completed engagements.
- Shard work: the publish consumer should enqueue per-page/per-firm work items rather
  than hold one queue message for the whole (potentially 6.7 h) fan-out.
- Add a distributed bulkhead / rate limiter when multiple worker instances share one
  downstream capacity budget. Size it against aggregate demand: 800k calls/week
  (40 publishes × 20k engagements) needs ~80 concurrent at 1 min/call just to break
  even; 50 concurrent is a 37% shortfall. Negotiate ~100 or a bulk endpoint.
- DLQ + reconciliation job to replay failed engagements and to catch missed events
  (transactional outbox on the template DB).
- Regional workers for EU/CA data residency; engagement metadata never leaves region.

## Tradeoffs

- At-least-once delivery with idempotency rather than exactly-once (simpler, robust).
- Page-by-page processing favours bounded resources over maximum throughput.
- In-memory ledger keeps the slice testable; production needs durable state.
