package com.caseware.pendingupdates.model;

public enum Outcome {
    IN_PROGRESS,      // claimed, not yet finished (ledger-only)
    APPLIED,          // downstream accepted the update
    SKIPPED,          // already processed (idempotency)
    FAILED,           // downstream error after retries
    DECLINED_BLOCKED  // engagement has explicitly declined this candidate
}
