package com.caseware.pendingupdates.ports;

import com.caseware.pendingupdates.model.Outcome;

import java.util.Optional;

/**
 * Tracks per-(publish, engagement) processing state so redelivered events do not
 * re-invoke the downstream system. Production backing: a table with a unique
 * constraint on (publish_id, engagement_id).
 */
public interface IdempotencyLedger {
    /** Returns true if this worker instance successfully claimed the work item. */
    boolean tryClaim(String publishId, String engagementId);

    /** Mark a claimed item complete (success or final failure). */
    void complete(String publishId, String engagementId, Outcome outcome);

    Optional<Outcome> status(String publishId, String engagementId);
}
