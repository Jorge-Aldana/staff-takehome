 package com.caseware.pendingupdates.ports;

import com.caseware.pendingupdates.model.EngagementRef;
import com.caseware.pendingupdates.model.PendingUpdate;
import com.caseware.pendingupdates.model.Outcome;

/**
 * Downstream engagement-management system. ~1 minute per call, owned by another team
 * with limited capacity. Implementations must be thread-safe.
 */
public interface EngagementUpdateClient {
    Outcome updatePending(EngagementRef engagement, PendingUpdate pending) throws DownstreamException;
}
