package com.caseware.pendingupdates.model;

import java.time.Instant;
import java.util.Set;

/**
 * Lightweight reference to an engagement. The fan-out worker never loads the engagement
 * itself (~1 min); it only needs identity, market and the version state kept in the
 * regional registry.
 *
 * pendingVersion is the version currently offered to the user (nullable). It is needed
 * for two correctness rules:
 *  - a withdrawal must only clear the pending state it actually refers to;
 *  - an older/out-of-order publish must not overwrite a newer pending candidate.
 */
public record EngagementRef(
        String engagementId,
        String firmId,
        String region,            // data-residency region, e.g. EU, CA, US
        String market,            // firm's market branch, e.g. US, EU
        String templateId,
        String baseVersion,       // version the engagement currently uses
        Instant basePublishedAt,  // when baseVersion was published (nullable for legacy)
        String pendingVersion,    // version currently offered, or null
        Instant pendingUpdatedAt, // when pendingVersion was last set
        Set<String> declinedVersions
) {
    public EngagementRef {
        declinedVersions = declinedVersions == null ? Set.of() : Set.copyOf(declinedVersions);
    }

    /** Convenience for tests/legacy rows without pending state or timestamps. */
    public EngagementRef(String engagementId, String firmId, String region, String market,
                         String templateId, String baseVersion, Set<String> declinedVersions) {
        this(engagementId, firmId, region, market, templateId, baseVersion, null, null, null, declinedVersions);
    }
}
