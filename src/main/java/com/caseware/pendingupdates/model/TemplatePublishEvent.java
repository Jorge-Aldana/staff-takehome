package com.caseware.pendingupdates.model;

import java.time.Instant;
import java.util.List;

/**
 * Event emitted (via transactional outbox) when a product template version is published.
 * Carries no firm-specific data: templates are shared across firms.
 */
public record TemplatePublishEvent(
        String publishId,          // idempotency key for the whole publish
        String templateId,
        String versionId,         // candidate version being published
        String market,            // market/branch this version targets
        List<String> parentVersions,
        boolean withdrawn,        // true if this event withdraws a version
        Instant publishedAt
) {
    public TemplatePublishEvent {
        if (publishId == null || publishId.isBlank()) throw new IllegalArgumentException("publishId required");
        if (templateId == null || templateId.isBlank()) throw new IllegalArgumentException("templateId required");
        if (versionId == null || versionId.isBlank()) throw new IllegalArgumentException("versionId required");
        if (publishedAt == null) throw new IllegalArgumentException("publishedAt required");
        parentVersions = parentVersions == null ? List.of() : List.copyOf(parentVersions);
    }
}
