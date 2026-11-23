package com.caseware.pendingupdates.model;

/**
 * Per-engagement pending update instruction handed to the downstream engagement system.
 * candidateVersion == null means "clear pending" (e.g. version withdrawn).
 */
public record PendingUpdate(
        String templateId,
        String baseVersion,
        String candidateVersion,
        String summaryKey,        // cache key for the human-readable summary
        boolean withdrawn
) {}
