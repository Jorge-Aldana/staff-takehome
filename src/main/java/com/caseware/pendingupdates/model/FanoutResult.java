package com.caseware.pendingupdates.model;

import java.util.List;

public record FanoutResult(
        String publishId,
        int processed,
        int applied,
        int skipped,
        int failed,
        int blocked,
        List<String> failedEngagementIds
) {}
