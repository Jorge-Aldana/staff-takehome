package com.caseware.pendingupdates.ports;

import com.caseware.pendingupdates.model.EngagementRef;

import java.util.List;

public interface EngagementSource {
    /**
     * Page engagements that reference the given template and market. Cursor-based to keep
     * memory bounded and to allow resumption after partial failure.
     *
     * <p>Production implementations should additionally filter by version ancestry (candidate
     * reachable from base) so a publish does not fan out to unrelated branches; the worker
     * re-checks market/declined/current-version defensively.</p>
     */
    Page findEngagements(String templateId, String market, String cursor, int pageSize);

    record Page(List<EngagementRef> items, String nextCursor) {
        public boolean isEmpty() { return items == null || items.isEmpty(); }
    }
}
