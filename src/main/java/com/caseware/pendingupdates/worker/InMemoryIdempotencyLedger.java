package com.caseware.pendingupdates.worker;

import com.caseware.pendingupdates.ports.IdempotencyLedger;
import com.caseware.pendingupdates.model.Outcome;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryIdempotencyLedger implements IdempotencyLedger {
    private final Map<String, Outcome> state = new ConcurrentHashMap<>();

    private static String key(String publishId, String engagementId) {
        return publishId + "|" + engagementId;
    }

    @Override
    public boolean tryClaim(String publishId, String engagementId) {
        String k = key(publishId, engagementId);
        while (true) {
            Outcome cur = state.get(k);
            if (cur == null) {
                if (state.putIfAbsent(k, Outcome.IN_PROGRESS) == null) return true;
            } else if (cur == Outcome.FAILED) {
                if (state.replace(k, Outcome.FAILED, Outcome.IN_PROGRESS)) return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void complete(String publishId, String engagementId, Outcome outcome) {
        state.put(key(publishId, engagementId), outcome);
    }

    @Override
    public Optional<Outcome> status(String publishId, String engagementId) {
        return Optional.ofNullable(state.get(key(publishId, engagementId)));
    }
}
