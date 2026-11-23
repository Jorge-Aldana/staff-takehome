package com.caseware.pendingupdates.ports;

/**
 * Contract for downstream failures. The client must say whether the failure is
 * retriable (timeout, throttling, 5xx) or terminal (validation, 4xx). Retrying a
 * programming error or a rejected payload just amplifies load.
 */
public class DownstreamException extends Exception {
    private final boolean retriable;

    public DownstreamException(String message, boolean retriable) {
        super(message);
        this.retriable = retriable;
    }

    public DownstreamException(String message, boolean retriable, Throwable cause) {
        super(message, cause);
        this.retriable = retriable;
    }

    public boolean isRetriable() {
        return retriable;
    }
}
