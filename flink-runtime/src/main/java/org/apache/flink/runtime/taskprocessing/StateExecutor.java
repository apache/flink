package org.apache.flink.runtime.taskprocessing;

import java.util.concurrent.CompletableFuture;

/**
 * Executor for executing batch {@link ProcessingRequest}s.
 *
 * @param <K> the type of key.
 */
public interface StateExecutor {
    /**
     * Execute a batch of state requests.
     *
     * @param processingRequests the given batch of processing requests
     * @return A future can determine whether execution has completed.
     */
    CompletableFuture<Boolean> executeBatchRequests(
            Iterable<ProcessingRequest<?>> processingRequests);
}
