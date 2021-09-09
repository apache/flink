package org.apache.flink.connector.base.sink.writer;

import java.util.Collection;

/**
 * The entire request may fail or single request entries that are part of the request may not be
 * persisted successfully, eg, because of network issues or service side throttling. All request
 * entries that failed with transient failures need to be re-queued with this method so that aren't
 * lost and can be retried later.
 *
 * <p>Request entries that are causing the same error in a reproducible manner, eg, ill-formed
 * request entries, must not be re-queued but the error needs to be handled in the logic of {@code
 * submitRequestEntries}. Otherwise these request entries will be retried indefinitely, always
 * causing the same error.
 *
 * @param <RequestEntryT>
 */
public interface ResultFuture<RequestEntryT> {
    /**
     * Completes the result future.
     *
     * <p>The result future must only be completed when the request sent to the endpoint completed
     * (sucessfully or unsuccessfully). Request entries that were not persisted successfully must be
     * included in the {@code failedRequestEntries} parameter, so that they can be retried later.
     *
     * @param failedRequestEntries Request entries that need to be retried at a later point
     */
    void complete(Collection<RequestEntryT> failedRequestEntries);
}
