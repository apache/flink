package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;

import java.io.Serializable;
import java.util.Deque;

/**
 * A pluggable interface for forming batches of request entries from a buffer. Implementations
 * control how many entries are grouped together and in what manner before sending them downstream.
 *
 * <p>The {@code AsyncSinkWriter} (or similar sink component) calls {@link
 * #createNextBatch(RequestInfo, BufferWrapper)} (RequestInfo, Deque)} when it decides to flush or
 * otherwise gather a new batch of elements. For instance, a batch creator might limit the batch by
 * the number of elements, total payload size, or any custom partition-based strategy.
 *
 * @param <RequestEntryT> the type of the request entries to be batched
 */
@PublicEvolving
public interface BatchCreator<RequestEntryT extends Serializable> {

    /**
     * Creates the next batch of request entries from the current buffer.
     *
     * <p>This method is typically invoked when the sink determines that it's time to flush—e.g.,
     * based on rate limiting, a buffer-size threshold, or a time-based trigger. The implementation
     * can select as many entries from {@code bufferedRequestEntries} as it deems appropriate for a
     * single batch, subject to any internal constraints (for example, a maximum byte size).
     *
     * @param requestInfo information about the desired request properties or constraints (e.g., an
     *     allowed batch size or other relevant hints)
     * @param bufferedRequestEntries a {@link Deque} of all currently buffered entries waiting to be
     *     grouped into batches
     * @return a {@link Batch} containing the new batch of entries along with metadata about the
     *     batch (e.g., total byte size, record count)
     */
    Batch<RequestEntryT> createNextBatch(
            RequestInfo requestInfo, BufferWrapper<RequestEntryT> bufferedRequestEntries);

    /**
     * Generic builder interface for creating instances of {@link BatchCreator}.
     *
     * @param <R> The type of {@link BatchCreator} that the builder will create.
     * @param <RequestEntryT> The type of request entries that the batch creator will process.
     */
    interface Builder<R extends BatchCreator<RequestEntryT>, RequestEntryT extends Serializable> {
        /**
         * Constructs and returns an instance of {@link BatchCreator} with the configured
         * parameters.
         *
         * @return A new instance of {@link BatchCreator}.
         */
        R build();
    }
}
