/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.connector.base.sink.writer.strategy.BasicRequestInfo;
import org.apache.flink.connector.base.sink.writer.strategy.BasicResultInfo;
import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Consumer;

/**
 * A generic sink writer that handles the general behaviour of a sink such as batching and flushing,
 * and allows extenders to implement the logic for persisting individual request elements, with
 * allowance for retries.
 *
 * <p>At least once semantics is supported through {@code prepareCommit} as outstanding requests are
 * flushed or completed prior to checkpointing.
 *
 * <p>Designed to be returned at {@code createWriter} time by an {@code AsyncSinkBase}.
 *
 * <p>There are configuration options to customize the buffer size etc.
 */
@PublicEvolving
public abstract class AsyncSinkWriter<InputT, RequestEntryT extends Serializable>
        implements StatefulSink.StatefulSinkWriter<InputT, BufferedRequestState<RequestEntryT>> {

    private final MailboxExecutor mailboxExecutor;
    private final ProcessingTimeService timeService;

    /* The timestamp of the previous batch of records was sent from this sink. */
    private long lastSendTimestamp = 0;

    /* The timestamp of the response to the previous request from this sink. */
    private long ackTime = Long.MAX_VALUE;

    /* The sink writer metric group. */
    private final SinkWriterMetricGroup metrics;

    /* Counter for number of bytes this sink has attempted to send to the destination. */
    private final Counter numBytesOutCounter;

    /* Counter for number of records this sink has attempted to send to the destination. */
    private final Counter numRecordsOutCounter;

    private final RateLimitingStrategy rateLimitingStrategy;

    private final int maxBatchSize;
    private final int maxBufferedRequests;
    private final long maxBatchSizeInBytes;
    private final long maxTimeInBufferMS;
    private final long maxRecordSizeInBytes;

    /**
     * The ElementConverter provides a mapping between for the elements of a stream to request
     * entries that can be sent to the destination.
     *
     * <p>The resulting request entry is buffered by the AsyncSinkWriter and sent to the destination
     * when the {@code submitRequestEntries} method is invoked.
     */
    private final ElementConverter<InputT, RequestEntryT> elementConverter;

    /**
     * Buffer to hold request entries that should be persisted into the destination, along with its
     * size in bytes.
     *
     * <p>A request entry contain all relevant details to make a call to the destination. Eg, for
     * Kinesis Data Streams a request entry contains the payload and partition key.
     *
     * <p>It seems more natural to buffer InputT, ie, the events that should be persisted, rather
     * than RequestEntryT. However, in practice, the response of a failed request call can make it
     * very hard, if not impossible, to reconstruct the original event. It is much easier, to just
     * construct a new (retry) request entry from the response and add that back to the queue for
     * later retry.
     */
    private final Deque<RequestEntryWrapper<RequestEntryT>> bufferedRequestEntries =
            new ArrayDeque<>();

    /**
     * Tracks all pending async calls that have been executed since the last checkpoint. Calls that
     * completed (successfully or unsuccessfully) are automatically decrementing the counter. Any
     * request entry that was not successfully persisted needs to be handled and retried by the
     * logic in {@code submitRequestsToApi}.
     *
     * <p>To complete a checkpoint, we need to make sure that no requests are in flight, as they may
     * fail, which could then lead to data loss.
     */
    private int inFlightRequestsCount;

    /**
     * Tracks the cumulative size of all elements in {@code bufferedRequestEntries} to facilitate
     * the criterion for flushing after {@code maxBatchSizeInBytes} is reached.
     */
    private double bufferedRequestEntriesTotalSizeInBytes;

    private boolean existsActiveTimerCallback = false;

    /**
     * The {@code accept} method should be called on this Consumer if the processing of the {@code
     * requestEntries} raises an exception that should not be retried. Specifically, any action that
     * we are sure will result in the same exception no matter how many times we retry should raise
     * a {@code RuntimeException} here. For example, wrong user credentials. However, it is possible
     * intermittent failures will recover, e.g. flaky network connections, in which case, some other
     * mechanism may be more appropriate.
     */
    private final Consumer<Exception> fatalExceptionCons;

    /**
     * This method specifies how to persist buffered request entries into the destination. It is
     * implemented when support for a new destination is added.
     *
     * <p>The method is invoked with a set of request entries according to the buffering hints (and
     * the valid limits of the destination). The logic then needs to create and execute the request
     * asynchronously against the destination (ideally by batching together multiple request entries
     * to increase efficiency). The logic also needs to identify individual request entries that
     * were not persisted successfully and resubmit them using the {@code requestToRetry} callback.
     *
     * <p>From a threading perspective, the mailbox thread will call this method and initiate the
     * asynchronous request to persist the {@code requestEntries}. NOTE: The client must support
     * asynchronous requests and the method called to persist the records must asynchronously
     * execute and return a future with the results of that request. A thread from the destination
     * client thread pool should complete the request and submit the failed entries that should be
     * retried. The {@code requestToRetry} will then trigger the mailbox thread to requeue the
     * unsuccessful elements.
     *
     * <p>An example implementation of this method is included:
     *
     * <pre>{@code
     * @Override
     * protected void submitRequestEntries
     *   (List<RequestEntryT> records, Consumer<Collection<RequestEntryT>> requestToRetry) {
     *     Future<Response> response = destinationClient.putRecords(records);
     *     response.whenComplete(
     *         (response, error) -> {
     *             if(error){
     *                 List<RequestEntryT> retryableFailedRecords = getRetryableFailed(response);
     *                 requestToRetry.accept(retryableFailedRecords);
     *             }else{
     *                 requestToRetry.accept(Collections.emptyList());
     *             }
     *         }
     *     );
     * }
     *
     * }</pre>
     *
     * <p>During checkpointing, the sink needs to ensure that there are no outstanding in-flight
     * requests.
     *
     * @param requestEntries a set of request entries that should be sent to the destination
     * @param requestToRetry the {@code accept} method should be called on this Consumer once the
     *     processing of the {@code requestEntries} are complete. Any entries that encountered
     *     difficulties in persisting should be re-queued through {@code requestToRetry} by
     *     including that element in the collection of {@code RequestEntryT}s passed to the {@code
     *     accept} method. All other elements are assumed to have been successfully persisted.
     */
    protected abstract void submitRequestEntries(
            List<RequestEntryT> requestEntries, Consumer<List<RequestEntryT>> requestToRetry);

    /**
     * This method allows the getting of the size of a {@code RequestEntryT} in bytes. The size in
     * this case is measured as the total bytes that is written to the destination as a result of
     * persisting this particular {@code RequestEntryT} rather than the serialized length (which may
     * be the same).
     *
     * @param requestEntry the requestEntry for which we want to know the size
     * @return the size of the requestEntry, as defined previously
     */
    protected abstract long getSizeInBytes(RequestEntryT requestEntry);

    /**
     * This method is deprecated, please use the constructor that specifies the {@link
     * AsyncSinkWriterConfiguration}.
     */
    @Deprecated
    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        this(
                elementConverter,
                context,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes,
                Collections.emptyList());
    }

    /**
     * This method is deprecated, please use the constructor that specifies the {@link
     * AsyncSinkWriterConfiguration}.
     */
    @Deprecated
    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            Sink.InitContext context,
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes,
            Collection<BufferedRequestState<RequestEntryT>> states) {
        this(
                elementConverter,
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(maxBatchSize)
                        .setMaxBatchSizeInBytes(maxBatchSizeInBytes)
                        .setMaxInFlightRequests(maxInFlightRequests)
                        .setMaxBufferedRequests(maxBufferedRequests)
                        .setMaxTimeInBufferMS(maxTimeInBufferMS)
                        .setMaxRecordSizeInBytes(maxRecordSizeInBytes)
                        .build(),
                states);
    }

    public AsyncSinkWriter(
            ElementConverter<InputT, RequestEntryT> elementConverter,
            Sink.InitContext context,
            AsyncSinkWriterConfiguration configuration,
            Collection<BufferedRequestState<RequestEntryT>> states) {
        this.elementConverter = elementConverter;
        this.mailboxExecutor = context.getMailboxExecutor();
        this.timeService = context.getProcessingTimeService();

        Preconditions.checkNotNull(elementConverter);
        Preconditions.checkArgument(configuration.getMaxBatchSize() > 0);
        Preconditions.checkArgument(configuration.getMaxBufferedRequests() > 0);
        Preconditions.checkArgument(configuration.getMaxBatchSizeInBytes() > 0);
        Preconditions.checkArgument(configuration.getMaxTimeInBufferMS() > 0);
        Preconditions.checkArgument(configuration.getMaxRecordSizeInBytes() > 0);
        Preconditions.checkArgument(
                configuration.getMaxBufferedRequests() > configuration.getMaxBatchSize(),
                "The maximum number of requests that may be buffered should be strictly"
                        + " greater than the maximum number of requests per batch.");
        Preconditions.checkArgument(
                configuration.getMaxBatchSizeInBytes() >= configuration.getMaxRecordSizeInBytes(),
                "The maximum allowed size in bytes per flush must be greater than or equal to the"
                        + " maximum allowed size in bytes of a single record.");
        Preconditions.checkNotNull(configuration.getRateLimitingStrategy());
        this.maxBatchSize = configuration.getMaxBatchSize();
        this.maxBufferedRequests = configuration.getMaxBufferedRequests();
        this.maxBatchSizeInBytes = configuration.getMaxBatchSizeInBytes();
        this.maxTimeInBufferMS = configuration.getMaxTimeInBufferMS();
        this.maxRecordSizeInBytes = configuration.getMaxRecordSizeInBytes();
        this.rateLimitingStrategy = configuration.getRateLimitingStrategy();

        this.inFlightRequestsCount = 0;
        this.bufferedRequestEntriesTotalSizeInBytes = 0;

        this.metrics = context.metricGroup();
        this.metrics.setCurrentSendTimeGauge(() -> this.ackTime - this.lastSendTimestamp);
        this.numBytesOutCounter = this.metrics.getIOMetricGroup().getNumBytesOutCounter();
        this.numRecordsOutCounter = this.metrics.getIOMetricGroup().getNumRecordsOutCounter();

        this.fatalExceptionCons =
                exception ->
                        mailboxExecutor.execute(
                                () -> {
                                    throw exception;
                                },
                                "A fatal exception occurred in the sink that cannot be recovered from or should not be retried.");

        elementConverter.open(context);
        initializeState(states);
    }

    private void registerCallback() {
        ProcessingTimeService.ProcessingTimeCallback ptc =
                instant -> {
                    existsActiveTimerCallback = false;
                    while (!bufferedRequestEntries.isEmpty()) {
                        flush();
                    }
                };
        timeService.registerTimer(timeService.getCurrentProcessingTime() + maxTimeInBufferMS, ptc);
        existsActiveTimerCallback = true;
    }

    @Override
    public void write(InputT element, Context context) throws IOException, InterruptedException {
        while (bufferedRequestEntries.size() >= maxBufferedRequests) {
            flush();
        }

        addEntryToBuffer(elementConverter.apply(element, context), false);

        nonBlockingFlush();
    }

    /**
     * Determines if a call to flush will be non-blocking (i.e. {@code inFlightRequestsCount} is
     * strictly smaller than {@code maxInFlightRequests}). Also requires one of the following
     * requirements to be met:
     *
     * <ul>
     *   <li>The number of elements buffered is greater than or equal to the {@code maxBatchSize}
     *   <li>The sum of the size in bytes of all records in the buffer is greater than or equal to
     *       {@code maxBatchSizeInBytes}
     * </ul>
     */
    private void nonBlockingFlush() throws InterruptedException {
        while (!rateLimitingStrategy.shouldBlock(createRequestInfo())
                && (bufferedRequestEntries.size() >= getNextBatchSizeLimit()
                        || bufferedRequestEntriesTotalSizeInBytes >= maxBatchSizeInBytes)) {
            flush();
        }
    }

    private BasicRequestInfo createRequestInfo() {
        int batchSize = getNextBatchSize();
        return new BasicRequestInfo(batchSize);
    }

    /**
     * Persists buffered RequestsEntries into the destination by invoking {@code
     * submitRequestEntries} with batches according to the user specified buffering hints.
     *
     * <p>The method checks with the {@code rateLimitingStrategy} to see if it should block the
     * request.
     */
    private void flush() throws InterruptedException {
        RequestInfo requestInfo = createRequestInfo();
        while (rateLimitingStrategy.shouldBlock(requestInfo)) {
            mailboxExecutor.yield();
            requestInfo = createRequestInfo();
        }

        List<RequestEntryT> batch = createNextAvailableBatch(requestInfo);
        if (batch.size() == 0) {
            return;
        }

        int batchSize = requestInfo.getBatchSize();
        long requestTimestamp = System.currentTimeMillis();
        Consumer<List<RequestEntryT>> requestToRetry =
                failedRequestEntries ->
                        mailboxExecutor.execute(
                                () ->
                                        completeRequest(
                                                failedRequestEntries, batchSize, requestTimestamp),
                                "Mark in-flight request as completed and requeue %d request entries",
                                failedRequestEntries.size());
        rateLimitingStrategy.registerInFlightRequest(requestInfo);
        inFlightRequestsCount++;
        submitRequestEntries(batch, requestToRetry);
    }

    private int getNextBatchSize() {
        return Math.min(getNextBatchSizeLimit(), bufferedRequestEntries.size());
    }

    /**
     * Creates the next batch of request entries while respecting the {@code maxBatchSize} and
     * {@code maxBatchSizeInBytes}. Also adds these to the metrics counters.
     */
    private List<RequestEntryT> createNextAvailableBatch(RequestInfo requestInfo) {
        List<RequestEntryT> batch = new ArrayList<>(requestInfo.getBatchSize());

        long batchSizeBytes = 0;
        for (int i = 0; i < requestInfo.getBatchSize(); i++) {
            long requestEntrySize = bufferedRequestEntries.peek().getSize();
            if (batchSizeBytes + requestEntrySize > maxBatchSizeInBytes) {
                break;
            }
            RequestEntryWrapper<RequestEntryT> elem = bufferedRequestEntries.remove();
            batch.add(elem.getRequestEntry());
            bufferedRequestEntriesTotalSizeInBytes -= requestEntrySize;
            batchSizeBytes += requestEntrySize;
        }

        numRecordsOutCounter.inc(batch.size());
        numBytesOutCounter.inc(batchSizeBytes);

        return batch;
    }

    /**
     * Marks an in-flight request as completed and prepends failed requestEntries back to the
     * internal requestEntry buffer for later retry.
     *
     * @param failedRequestEntries requestEntries that need to be retried
     */
    private void completeRequest(
            List<RequestEntryT> failedRequestEntries, int batchSize, long requestStartTime)
            throws InterruptedException {
        lastSendTimestamp = requestStartTime;
        ackTime = System.currentTimeMillis();

        inFlightRequestsCount--;
        rateLimitingStrategy.registerCompletedRequest(
                new BasicResultInfo(failedRequestEntries.size(), batchSize));

        ListIterator<RequestEntryT> iterator =
                failedRequestEntries.listIterator(failedRequestEntries.size());
        while (iterator.hasPrevious()) {
            addEntryToBuffer(iterator.previous(), true);
        }
        nonBlockingFlush();
    }

    private void addEntryToBuffer(RequestEntryT entry, boolean insertAtHead) {
        addEntryToBuffer(new RequestEntryWrapper<>(entry, getSizeInBytes(entry)), insertAtHead);
    }

    private void addEntryToBuffer(RequestEntryWrapper<RequestEntryT> entry, boolean insertAtHead) {
        if (bufferedRequestEntries.isEmpty() && !existsActiveTimerCallback) {
            registerCallback();
        }

        if (entry.getSize() > maxRecordSizeInBytes) {
            throw new IllegalArgumentException(
                    String.format(
                            "The request entry sent to the buffer was of size [%s], when the maxRecordSizeInBytes was set to [%s].",
                            entry.getSize(), maxRecordSizeInBytes));
        }

        if (insertAtHead) {
            bufferedRequestEntries.addFirst(entry);
        } else {
            bufferedRequestEntries.add(entry);
        }

        bufferedRequestEntriesTotalSizeInBytes += entry.getSize();
    }

    /**
     * In flight requests will be retried if the sink is still healthy. But if in-flight requests
     * fail after a checkpoint has been triggered and Flink needs to recover from the checkpoint,
     * the (failed) in-flight requests are gone and cannot be retried. Hence, there cannot be any
     * outstanding in-flight requests when a commit is initialized.
     *
     * <p>To this end, all in-flight requests need to completed before proceeding with the commit.
     */
    @Override
    public void flush(boolean flush) throws InterruptedException {
        while (inFlightRequestsCount > 0 || (bufferedRequestEntries.size() > 0 && flush)) {
            yieldIfThereExistsInFlightRequests();
            if (flush) {
                flush();
            }
        }
    }

    private void yieldIfThereExistsInFlightRequests() throws InterruptedException {
        if (inFlightRequestsCount > 0) {
            mailboxExecutor.yield();
        }
    }

    /**
     * All in-flight requests that are relevant for the snapshot have been completed, but there may
     * still be request entries in the internal buffers that are yet to be sent to the endpoint.
     * These request entries are stored in the snapshot state so that they don't get lost in case of
     * a failure/restart of the application.
     */
    @Override
    public List<BufferedRequestState<RequestEntryT>> snapshotState(long checkpointId) {
        return Collections.singletonList(new BufferedRequestState<>((bufferedRequestEntries)));
    }

    private void initializeState(Collection<BufferedRequestState<RequestEntryT>> states) {
        for (BufferedRequestState<RequestEntryT> state : states) {
            for (RequestEntryWrapper<RequestEntryT> wrapper : state.getBufferedRequestEntries()) {
                addEntryToBuffer(wrapper, false);
            }
        }
    }

    @Override
    public void close() {}

    private int getNextBatchSizeLimit() {
        return Math.min(maxBatchSize, rateLimitingStrategy.getMaxBatchSize());
    }

    protected Consumer<Exception> getFatalExceptionCons() {
        return fatalExceptionCons;
    }
}
