/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link OrderedAsyncBatchWaitOperator} batches incoming stream records and invokes the {@link
 * AsyncBatchFunction} when the batch size reaches the configured maximum or when the batch timeout
 * is reached.
 *
 * <p>This operator implements <b>ordered semantics</b> - output records are emitted in the same
 * order as input records, even though async batch invocations may complete out-of-order internally.
 *
 * <p>Ordering is achieved by:
 *
 * <ul>
 *   <li>Assigning a monotonic sequence number to each batch
 *   <li>Buffering completed batch results in a pending results map
 *   <li>Emitting results strictly in sequence order
 * </ul>
 *
 * <p>Key behaviors:
 *
 * <ul>
 *   <li>Buffer incoming records until batch size is reached OR timeout expires
 *   <li>Flush remaining records when end of input is signaled
 *   <li>Wait for all batches to complete and emit in order before finishing
 * </ul>
 *
 * <p>Timer lifecycle (when batchTimeoutMs > 0):
 *
 * <ul>
 *   <li>Timer is registered when first element is added to an empty buffer
 *   <li>Timer fires at: currentBatchStartTime + batchTimeoutMs
 *   <li>Timer is cleared when batch is flushed (by size, timeout, or end-of-input)
 *   <li>At most one timer is active at any time
 * </ul>
 *
 * <h3>Metrics</h3>
 *
 * <p>This operator exposes the following metrics for monitoring AI/ML inference workloads:
 *
 * <ul>
 *   <li>{@code batchSize} - Histogram of batch sizes (number of records per batch)
 *   <li>{@code batchLatencyMs} - Histogram of batch latency in milliseconds (time from first
 *       element buffered to batch flush)
 *   <li>{@code asyncCallDurationMs} - Histogram of async call duration in milliseconds (time from
 *       async invocation to completion)
 *   <li>{@code inflightBatches} - Gauge showing current number of in-flight async batch operations
 *   <li>{@code totalBatchesProcessed} - Counter of total batches processed
 *   <li>{@code totalRecordsProcessed} - Counter of total records processed
 *   <li>{@code asyncCallFailures} - Counter of failed async calls
 *   <li>{@code pendingOrderedBatches} - Gauge showing batches waiting for in-order emission
 * </ul>
 *
 * <p>Future enhancements may include:
 *
 * <ul>
 *   <li>Event-time or watermark-based ordering
 *   <li>Multiple inflight batches concurrency control
 *   <li>Retry logic
 * </ul>
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class OrderedAsyncBatchWaitOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    /** Constant indicating timeout is disabled. */
    private static final long NO_TIMEOUT = 0L;

    /** Default window size for histogram metrics. */
    private static final int METRICS_HISTOGRAM_WINDOW_SIZE = 1000;

    // ================================================================================
    //  Metric names - exposed as constants for testing and documentation
    // ================================================================================

    /** Metric name for batch size histogram. */
    public static final String METRIC_BATCH_SIZE = "batchSize";

    /** Metric name for batch latency histogram (in milliseconds). */
    public static final String METRIC_BATCH_LATENCY_MS = "batchLatencyMs";

    /** Metric name for async call duration histogram (in milliseconds). */
    public static final String METRIC_ASYNC_CALL_DURATION_MS = "asyncCallDurationMs";

    /** Metric name for in-flight batches gauge. */
    public static final String METRIC_INFLIGHT_BATCHES = "inflightBatches";

    /** Metric name for total batches processed counter. */
    public static final String METRIC_TOTAL_BATCHES_PROCESSED = "totalBatchesProcessed";

    /** Metric name for total records processed counter. */
    public static final String METRIC_TOTAL_RECORDS_PROCESSED = "totalRecordsProcessed";

    /** Metric name for async call failures counter. */
    public static final String METRIC_ASYNC_CALL_FAILURES = "asyncCallFailures";

    /** Metric name for pending ordered batches gauge. */
    public static final String METRIC_PENDING_ORDERED_BATCHES = "pendingOrderedBatches";

    // ================================================================================
    //  Configuration fields
    // ================================================================================

    /** The async batch function to invoke. */
    private final AsyncBatchFunction<IN, OUT> asyncBatchFunction;

    /** Maximum batch size before triggering async invocation. */
    private final int maxBatchSize;

    /**
     * Batch timeout in milliseconds. When positive, a timer is registered to flush the batch after
     * this duration since the first buffered element. A value <= 0 disables timeout-based batching.
     */
    private final long batchTimeoutMs;

    /** Buffer for incoming stream records. */
    private transient List<IN> buffer;

    /** Mailbox executor for processing async results on the main thread. */
    private final transient MailboxExecutor mailboxExecutor;

    /** Counter for in-flight async operations. */
    private transient int inFlightCount;

    // ================================================================================
    //  Timer state fields for timeout-based batching
    // ================================================================================

    /**
     * The processing time when the current batch started (i.e., when first element was added to
     * empty buffer). Used to calculate timer fire time.
     */
    private transient long currentBatchStartTime;

    /** Whether a timer is currently registered for the current batch. */
    private transient boolean timerRegistered;

    // ================================================================================
    //  Ordered emission state fields
    // ================================================================================

    /**
     * The sequence number to assign to the next batch. Monotonically increasing, starting from 0.
     */
    private transient long nextBatchSequenceNumber;

    /**
     * The sequence number of the next batch whose results should be emitted. Used to ensure
     * strictly ordered output emission.
     */
    private transient long nextExpectedSequenceNumber;

    /**
     * Pending results buffer. Maps batch sequence number to completed results. Results are held
     * here until all preceding batches have been emitted.
     */
    private transient Map<Long, Collection<OUT>> pendingResults;

    // ================================================================================
    //  Metrics fields
    // ================================================================================

    /**
     * Histogram tracking the size of each batch. Useful for monitoring batch efficiency and tuning
     * maxBatchSize parameter.
     */
    private transient Histogram batchSizeHistogram;

    /**
     * Histogram tracking batch latency in milliseconds. Measures time from when first element is
     * added to buffer until batch is flushed. Helps identify buffering overhead.
     */
    private transient Histogram batchLatencyHistogram;

    /**
     * Histogram tracking async call duration in milliseconds. Measures time from async invocation
     * to completion callback. Critical for monitoring inference latency.
     */
    private transient Histogram asyncCallDurationHistogram;

    /**
     * Gauge showing current number of in-flight batches. Useful for monitoring backpressure and
     * concurrency.
     */
    @SuppressWarnings("unused") // Registered as gauge, kept as field reference
    private transient Gauge<Integer> inflightBatchesGauge;

    /** Gauge showing number of batches waiting for in-order emission. */
    @SuppressWarnings("unused") // Registered as gauge, kept as field reference
    private transient Gauge<Integer> pendingOrderedBatchesGauge;

    /** Counter for total batches processed. */
    private transient Counter totalBatchesProcessedCounter;

    /** Counter for total records processed. */
    private transient Counter totalRecordsProcessedCounter;

    /** Counter for failed async calls. */
    private transient Counter asyncCallFailuresCounter;

    /**
     * Creates an OrderedAsyncBatchWaitOperator with size-based batching only (no timeout).
     *
     * @param parameters Stream operator parameters
     * @param asyncBatchFunction The async batch function to invoke
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param mailboxExecutor Mailbox executor for processing async results
     */
    public OrderedAsyncBatchWaitOperator(
            @Nonnull StreamOperatorParameters<OUT> parameters,
            @Nonnull AsyncBatchFunction<IN, OUT> asyncBatchFunction,
            int maxBatchSize,
            @Nonnull MailboxExecutor mailboxExecutor) {
        this(parameters, asyncBatchFunction, maxBatchSize, NO_TIMEOUT, mailboxExecutor);
    }

    /**
     * Creates an OrderedAsyncBatchWaitOperator with size-based and optional timeout-based batching.
     *
     * @param parameters Stream operator parameters
     * @param asyncBatchFunction The async batch function to invoke
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param batchTimeoutMs Batch timeout in milliseconds; <= 0 means disabled
     * @param mailboxExecutor Mailbox executor for processing async results
     */
    public OrderedAsyncBatchWaitOperator(
            @Nonnull StreamOperatorParameters<OUT> parameters,
            @Nonnull AsyncBatchFunction<IN, OUT> asyncBatchFunction,
            int maxBatchSize,
            long batchTimeoutMs,
            @Nonnull MailboxExecutor mailboxExecutor) {
        Preconditions.checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than 0");
        this.asyncBatchFunction = Preconditions.checkNotNull(asyncBatchFunction);
        this.maxBatchSize = maxBatchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.mailboxExecutor = Preconditions.checkNotNull(mailboxExecutor);

        // Setup the operator using parameters
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.buffer = new ArrayList<>(maxBatchSize);
        this.inFlightCount = 0;
        this.currentBatchStartTime = 0L;
        this.timerRegistered = false;

        // Initialize ordered emission state
        this.nextBatchSequenceNumber = 0L;
        this.nextExpectedSequenceNumber = 0L;
        this.pendingResults = new TreeMap<>();

        // Initialize metrics
        registerMetrics();
    }

    /**
     * Registers all metrics for this operator.
     *
     * <p>Metrics are registered under the operator's metric group and provide visibility into batch
     * processing behavior for AI/ML inference workloads.
     */
    private void registerMetrics() {
        MetricGroup metricGroup = metrics;

        // Histogram for batch sizes
        this.batchSizeHistogram =
                metricGroup.histogram(
                        METRIC_BATCH_SIZE,
                        new DescriptiveStatisticsHistogram(METRICS_HISTOGRAM_WINDOW_SIZE));

        // Histogram for batch latency (time from first element to flush)
        this.batchLatencyHistogram =
                metricGroup.histogram(
                        METRIC_BATCH_LATENCY_MS,
                        new DescriptiveStatisticsHistogram(METRICS_HISTOGRAM_WINDOW_SIZE));

        // Histogram for async call duration
        this.asyncCallDurationHistogram =
                metricGroup.histogram(
                        METRIC_ASYNC_CALL_DURATION_MS,
                        new DescriptiveStatisticsHistogram(METRICS_HISTOGRAM_WINDOW_SIZE));

        // Gauge for in-flight batches
        this.inflightBatchesGauge = metricGroup.gauge(METRIC_INFLIGHT_BATCHES, () -> inFlightCount);

        // Gauge for pending ordered batches (specific to ordered operator)
        this.pendingOrderedBatchesGauge =
                metricGroup.gauge(
                        METRIC_PENDING_ORDERED_BATCHES,
                        () -> pendingResults != null ? pendingResults.size() : 0);

        // Counter for total batches processed
        this.totalBatchesProcessedCounter = metricGroup.counter(METRIC_TOTAL_BATCHES_PROCESSED);

        // Counter for total records processed
        this.totalRecordsProcessedCounter = metricGroup.counter(METRIC_TOTAL_RECORDS_PROCESSED);

        // Counter for failed async calls
        this.asyncCallFailuresCounter = metricGroup.counter(METRIC_ASYNC_CALL_FAILURES);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // If buffer is empty and timeout is enabled, record batch start time and register timer
        if (buffer.isEmpty() && isTimeoutEnabled()) {
            currentBatchStartTime = getProcessingTimeService().getCurrentProcessingTime();
            registerBatchTimer();
        }

        // Record batch start time for latency tracking (even without timeout)
        if (buffer.isEmpty() && !isTimeoutEnabled()) {
            currentBatchStartTime = System.currentTimeMillis();
        }

        buffer.add(element.getValue());

        // Size-triggered flush: cancel pending timer and flush
        if (buffer.size() >= maxBatchSize) {
            flushBuffer();
        }
    }

    /**
     * Callback when processing time timer fires. Flushes the buffer if non-empty.
     *
     * @param timestamp The timestamp for which the timer was registered
     */
    @Override
    public void onProcessingTime(long timestamp) throws Exception {
        // Timer fired - clear timer state first
        timerRegistered = false;

        // Flush buffer if non-empty (timeout-triggered flush)
        if (!buffer.isEmpty()) {
            flushBuffer();
        }
    }

    /** Flush the current buffer by invoking the async batch function. */
    private void flushBuffer() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

        // Calculate batch latency (time from first element to now)
        long batchLatencyMs;
        if (isTimeoutEnabled()) {
            batchLatencyMs =
                    getProcessingTimeService().getCurrentProcessingTime() - currentBatchStartTime;
        } else {
            batchLatencyMs = System.currentTimeMillis() - currentBatchStartTime;
        }

        // Clear timer state since we're flushing the batch
        clearTimerState();

        // Create a copy of the buffer and clear it for new incoming elements
        List<IN> batch = new ArrayList<>(buffer);
        buffer.clear();

        // Update metrics
        int batchSize = batch.size();
        batchSizeHistogram.update(batchSize);
        batchLatencyHistogram.update(batchLatencyMs);
        totalBatchesProcessedCounter.inc();
        totalRecordsProcessedCounter.inc(batchSize);

        // Assign sequence number to this batch and increment counter
        long batchSequenceNumber = nextBatchSequenceNumber++;

        // Increment in-flight counter
        inFlightCount++;

        // Record async call start time for duration tracking
        long asyncCallStartTime = System.currentTimeMillis();

        // Create result handler for this batch with its sequence number
        OrderedBatchResultHandler resultHandler =
                new OrderedBatchResultHandler(batchSequenceNumber, asyncCallStartTime);

        // Invoke the async batch function
        asyncBatchFunction.asyncInvokeBatch(batch, resultHandler);
    }

    @Override
    public void endInput() throws Exception {
        // Flush any remaining elements in the buffer
        flushBuffer();

        // Wait for all in-flight async operations to complete and emit results in order
        while (inFlightCount > 0 || !pendingResults.isEmpty()) {
            mailboxExecutor.yield();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    // ================================================================================
    //  Timer management methods
    // ================================================================================

    /** Check if timeout-based batching is enabled. */
    private boolean isTimeoutEnabled() {
        return batchTimeoutMs > NO_TIMEOUT;
    }

    /** Register a processing time timer for the current batch. */
    private void registerBatchTimer() {
        if (!timerRegistered && isTimeoutEnabled()) {
            long fireTime = currentBatchStartTime + batchTimeoutMs;
            getProcessingTimeService().registerTimer(fireTime, this);
            timerRegistered = true;
        }
    }

    /**
     * Clear timer state. Note: We don't explicitly cancel the timer because: 1. The timer callback
     * checks buffer state before flushing 2. Cancelling timers has overhead 3. Timer will be
     * ignored if buffer is empty when it fires
     */
    private void clearTimerState() {
        timerRegistered = false;
        currentBatchStartTime = 0L;
    }

    // ================================================================================
    //  Ordered emission methods
    // ================================================================================

    /**
     * Try to emit results in order. Called when a batch completes. Emits all consecutive completed
     * batches starting from nextExpectedSequenceNumber.
     */
    private void tryEmitInOrder() {
        // Emit results in strict sequence order
        while (pendingResults.containsKey(nextExpectedSequenceNumber)) {
            Collection<OUT> results = pendingResults.remove(nextExpectedSequenceNumber);

            // Emit all results from this batch
            for (OUT result : results) {
                output.collect(new StreamRecord<>(result));
            }

            // Move to next expected sequence number
            nextExpectedSequenceNumber++;
        }
    }

    // ================================================================================
    //  Test accessors
    // ================================================================================

    /** Returns the current buffer size. Visible for testing. */
    @VisibleForTesting
    int getBufferSize() {
        return buffer != null ? buffer.size() : 0;
    }

    /** Returns the number of pending result batches. Visible for testing. */
    @VisibleForTesting
    int getPendingResultsCount() {
        return pendingResults != null ? pendingResults.size() : 0;
    }

    /** Returns the current in-flight count. Visible for testing. */
    @VisibleForTesting
    int getInFlightCount() {
        return inFlightCount;
    }

    /** Returns the batch size histogram. Visible for testing. */
    @VisibleForTesting
    Histogram getBatchSizeHistogram() {
        return batchSizeHistogram;
    }

    /** Returns the batch latency histogram. Visible for testing. */
    @VisibleForTesting
    Histogram getBatchLatencyHistogram() {
        return batchLatencyHistogram;
    }

    /** Returns the async call duration histogram. Visible for testing. */
    @VisibleForTesting
    Histogram getAsyncCallDurationHistogram() {
        return asyncCallDurationHistogram;
    }

    /** Returns the total batches processed counter. Visible for testing. */
    @VisibleForTesting
    Counter getTotalBatchesProcessedCounter() {
        return totalBatchesProcessedCounter;
    }

    /** Returns the total records processed counter. Visible for testing. */
    @VisibleForTesting
    Counter getTotalRecordsProcessedCounter() {
        return totalRecordsProcessedCounter;
    }

    /** Returns the async call failures counter. Visible for testing. */
    @VisibleForTesting
    Counter getAsyncCallFailuresCounter() {
        return asyncCallFailuresCounter;
    }

    /**
     * A handler for the results of a batch async invocation that maintains ordering.
     *
     * <p>Results are stored in the pending results buffer and emitted in sequence order.
     */
    private class OrderedBatchResultHandler implements ResultFuture<OUT> {

        /** Guard against multiple completions. */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        /** The sequence number of this batch. */
        private final long batchSequenceNumber;

        /** Start time of the async call for duration tracking. */
        private final long asyncCallStartTime;

        OrderedBatchResultHandler(long batchSequenceNumber, long asyncCallStartTime) {
            this.batchSequenceNumber = batchSequenceNumber;
            this.asyncCallStartTime = asyncCallStartTime;
        }

        @Override
        public void complete(Collection<OUT> results) {
            Preconditions.checkNotNull(
                    results, "Results must not be null, use empty collection to emit nothing");

            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // Process results in the mailbox thread
            mailboxExecutor.execute(
                    () -> processResultsOrdered(results),
                    "OrderedAsyncBatchWaitOperator#processResultsOrdered");
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // Update failure metric
            asyncCallFailuresCounter.inc();

            // Record async call duration even for failures
            long duration = System.currentTimeMillis() - asyncCallStartTime;
            asyncCallDurationHistogram.update(duration);

            // Signal failure through the containing task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(new Exception("Async batch operation failed.", error));

            // Decrement in-flight counter in mailbox thread
            mailboxExecutor.execute(
                    () -> inFlightCount--, "OrderedAsyncBatchWaitOperator#decrementInFlight");
        }

        @Override
        public void complete(CollectionSupplier<OUT> supplier) {
            Preconditions.checkNotNull(
                    supplier, "Supplier must not be null, return empty collection to emit nothing");

            if (!completed.compareAndSet(false, true)) {
                return;
            }

            mailboxExecutor.execute(
                    () -> {
                        try {
                            processResultsOrdered(supplier.get());
                        } catch (Throwable t) {
                            // Update failure metric
                            asyncCallFailuresCounter.inc();

                            // Record async call duration even for failures
                            long duration = System.currentTimeMillis() - asyncCallStartTime;
                            asyncCallDurationHistogram.update(duration);

                            getContainingTask()
                                    .getEnvironment()
                                    .failExternally(
                                            new Exception("Async batch operation failed.", t));
                            inFlightCount--;
                        }
                    },
                    "OrderedAsyncBatchWaitOperator#processResultsFromSupplier");
        }

        /**
         * Process results with ordering guarantee.
         *
         * <p>Results are added to the pending buffer and then we try to emit all consecutive
         * completed batches in order.
         */
        private void processResultsOrdered(Collection<OUT> results) {
            // Record async call duration
            long duration = System.currentTimeMillis() - asyncCallStartTime;
            asyncCallDurationHistogram.update(duration);

            // Store results in pending buffer keyed by sequence number
            pendingResults.put(batchSequenceNumber, new ArrayList<>(results));

            // Try to emit all consecutive completed batches
            tryEmitInOrder();

            // Decrement in-flight counter
            inFlightCount--;
        }
    }
}
