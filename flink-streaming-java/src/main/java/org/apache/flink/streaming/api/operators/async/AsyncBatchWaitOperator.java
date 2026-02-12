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
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link AsyncBatchWaitOperator} batches incoming stream records and invokes the {@link
 * AsyncBatchFunction} when the batch size reaches the configured maximum or when the batch timeout
 * is reached.
 *
 * <p>This operator implements unordered semantics only - results are emitted as soon as they are
 * available, regardless of input order. This is suitable for AI inference workloads where order
 * does not matter.
 *
 * <p>Key behaviors:
 *
 * <ul>
 *   <li>Buffer incoming records until batch size is reached OR timeout expires
 *   <li>Flush remaining records when end of input is signaled
 *   <li>Emit all results from the batch function to downstream
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
 * <p>Future enhancements may include:
 *
 * <ul>
 *   <li>Ordered mode support
 *   <li>Event-time based batching
 *   <li>Multiple inflight batches
 *   <li>Retry logic
 *   <li>Metrics
 * </ul>
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncBatchWaitOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput, ProcessingTimeCallback {

    private static final long serialVersionUID = 1L;

    /** Constant indicating timeout is disabled. */
    private static final long NO_TIMEOUT = 0L;

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

    /**
     * Creates an AsyncBatchWaitOperator with size-based batching only (no timeout).
     *
     * @param parameters Stream operator parameters
     * @param asyncBatchFunction The async batch function to invoke
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param mailboxExecutor Mailbox executor for processing async results
     */
    public AsyncBatchWaitOperator(
            @Nonnull StreamOperatorParameters<OUT> parameters,
            @Nonnull AsyncBatchFunction<IN, OUT> asyncBatchFunction,
            int maxBatchSize,
            @Nonnull MailboxExecutor mailboxExecutor) {
        this(parameters, asyncBatchFunction, maxBatchSize, NO_TIMEOUT, mailboxExecutor);
    }

    /**
     * Creates an AsyncBatchWaitOperator with size-based and optional timeout-based batching.
     *
     * @param parameters Stream operator parameters
     * @param asyncBatchFunction The async batch function to invoke
     * @param maxBatchSize Maximum batch size before triggering async invocation
     * @param batchTimeoutMs Batch timeout in milliseconds; <= 0 means disabled
     * @param mailboxExecutor Mailbox executor for processing async results
     */
    public AsyncBatchWaitOperator(
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
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // If buffer is empty and timeout is enabled, record batch start time and register timer
        if (buffer.isEmpty() && isTimeoutEnabled()) {
            currentBatchStartTime = getProcessingTimeService().getCurrentProcessingTime();
            registerBatchTimer();
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

        // Clear timer state since we're flushing the batch
        clearTimerState();

        // Create a copy of the buffer and clear it for new incoming elements
        List<IN> batch = new ArrayList<>(buffer);
        buffer.clear();

        // Increment in-flight counter
        inFlightCount++;

        // Create result handler for this batch
        BatchResultHandler resultHandler = new BatchResultHandler();

        // Invoke the async batch function
        asyncBatchFunction.asyncInvokeBatch(batch, resultHandler);
    }

    @Override
    public void endInput() throws Exception {
        // Flush any remaining elements in the buffer
        flushBuffer();

        // Wait for all in-flight async operations to complete
        while (inFlightCount > 0) {
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

    /** Returns the current buffer size. Visible for testing. */
    int getBufferSize() {
        return buffer != null ? buffer.size() : 0;
    }

    /** A handler for the results of a batch async invocation. */
    private class BatchResultHandler implements ResultFuture<OUT> {

        /** Guard against multiple completions. */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        @Override
        public void complete(Collection<OUT> results) {
            Preconditions.checkNotNull(
                    results, "Results must not be null, use empty collection to emit nothing");

            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // Process results in the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results), "AsyncBatchWaitOperator#processResults");
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // Signal failure through the containing task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(new Exception("Async batch operation failed.", error));

            // Decrement in-flight counter in mailbox thread
            mailboxExecutor.execute(
                    () -> inFlightCount--, "AsyncBatchWaitOperator#decrementInFlight");
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
                            processResults(supplier.get());
                        } catch (Throwable t) {
                            getContainingTask()
                                    .getEnvironment()
                                    .failExternally(
                                            new Exception("Async batch operation failed.", t));
                            inFlightCount--;
                        }
                    },
                    "AsyncBatchWaitOperator#processResultsFromSupplier");
        }

        private void processResults(Collection<OUT> results) {
            // Emit all results downstream
            for (OUT result : results) {
                output.collect(new StreamRecord<>(result));
            }
            // Decrement in-flight counter
            inFlightCount--;
        }
    }
}
