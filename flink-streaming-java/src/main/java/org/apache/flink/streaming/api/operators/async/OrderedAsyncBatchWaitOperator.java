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
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link OrderedAsyncBatchWaitOperator} extends {@link AsyncBatchWaitOperator} with ordered
 * output semantics.
 *
 * <p>Output records are emitted in the same order as input records, even though async batch
 * invocations may complete out-of-order internally.
 *
 * <p>Ordering is achieved by maintaining a FIFO queue of pending result slots. Each slot is
 * pre-allocated when a batch is flushed. When a batch completes, it fills its slot and a drain pass
 * emits all consecutive head-of-queue completed slots in order.
 *
 * <p>Key behaviors:
 *
 * <ul>
 *   <li>Buffer incoming records until batch size is reached OR timeout expires
 *   <li>Flush remaining records when end of input is signaled
 *   <li>Wait for all batches to complete and emit in order before finishing
 * </ul>
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class OrderedAsyncBatchWaitOperator<IN, OUT> extends AsyncBatchWaitOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    /**
     * FIFO queue of result slots. Each slot is added to the tail when a batch is dispatched and
     * removed from the head when results are drained in order.
     */
    private transient Deque<ResultSlot<OUT>> resultQueue;

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
        super(parameters, asyncBatchFunction, maxBatchSize, batchTimeoutMs, mailboxExecutor);
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.resultQueue = new ArrayDeque<>();
    }

    @Override
    protected ResultFuture<OUT> createResultHandler(List<IN> batch) {
        ResultSlot<OUT> slot = new ResultSlot<>();
        resultQueue.addLast(slot);
        return new OrderedBatchResultHandler(slot);
    }

    @Override
    public void endInput() throws Exception {
        flushBuffer();

        while (inFlightCount > 0 || !resultQueue.isEmpty()) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Drain all consecutive completed slots from the head of the queue and emit their results. Runs
     * on the mailbox thread.
     */
    private void drainInOrder() {
        while (!resultQueue.isEmpty()) {
            ResultSlot<OUT> head = resultQueue.peekFirst();
            if (!head.isDone()) {
                break;
            }
            resultQueue.pollFirst();
            for (OUT result : head.getResults()) {
                output.collect(new StreamRecord<>(result));
            }
        }
    }

    /** Returns the number of pending result slots. Visible for testing. */
    int getPendingResultsCount() {
        return resultQueue != null ? resultQueue.size() : 0;
    }

    // ================================================================================
    //  Internal classes
    // ================================================================================

    /**
     * A pre-allocated slot in the result FIFO queue. The slot transitions from "pending" to "done"
     * exactly once when the batch completes.
     */
    private static class ResultSlot<OUT> {
        private Collection<OUT> results;
        private boolean done = false;

        void complete(Collection<OUT> results) {
            this.results = results;
            this.done = true;
        }

        boolean isDone() {
            return done;
        }

        Collection<OUT> getResults() {
            return results;
        }
    }

    /**
     * A {@link ResultFuture} implementation that fills a {@link ResultSlot} and triggers in-order
     * emission.
     */
    private class OrderedBatchResultHandler implements ResultFuture<OUT> {

        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final ResultSlot<OUT> slot;

        OrderedBatchResultHandler(ResultSlot<OUT> slot) {
            this.slot = slot;
        }

        @Override
        public void complete(Collection<OUT> results) {
            Preconditions.checkNotNull(
                    results, "Results must not be null, use empty collection to emit nothing");

            if (!completed.compareAndSet(false, true)) {
                return;
            }

            mailboxExecutor.execute(
                    () -> {
                        slot.complete(new ArrayList<>(results));
                        drainInOrder();
                        inFlightCount--;
                    },
                    "OrderedAsyncBatchWaitOperator#complete");
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            getContainingTask()
                    .getEnvironment()
                    .failExternally(new Exception("Async batch operation failed.", error));

            mailboxExecutor.execute(
                    () -> {
                        // Mark slot as done with empty results so the queue can be drained
                        slot.complete(new ArrayList<>());
                        drainInOrder();
                        inFlightCount--;
                    },
                    "OrderedAsyncBatchWaitOperator#completeExceptionally");
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
                            slot.complete(new ArrayList<>(supplier.get()));
                            drainInOrder();
                            inFlightCount--;
                        } catch (Throwable t) {
                            getContainingTask()
                                    .getEnvironment()
                                    .failExternally(
                                            new Exception("Async batch operation failed.", t));
                            inFlightCount--;
                        }
                    },
                    "OrderedAsyncBatchWaitOperator#completeFromSupplier");
        }
    }
}
