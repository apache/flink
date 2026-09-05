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
 * AsyncBatchFunction} when the batch size reaches the configured maximum.
 *
 * <p>This operator implements unordered semantics only - results are emitted as soon as they are
 * available, regardless of input order. This is suitable for AI inference workloads where order
 * does not matter.
 *
 * <p>Key behaviors:
 *
 * <ul>
 *   <li>Buffer incoming records until batch size is reached
 *   <li>Flush remaining records when end of input is signaled
 *   <li>Emit all results from the batch function to downstream
 * </ul>
 *
 * <p>This is a minimal implementation for the first PR. Future enhancements may include:
 *
 * <ul>
 *   <li>Ordered mode support
 *   <li>Time-based batching with timers
 *   <li>Timeout handling
 *   <li>Retry logic
 *   <li>Metrics
 * </ul>
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class AsyncBatchWaitOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    /** The async batch function to invoke. */
    private final AsyncBatchFunction<IN, OUT> asyncBatchFunction;

    /** Maximum batch size before triggering async invocation. */
    private final int maxBatchSize;

    /** Buffer for incoming stream records. */
    private transient List<IN> buffer;

    /** Mailbox executor for processing async results on the main thread. */
    private final transient MailboxExecutor mailboxExecutor;

    /** Counter for in-flight async operations. */
    private transient int inFlightCount;

    public AsyncBatchWaitOperator(
            @Nonnull StreamOperatorParameters<OUT> parameters,
            @Nonnull AsyncBatchFunction<IN, OUT> asyncBatchFunction,
            int maxBatchSize,
            @Nonnull MailboxExecutor mailboxExecutor) {
        Preconditions.checkArgument(maxBatchSize > 0, "maxBatchSize must be greater than 0");
        this.asyncBatchFunction = Preconditions.checkNotNull(asyncBatchFunction);
        this.maxBatchSize = maxBatchSize;
        this.mailboxExecutor = Preconditions.checkNotNull(mailboxExecutor);

        // Setup the operator using parameters
        setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.buffer = new ArrayList<>(maxBatchSize);
        this.inFlightCount = 0;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        buffer.add(element.getValue());

        if (buffer.size() >= maxBatchSize) {
            flushBuffer();
        }
    }

    /** Flush the current buffer by invoking the async batch function. */
    private void flushBuffer() throws Exception {
        if (buffer.isEmpty()) {
            return;
        }

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
