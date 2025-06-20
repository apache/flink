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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This operator serves a similar purpose to {@link AsyncWaitOperator}. Unlike {@link
 * AsyncWaitOperator}, this operator supports key-ordered async processing.
 *
 * <p>If the planner can infer the upsert key, then the order key used for processing will be the
 * upsert key; otherwise, the entire row will be treated as the order key.
 *
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 * @param <KEY> Key type for the operator.
 */
public class AsyncKeyOrderedLookupOperator<IN, OUT, KEY>
        extends AsyncUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    /** Timeout for the async collectors. */
    private final long timeout;

    private transient TimestampedCollector<OUT> timestampedCollector;

    /** Number of inputs which is invoked for lookup but do not output until now. */
    private final transient AtomicInteger totalInflightNum;

    public AsyncKeyOrderedLookupOperator(
            AsyncFunction<IN, OUT> asyncFunction,
            KeySelector<IN, KEY> keySelector,
            ExecutorService asyncThreadPool,
            int asyncBufferSize,
            long asyncBufferTimeout,
            int inFlightRecordsLimit,
            long timeout,
            ProcessingTimeService processingTimeService) {
        super(
                asyncFunction,
                keySelector,
                null,
                asyncThreadPool,
                asyncBufferSize,
                asyncBufferTimeout,
                inFlightRecordsLimit);
        this.timeout = timeout;
        this.totalInflightNum = new AtomicInteger(0);
        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.timestampedCollector = new TimestampedCollector<>(super.output);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
    }

    @Override
    public void open() throws Exception {
        super.open();
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        totalInflightNum.incrementAndGet();
        asyncProcess(
                // Once this runnable returned, the ref of corresponding count decrement to zero
                // which would trigger dispose the context in AsyncExecutionController
                // This part is executed in the AsyncExecutor
                () -> {
                    KeyedResultHandler handler = invoke(element);
                    handler.waitUntilOutput();
                    return null;
                });
    }

    @Override
    public void endInput() throws Exception {
        asyncExecutionController.drainInflightRecords(0);
        waitAllInFlightInputsFinished();
    }

    @Override
    protected KeySelector<?, ?> getKeySelectorForAsyncKeyedContext(int index) {
        // This operator is OneInputStreamOperator
        return keySelector1;
    }

    public KeyedResultHandler invoke(StreamRecord<IN> element) throws Exception {
        final KeyedResultHandler resultHandler =
                new KeyedResultHandler(new StreamRecordQueueEntry<>(element));
        // register a timeout for the entry if timeout is configured
        if (timeout > 0L) {
            resultHandler.registerTimeout(getProcessingTimeService(), element, timeout);
        }
        userFunction.asyncInvoke(element.getValue(), resultHandler);
        return resultHandler;
    }

    public void waitAllInFlightInputsFinished() {
        asyncExecutionController.waitUntil(()-> totalInflightNum.get() == 0);
    }

    public class KeyedResultHandler implements ResultFuture<OUT> {

        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        protected ScheduledFuture<?> timeoutTimer;

        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        protected final ResultFuture<OUT> resultFuture;

        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invocations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        protected final AtomicBoolean completed = new AtomicBoolean(false);

        /** Latch to ensure result is output. */
        protected final CountDownLatch latch;

        KeyedResultHandler(ResultFuture<OUT> resultFuture) {
            this.latch = new CountDownLatch(1);
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<OUT> results) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            // deal with result
            processInMailbox(results);
        }

        @Override
        public void completeExceptionally(Throwable error) {
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // signal failure through task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(
                            new Exception(
                                    "Could not complete the stream element in async lookup process.",
                                    error));
            // complete with empty result, so that we remove timer and move ahead processing (to
            // leave potentially
            // blocking section in #addToWorkQueue or #waitInFlightInputsFinished)
            processInMailbox(Collections.emptyList());
        }

        /**
         * Unsupported, because the containing classes are AsyncFunctions which don't have access to
         * the mailbox to invoke from the caller thread.
         */
        @Override
        public void complete(CollectionSupplier<OUT> supplier) {
            throw new UnsupportedOperationException();
        }

        public void waitUntilOutput() throws Exception {
            latch.await();
        }

        private void processInMailbox(Collection<OUT> results) {
            asyncExecutionController
                    .getMailboxExecutor()
                    .execute(() -> processResults(results), "Execute in Mailbox located in AEC");
        }

        private void processResults(Collection<OUT> results) {
            // Cancel the timer once we've completed the stream record buffer entry. This will
            // remove the registered
            // timer task
            if (timeoutTimer != null) {
                // canceling in mailbox thread avoids
                // https://issues.apache.org/jira/browse/FLINK-13635
                timeoutTimer.cancel(true);
            }

            // update the queue entry with the result
            resultFuture.complete(results);
            ((StreamRecordQueueEntry<OUT>) resultFuture).emitResult(timestampedCollector);
            totalInflightNum.decrementAndGet();
            latch.countDown();
        }

        private void registerTimeout(
                ProcessingTimeService processingTimeService,
                StreamRecord<IN> inputRecord,
                long timeout) {
            timeoutTimer =
                    registerTimer(
                            processingTimeService,
                            timeout,
                            t -> {
                                if (!completed.get()) {
                                    userFunction.timeout(inputRecord.getValue(), this);
                                }
                            });
        }
    }

    /** Utility method to register timeout timer. */
    private ScheduledFuture<?> registerTimer(
            ProcessingTimeService processingTimeService,
            long timeout,
            ThrowingConsumer<Void, Exception> callback) {
        final long timeoutTimestamp = timeout + processingTimeService.getCurrentProcessingTime();

        return processingTimeService.registerTimer(
                timeoutTimestamp, timestamp -> callback.accept(null));
    }
}
