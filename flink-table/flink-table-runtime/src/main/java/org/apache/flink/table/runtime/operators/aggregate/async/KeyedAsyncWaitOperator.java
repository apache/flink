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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.MailboxWatermarkProcessor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.aggregate.async.queue.KeyedAsyncOutputMode;
import org.apache.flink.table.runtime.operators.aggregate.async.queue.KeyedStreamElementQueue;
import org.apache.flink.table.runtime.operators.aggregate.async.queue.KeyedStreamElementQueueImpl;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * The {@link KeyedAsyncWaitOperator} allows to asynchronously process incoming stream records. For
 * that the operator creates an {@link ResultFuture} which is passed to an {@link
 * KeyedAsyncFunction}. Within the async function, the user can complete the async collector
 * arbitrarily. Once the async collector has been completed, the result is emitted by the operator's
 * emitter to downstream operators.
 *
 * <p>The operator offers different output modes depending on the chosen {@link
 * KeyedAsyncOutputMode}. In order to give exactly once processing guarantees, the operator stores
 * all currently in-flight {@link StreamElement} in per-key state. Upon recovery the recorded set of
 * stream elements is replayed.
 *
 * <p>Because {@link KeyedAsyncFunction}s can utilize row-based timers, retries are not supported by
 * this operator. Moving the current watermark forward prevents timers from being retried. If
 * retries are desired, they should be internal to the {@link KeyedAsyncFunction}.
 *
 * <p>In case of chaining of this operator, it has to be made sure that the operators in the chain
 * are opened tail to head. The reason for this is that an opened {@link KeyedAsyncWaitOperator}
 * starts already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <K> Key type for the operator.
 * @param <IN> Input type for the operator.
 * @param <OUT> Output type for the operator.
 */
@Internal
public class KeyedAsyncWaitOperator<K, IN, OUT>
        extends AbstractUdfStreamOperator<OUT, KeyedAsyncFunction<K, IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
    private static final long serialVersionUID = 1L;

    private static final String STATE_NAME = "_keyed_async_wait_operator_state_";

    /** Capacity of the stream element queue. */
    private final int capacity;

    /** Output mode for this operator. */
    private final KeyedAsyncOutputMode outputMode;

    /** Timeout for the async collectors. */
    private final long timeout;

    /** {@link TypeSerializer} for inputs while making snapshots. */
    private transient StreamElementSerializer<IN> inStreamElementSerializer;

    /** Recovered input stream elements. */
    private transient Map<K, List<StreamElement>> recoveredStreamElements;

    /** Queue, into which to store the currently in-flight stream elements. */
    private transient KeyedStreamElementQueue<K, OUT> queue;

    /** Mailbox executor used to yield while waiting for buffers to empty. */
    private final transient MailboxExecutor mailboxExecutor;

    private transient TimestampedCollector<OUT> timestampedCollector;

    /** Whether object reuse has been enabled or disabled. */
    private transient boolean isObjectReuseEnabled;

    private transient TimerService timerService;

    public KeyedAsyncWaitOperator(
            @Nonnull KeyedAsyncFunction<K, IN, OUT> asyncFunction,
            long timeout,
            int capacity,
            @Nonnull KeyedAsyncOutputMode outputMode,
            @Nonnull ProcessingTimeService processingTimeService,
            @Nonnull MailboxExecutor mailboxExecutor) {
        super(asyncFunction);

        Preconditions.checkArgument(
                capacity > 0, "The number of concurrent async operation should be greater than 0.");
        this.capacity = capacity;

        this.outputMode = Preconditions.checkNotNull(outputMode, "outputMode");

        this.timeout = timeout;

        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);

        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        this.inStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));

        switch (outputMode) {
            case ORDERED:
                queue = KeyedStreamElementQueueImpl.createOrderedQueue(capacity);
                break;
            default:
                throw new IllegalStateException("Unknown async mode: " + outputMode + '.');
        }

        this.timestampedCollector = new TimestampedCollector<>(super.output);
    }

    @Override
    public void open() throws Exception {
        super.open();

        userFunction.open(new OpenContextImpl(this, getRuntimeContext(), mailboxExecutor));

        this.isObjectReuseEnabled = getExecutionConfig().isObjectReuseEnabled();

        if (recoveredStreamElements != null) {
            for (Map.Entry<K, List<StreamElement>> e : recoveredStreamElements.entrySet()) {
                List<StreamElement> elementList = e.getValue();
                setCurrentKey(e.getKey());
                for (StreamElement element : elementList) {
                    if (element.isRecord()) {
                        processElement(element.<IN>asRecord());
                    } else if (element.isWatermark()) {
                        processWatermark(element.asWatermark());
                    } else if (element.isLatencyMarker()) {
                        processLatencyMarker(element.asLatencyMarker());
                    } else {
                        throw new IllegalStateException(
                                "Unknown record type "
                                        + element.getClass()
                                        + " encountered while opening the operator.");
                    }
                }
            }
            recoveredStreamElements = null;
        }
    }

    @Override
    public void close() throws Exception {
        userFunction.close();
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        StreamRecord<IN> element;
        // copy the element avoid the element is reused
        if (isObjectReuseEnabled) {
            //noinspection unchecked
            element = (StreamRecord<IN>) inStreamElementSerializer.copy(record);
        } else {
            element = record;
        }

        // add element first to the queue
        final ResultFuture<OUT> entry = addToWorkQueue(element);

        final ResultHandler resultHandler = new ResultHandler(element, entry);

        // register a timeout for the entry if timeout is configured
        if (timeout > 0L) {
            resultHandler.registerTimeout(getProcessingTimeService(), timeout);
        }

        userFunction.asyncInvoke(element.getValue(), resultHandler);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        MailboxWatermarkProcessor.WatermarkEmitter addToQueue =
                w -> {
                    addToWorkQueue(mark);

                    // watermarks are always completed
                    // if there is no prior element, we can directly emit them
                    // this also avoids watermarks being held back until the next element has been
                    // processed
                    outputCompletedElement();
                };

        if (watermarkProcessor != null) {
            watermarkProcessor.emitWatermarkInsideMailbox(mark, addToQueue);
        } else {
            if (getTimeServiceManager().isPresent()) {
                getTimeServiceManager().get().advanceWatermark(mark);
            }

            addToQueue.emitWatermark(mark);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        for (Map.Entry<K, List<StreamElement>> entry : queue.valuesByKey().entrySet()) {
            setCurrentKey(entry.getKey());

            ListState<StreamElement> partitionableState =
                    getKeyedStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            STATE_NAME, inStreamElementSerializer));

            try {
                partitionableState.update(entry.getValue());
            } catch (Exception e) {
                partitionableState.clear();

                throw new Exception(
                        "Could not add stream element queue entries to operator state "
                                + "backend of operator "
                                + getOperatorName()
                                + '.',
                        e);
            }
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        List<K> keys =
                this.<K>getKeyedStateBackend()
                        .getKeys(STATE_NAME, VoidNamespace.INSTANCE)
                        .collect(Collectors.toList());
        recoveredStreamElements = new LinkedHashMap<>();
        for (K key : keys) {
            setCurrentKey(key);
            ListState<StreamElement> state =
                    context.getKeyedStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            STATE_NAME, inStreamElementSerializer));

            List<StreamElement> streamElements = new ArrayList<>();
            for (StreamElement elements : state.get()) {
                streamElements.add(elements);
            }
            recoveredStreamElements.put(key, streamElements);
        }
    }

    @Override
    public void endInput() throws Exception {
        // we should finish all in fight delayed retry immediately.
        finishInFlightDelayedRetry();

        // we should wait here for the data in flight to be finished. the reason is that the
        // timer not in running will be forbidden to fire after this, so that when the async
        // operation is stuck, it results in deadlock due to what the timeout timer is not fired
        waitInFlightInputsFinished();
    }

    /**
     * Add the given stream element to the operator's stream element queue. This operation blocks
     * until the element has been added.
     *
     * <p>Between two insertion attempts, this method yields the execution to the mailbox, such that
     * events as well as asynchronous results can be processed.
     *
     * @param streamElement to add to the operator's queue
     * @throws InterruptedException if the current thread has been interrupted while yielding to
     *     mailbox
     * @return a handle that allows to set the result of the async computation for the given
     *     element.
     */
    private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement)
            throws InterruptedException {
        Optional<ResultFuture<OUT>> queueEntry;
        while (!(queueEntry = queue.tryPut((K) getCurrentKey(), streamElement)).isPresent()) {
            mailboxExecutor.yield();
        }
        return queueEntry.get();
    }

    private void finishInFlightDelayedRetry() throws Exception {}

    private void waitInFlightInputsFinished() throws InterruptedException {

        while (!queue.isEmpty()) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Outputs one completed element. Watermarks are always completed if it's their turn to be
     * processed.
     *
     * <p>This method will be called from {@link #processWatermark(Watermark)} and from a mail
     * processing the result of an async function call.
     */
    private void outputCompletedElement() {
        if (queue.hasCompletedElements()) {
            // emit only one element to not block the mailbox thread unnecessarily
            queue.emitCompletedElement(timestampedCollector);

            // if there are more completed elements, emit them with subsequent mails
            if (queue.hasCompletedElements()) {
                try {
                    mailboxExecutor.execute(
                            this::outputCompletedElement,
                            "AsyncWaitOperator#outputCompletedElement");
                } catch (RejectedExecutionException mailboxClosedException) {
                    // This exception can only happen if the operator is cancelled which means all
                    // pending records can be safely ignored since they will be processed one more
                    // time after recovery.
                    LOG.debug(
                            "Attempt to complete element is ignored since the mailbox rejected the execution.",
                            mailboxClosedException);
                }
            }
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

    /** A handler for the results of a specific input record. */
    private class ResultHandler implements ResultFuture<OUT> {
        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        private ScheduledFuture<?> timeoutTimer;

        /** Record for which this result handler exists. Used only to report errors. */
        private final StreamRecord<IN> inputRecord;

        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        private final ResultFuture<OUT> resultFuture;

        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invokations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        ResultHandler(StreamRecord<IN> inputRecord, ResultFuture<OUT> resultFuture) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
        }

        @Override
        public void complete(Collection<OUT> results) {

            // already completed (exceptionally or with previous complete call from ill-written
            // AsyncFunction), so
            // ignore additional result
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            processInMailbox(results);
        }

        @Override
        public void complete(CollectionSupplier<OUT> runnable) {
            // already completed (exceptionally or with previous complete call from ill-written
            // AsyncFunction), so ignore additional result
            if (!completed.compareAndSet(false, true)) {
                return;
            }
            mailboxExecutor.execute(
                    () -> {
                        // If there is an exception, let it bubble up and fail the job.
                        processResults(runnable.get());
                    },
                    "ResultHandler#complete");
        }

        private void processInMailbox(Collection<OUT> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
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
            // now output all elements from the queue that have been completed (in the correct
            // order)
            outputCompletedElement();
        }

        @Override
        public void completeExceptionally(Throwable error) {
            // already completed, so ignore exception
            if (!completed.compareAndSet(false, true)) {
                return;
            }

            // signal failure through task
            getContainingTask()
                    .getEnvironment()
                    .failExternally(
                            new Exception(
                                    "Could not complete the stream element: <redacted>.", error));

            // complete with empty result, so that we remove timer and move ahead processing (to
            // leave potentially
            // blocking section in #addToWorkQueue or #waitInFlightInputsFinished)
            processInMailbox(Collections.emptyList());
        }

        private void registerTimeout(ProcessingTimeService processingTimeService, long timeout) {
            timeoutTimer = registerTimer(processingTimeService, timeout, t -> timerTriggered());
        }

        private void timerTriggered() throws Exception {
            if (!completed.get()) {
                userFunction.timeout(inputRecord.getValue(), this);
            }
        }
    }

    private static class OpenContextImpl implements KeyedAsyncFunction.OpenContext {

        private final AbstractStreamOperator<?> operator;
        private final RuntimeContext runtimeContext;
        private final MailboxExecutor mailboxExecutor;

        public OpenContextImpl(
                KeyedAsyncWaitOperator<?, ?, ?> operator,
                RuntimeContext runtimeContext,
                MailboxExecutor mailboxExecutor) {
            this.operator = Preconditions.checkNotNull(operator);
            this.runtimeContext = Preconditions.checkNotNull(runtimeContext);
            this.mailboxExecutor = mailboxExecutor;
        }

        @Override
        public void runOnMailboxThread(ThrowingRunnable<Exception> runnable) {
            mailboxExecutor.execute(runnable, "keyedAsyncWaitOperator.runOnMailboxThread");
        }

        @Override
        public RowData currentKey() {
            return (RowData) operator.getCurrentKey();
        }

        @Override
        public void setCurrentKey(RowData key) {
            operator.setCurrentKey(key);
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            return runtimeContext;
        }
    }
}
