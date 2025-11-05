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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.AecRecord;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
public class TableKeyedAsyncWaitOperator<IN, OUT, KEY>
        extends AbstractUdfStreamOperator<OUT, AsyncFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {

    protected static final Logger LOG = LoggerFactory.getLogger(TableKeyedAsyncWaitOperator.class);

    private static final long serialVersionUID = 1L;

    private static final String STATE_NAME = "_keyed_async_wait_operator_state_";

    /** Selector to get ordered keys from input record. */
    private final KeySelector<IN, KEY> keySelector;

    /** Timeout for the async collectors. */
    private final long timeout;

    /** Max number of inflight invocation. */
    private final transient int capacity;

    /** {@link TypeSerializer} for inputs while making snapshots. */
    protected transient StreamElementSerializer<IN> inStreamElementSerializer;

    protected transient TimestampedCollector<OUT> timestampedCollector;

    protected transient boolean needDeepCopy;

    /** Structure to control the process order of input records. */
    private transient TableAsyncExecutionController<IN, OUT, KEY> asyncExecutionController;

    /** Recovered input stream elements backed by keyed state. */
    private transient ListState<Tuple2<StreamElement, StreamElement>> recoveredStreamElements;

    /** Mailbox executor used to yield while waiting for buffers to empty. */
    private final transient MailboxExecutor mailboxExecutor;

    /** Number of inputs which is invoked for lookup but do not output until now. */
    private final transient AtomicInteger totalInflightNum;

    public TableKeyedAsyncWaitOperator(
            AsyncFunction<IN, OUT> asyncFunction,
            KeySelector<IN, KEY> keySelector,
            long timeout,
            int capacity,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {
        super(asyncFunction);
        this.timeout = timeout;
        this.keySelector = keySelector;
        Preconditions.checkArgument(
                capacity > 0,
                "The maxInflight of concurrent async operation should be greater than 0.");
        this.capacity = capacity;
        this.totalInflightNum = new AtomicInteger(0);
        this.mailboxExecutor = mailboxExecutor;
        this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);

        this.inStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().getTypeSerializerIn(0, getUserCodeClassloader()));

        this.timestampedCollector = new TimestampedCollector<>(super.output);
        this.asyncExecutionController =
                new TableAsyncExecutionController<>(
                        this::invoke,
                        super::processWatermark,
                        entry -> {
                            entry.emitResult(timestampedCollector);
                            totalInflightNum.decrementAndGet();
                        },
                        // the drive side is always left side
                        entry -> 0,
                        (record, inputIndex) -> keySelector.getKey(record.getValue()));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        TypeSerializer[] elementSerializers =
                new TypeSerializer[] {inStreamElementSerializer, inStreamElementSerializer};
        TypeInformation<Tuple2<StreamElement, StreamElement>> typeInfo =
                Types.TUPLE(
                        TypeInformation.of(StreamElement.class),
                        TypeInformation.of(StreamElement.class));
        Class<Tuple2<StreamElement, StreamElement>> type = typeInfo.getTypeClass();
        TupleSerializer<Tuple2<StreamElement, StreamElement>> stateSerializer =
                new TupleSerializer<>(type, elementSerializers);
        recoveredStreamElements =
                context.getKeyedStateStore()
                        .getListState(new ListStateDescriptor<>(STATE_NAME, stateSerializer));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void open() throws Exception {
        super.open();
        this.needDeepCopy = getExecutionConfig().isObjectReuseEnabled() && !config.isChainStart();
        List<KEY> keys =
                (List<KEY>)
                        getKeyedStateBackend()
                                .getKeys(STATE_NAME, VoidNamespace.INSTANCE)
                                .collect(Collectors.toList());
        for (KEY key : keys) {
            setCurrentKey(key);
            triggerRecoveryProcess();
        }
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        tryProcess();
        StreamRecord<IN> element;
        if (needDeepCopy) {
            //noinspection unchecked
            element = (StreamRecord<IN>) inStreamElementSerializer.copy(record);
        } else {
            element = record;
        }
        asyncExecutionController.submitRecord(element, null, 0);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        asyncExecutionController.submitWatermark(mark);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {
        Map<KEY, Deque<AecRecord<IN, OUT>>> pendingElements =
                asyncExecutionController.pendingElements();
        for (Map.Entry<KEY, Deque<AecRecord<IN, OUT>>> entry : pendingElements.entrySet()) {
            KEY key = entry.getKey();
            setCurrentKey(key);
            Deque<AecRecord<IN, OUT>> elements = entry.getValue();
            List<Tuple2<StreamElement, StreamElement>> value = new ArrayList<>();
            for (AecRecord<IN, OUT> aecRecord : elements) {
                value.add(Tuple2.of(aecRecord.getRecord(), aecRecord.getEpoch().getWatermark()));
            }
            recoveredStreamElements.update(value);
        }
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
    }

    @Override
    public void endInput() throws Exception {
        // we should wait here for the data in flight to be finished. the reason is that the
        // timer not in running will be forbidden to fire after this, so that when the async
        // operation is stuck, it results in deadlock due to what the timeout timer is not fired
        waitInFlightInputsFinished();
    }

    public void invoke(AecRecord<IN, OUT> element) throws Exception {
        final KeyedResultHandler resultHandler =
                new KeyedResultHandler(element, new StreamRecordQueueEntry<>(element.getRecord()));
        // register a timeout for the entry if timeout is configured
        if (timeout > 0L) {
            resultHandler.registerTimeout(getProcessingTimeService(), timeout);
        }
        userFunction.asyncInvoke(element.getRecord().getValue(), resultHandler);
    }

    public void emitWatermark(Watermark mark) {
        // place the action of emission in mailbox instead do it right now
        timestampedCollector.emitWatermark(mark);
    }

    public void waitInFlightInputsFinished() throws InterruptedException {
        while (!allInflightFinished()) {
            mailboxExecutor.yield();
        }
    }

    @SuppressWarnings("unchecked")
    private void triggerRecoveryProcess() throws Exception {
        if (recoveredStreamElements != null) {
            for (Tuple2<StreamElement, StreamElement> tuple : recoveredStreamElements.get()) {
                tryProcess();
                asyncExecutionController.recovery(
                        (StreamRecord<IN>) tuple.f0, (Watermark) tuple.f1, 0);
            }
        }
    }

    private void tryProcess() throws Exception {
        while (totalInflightNum.get() >= capacity) {
            LOG.debug(
                    "Failed to put element into asyncExecutionController because totalInflightNum is greater or equal to "
                            + "maxInflight ({}/{}).",
                    totalInflightNum.get(),
                    capacity);
            mailboxExecutor.yield();
        }
        totalInflightNum.incrementAndGet();
    }

    private class KeyedResultHandler implements ResultFuture<OUT> {

        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        protected ScheduledFuture<?> timeoutTimer;

        /** Record for which this result handler exists. Used only to report errors. */
        protected final AecRecord<IN, OUT> inputRecord;

        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        protected final ResultFuture<OUT> resultFuture;

        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invokations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        protected final AtomicBoolean completed = new AtomicBoolean(false);

        KeyedResultHandler(AecRecord<IN, OUT> inputRecord, ResultFuture<OUT> resultFuture) {
            this.inputRecord = inputRecord;
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
                                    "Could not complete the stream element: " + inputRecord + '.',
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

        private void processInMailbox(Collection<OUT> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
        }

        private void processResults(Collection<OUT> results) throws Exception {
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
            asyncExecutionController.completeRecord(
                    (StreamRecordQueueEntry<OUT>) resultFuture, inputRecord);
        }

        private void registerTimeout(ProcessingTimeService processingTimeService, long timeout) {
            timeoutTimer = registerTimer(processingTimeService, timeout, t -> timerTriggered());
        }

        private void timerTriggered() throws Exception {
            if (!completed.get()) {
                userFunction.timeout((IN) inputRecord.getRecord().getValue(), this);
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

    private boolean allInflightFinished() {
        return totalInflightNum.get() == 0;
    }

    public KEY getKey(AecRecord<IN, OUT> aecRecord) {
        StreamRecord<IN> record = aecRecord.getRecord();
        try {
            return keySelector.getKey(record.getValue());
        } catch (Exception e) {
            throw new RuntimeException("Unable to retrieve key from record " + record, e);
        }
    }

    @VisibleForTesting
    public TableAsyncExecutionController<IN, OUT, KEY> getAsyncExecutionController() {
        return asyncExecutionController;
    }
}
