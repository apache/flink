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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.TableAbstractCoUdfStreamOperator;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.AecRecord;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** This operator supports delta join in table layer. */
public class StreamingDeltaJoinOperator
        extends TableAbstractCoUdfStreamOperator<
                RowData, AsyncDeltaJoinRunner, AsyncDeltaJoinRunner>
        implements TwoInputStreamOperator<RowData, RowData, RowData>, BoundedMultiInput {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingDeltaJoinOperator.class);

    private static final long serialVersionUID = 1L;

    private static final int LEFT_INPUT_INDEX = 0;
    private static final int RIGHT_INPUT_INDEX = 1;

    private static final String STATE_NAME = "_delta_join_operator_async_wait_state_";

    private static final String METRIC_DELTA_JOIN_OP_TOTAL_IN_FLIGHT_NUM =
            "deltaJoinOpTotalInFlightNum";
    private static final String METRIC_DELTA_JOIN_ASYNC_IO_TIME = "deltaJoinAsyncIOTime";

    private final StreamRecord<RowData> leftEmptyStreamRecord;
    private final StreamRecord<RowData> rightEmptyStreamRecord;

    /** Selector to get join key from left input. */
    private final RowDataKeySelector leftJoinKeySelector;

    /** Selector to get join key from right input. */
    private final RowDataKeySelector rightJoinKeySelector;

    /** Timeout for the async collectors. */
    private final long timeout;

    /** Max number of inflight invocation. */
    private final int capacity;

    private final long leftSideCacheSize;

    private final long rightSideCacheSize;

    private transient boolean needDeepCopy;

    /** {@link TypeSerializer} for left side inputs while making snapshots. */
    private transient StreamElementSerializer<RowData> leftStreamElementSerializer;

    /** {@link TypeSerializer} for right side inputs while making snapshots. */
    private transient StreamElementSerializer<RowData> rightStreamElementSerializer;

    private transient TimestampedCollector<RowData> timestampedCollector;

    /**
     * Recovered input stream elements backed by keyed state.
     *
     * <p>{@code <LeftElementRecord, RightElementRecord, Watermark, InputIndex>}.
     */
    private transient ListState<Tuple4<StreamElement, StreamElement, StreamElement, Integer>>
            recoveredStreamElements;

    /** Structure to control the process order of input records. */
    private transient TableAsyncExecutionController<RowData, RowData, RowData>
            asyncExecutionController;

    private transient DeltaJoinCache cache;

    /** Mailbox executor used to yield while waiting for buffers to empty. */
    private final transient MailboxExecutor mailboxExecutor;

    private final boolean[] isInputEnded;

    private final transient AtomicInteger totalInflightNum = new AtomicInteger(0);

    // ---------------------------- Metrics -----------------------------------

    private final transient AtomicLong asyncIOTime = new AtomicLong(Long.MIN_VALUE);

    /**
     * Buffers {@link ReusableKeyedResultHandler} to avoid newInstance cost when processing elements
     * every time. We use {@link BlockingQueue} to make sure the head {@link
     * ReusableKeyedResultHandler}s are available.
     */
    private transient BlockingQueue<ReusableKeyedResultHandler> resultHandlerBuffer;

    /**
     * A Collection contains all KeyedResultHandlers in the runner which is used to invoke {@code
     * close()} on every ResultFuture. {@link #resultHandlerBuffer} may not contain all the
     * ResultFutures because ResultFutures will be polled from the buffer when processing.
     */
    private transient List<ReusableKeyedResultHandler> allResultHandlers;

    public StreamingDeltaJoinOperator(
            AsyncDeltaJoinRunner rightLookupTableAsyncFunction,
            AsyncDeltaJoinRunner leftLookupTableAsyncFunction,
            RowDataKeySelector leftJoinKeySelector,
            RowDataKeySelector rightJoinKeySelector,
            long timeout,
            int capacity,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            long leftSideCacheSize,
            long rightSideCacheSize,
            RowType leftStreamType,
            RowType rightStreamType) {
        // rightLookupTableAsyncFunction is an udx used for left records
        // leftLookupTableAsyncFunction is an udx used for right records
        super(rightLookupTableAsyncFunction, leftLookupTableAsyncFunction);
        this.leftJoinKeySelector = leftJoinKeySelector;
        this.rightJoinKeySelector = rightJoinKeySelector;
        this.timeout = timeout;
        this.capacity = capacity;
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = mailboxExecutor;
        this.isInputEnded = new boolean[2];
        this.leftSideCacheSize = leftSideCacheSize;
        this.rightSideCacheSize = rightSideCacheSize;
        this.leftEmptyStreamRecord =
                new StreamRecord<>(new GenericRowData(leftStreamType.getFieldCount()));
        this.rightEmptyStreamRecord =
                new StreamRecord<>(new GenericRowData(rightStreamType.getFieldCount()));
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<RowData>> output) {
        super.setup(containingTask, config, output);

        this.leftStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().getTypeSerializerIn(0, getUserCodeClassloader()));
        this.rightStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().getTypeSerializerIn(1, getUserCodeClassloader()));

        this.timestampedCollector = new TimestampedCollector<>(super.output);
        this.asyncExecutionController =
                new TableAsyncExecutionController<>(
                        this::invoke,
                        this::emitWatermark,
                        entry -> {
                            entry.emitResult(timestampedCollector);
                            totalInflightNum.decrementAndGet();
                        },
                        (entry) -> {
                            checkState(
                                    entry instanceof InputIndexAwareStreamRecordQueueEntry,
                                    "The entry type is " + entry.getClass().getSimpleName());
                            return ((InputIndexAwareStreamRecordQueueEntry) entry).inputIndex;
                        },
                        (record, inputIndex) -> {
                            RowDataKeySelector keySelector =
                                    isLeft(inputIndex) ? leftJoinKeySelector : rightJoinKeySelector;
                            return keySelector.getKey(record.getValue());
                        });

        this.cache = new DeltaJoinCache(leftSideCacheSize, rightSideCacheSize);

        leftTriggeredUserFunction.setCache(cache);
        rightTriggeredUserFunction.setCache(cache);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        TypeInformation<Tuple4<StreamElement, StreamElement, StreamElement, Integer>>
                stateTypeInfo =
                        Types.TUPLE(
                                TypeInformation.of(StreamElement.class),
                                TypeInformation.of(StreamElement.class),
                                TypeInformation.of(StreamElement.class),
                                BasicTypeInfo.INT_TYPE_INFO);

        Class<Tuple4<StreamElement, StreamElement, StreamElement, Integer>> type =
                stateTypeInfo.getTypeClass();

        TypeSerializer<?>[] stateElementSerializers =
                new TypeSerializer[] {
                    leftStreamElementSerializer,
                    rightStreamElementSerializer,
                    leftStreamElementSerializer, // watermark
                    IntSerializer.INSTANCE
                };
        TupleSerializer<Tuple4<StreamElement, StreamElement, StreamElement, Integer>>
                stateSerializer = new TupleSerializer<>(type, stateElementSerializers);
        recoveredStreamElements =
                context.getKeyedStateStore()
                        .getListState(new ListStateDescriptor<>(STATE_NAME, stateSerializer));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        asyncExecutionController.submitWatermark(mark);
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.needDeepCopy = getExecutionConfig().isObjectReuseEnabled() && !config.isChainStart();

        // register metrics
        // 1. aec metrics
        asyncExecutionController.registerMetrics(getRuntimeContext().getMetricGroup());
        // 2. delta-join operator metrics
        getRuntimeContext()
                .getMetricGroup()
                .gauge(METRIC_DELTA_JOIN_OP_TOTAL_IN_FLIGHT_NUM, totalInflightNum::get);
        getRuntimeContext()
                .getMetricGroup()
                .gauge(METRIC_DELTA_JOIN_ASYNC_IO_TIME, asyncIOTime::get);
        // 3. cache metric
        cache.registerMetrics(getRuntimeContext().getMetricGroup());

        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        this.resultHandlerBuffer = new ArrayBlockingQueue<>(capacity + 1);
        this.allResultHandlers = new ArrayList<>(capacity + 1);
        for (int i = 0; i < capacity + 1; i++) {
            ReusableKeyedResultHandler reusableKeyedResultHandler =
                    new ReusableKeyedResultHandler(resultHandlerBuffer);
            // add will throw exception immediately if the queue is full which should never happen
            resultHandlerBuffer.add(reusableKeyedResultHandler);
            allResultHandlers.add(reusableKeyedResultHandler);
        }

        // We cannot move the following code to #initializeState because they depend on
        // resultHandlerBuffer
        List<Object> keys = getAllStateKeys();
        for (Object key : keys) {
            setCurrentKey(key);
            triggerRecoveryProcess();
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element, LEFT_INPUT_INDEX);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element, RIGHT_INPUT_INDEX);
    }

    public void emitWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
    }

    public void invoke(AecRecord<RowData, RowData> element) throws Exception {
        final ReusableKeyedResultHandler resultHandler = resultHandlerBuffer.take();
        resultHandler.reset(element);
        boolean isLeft = isLeft(element.getInputIndex());
        // register a timeout for the entry if timeout is configured
        if (timeout > 0L) {
            resultHandler.registerTimeout(getProcessingTimeService(), timeout, isLeft);
        }
        if (isLeft) {
            leftTriggeredUserFunction.asyncInvoke(element.getRecord().getValue(), resultHandler);
        } else {
            rightTriggeredUserFunction.asyncInvoke(element.getRecord().getValue(), resultHandler);
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

    private void processElement(StreamRecord<RowData> element, int inputIndex) throws Exception {
        Preconditions.checkArgument(
                RowKind.INSERT == element.getValue().getRowKind()
                        || RowKind.UPDATE_AFTER == element.getValue().getRowKind(),
                "Currently, delta join only supports to consume insert record or update after record.");
        tryProcess();
        StreamRecord<RowData> record;
        boolean isLeft = isLeft(inputIndex);
        if (needDeepCopy) {
            if (isLeft) {
                //noinspection unchecked
                record = (StreamRecord<RowData>) leftStreamElementSerializer.copy(element);
            } else {
                //noinspection unchecked
                record = (StreamRecord<RowData>) rightStreamElementSerializer.copy(element);
            }
        } else {
            record = element;
        }
        asyncExecutionController.submitRecord(record, null, inputIndex);
    }

    @SuppressWarnings("unchecked")
    private void triggerRecoveryProcess() throws Exception {
        if (recoveredStreamElements != null) {
            for (Tuple4<StreamElement, StreamElement, StreamElement, Integer> tuple :
                    recoveredStreamElements.get()) {
                tryProcess();
                int inputIndex = Objects.requireNonNull(tuple.f3);
                if (isLeft(inputIndex)) {
                    asyncExecutionController.recovery(
                            (StreamRecord<RowData>) tuple.f0, (Watermark) tuple.f2, tuple.f3);
                } else {
                    asyncExecutionController.recovery(
                            (StreamRecord<RowData>) tuple.f1, (Watermark) tuple.f2, tuple.f3);
                }
            }
        }
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory factory)
            throws Exception {

        clearLegacyState();

        Map<RowData, Deque<AecRecord<RowData, RowData>>> pendingElements =
                asyncExecutionController.pendingElements();
        for (Map.Entry<RowData, Deque<AecRecord<RowData, RowData>>> entry :
                pendingElements.entrySet()) {
            RowData key = entry.getKey();
            setCurrentKey(key);
            Deque<AecRecord<RowData, RowData>> elements = entry.getValue();
            List<Tuple4<StreamElement, StreamElement, StreamElement, Integer>> storedData =
                    new ArrayList<>();
            for (AecRecord<RowData, RowData> aecRecord : elements) {
                if (isLeft(aecRecord.getInputIndex())) {
                    storedData.add(
                            Tuple4.of(
                                    aecRecord.getRecord(),
                                    rightEmptyStreamRecord,
                                    aecRecord.getEpoch().getWatermark(),
                                    aecRecord.getInputIndex()));
                } else {
                    storedData.add(
                            Tuple4.of(
                                    leftEmptyStreamRecord,
                                    aecRecord.getRecord(),
                                    aecRecord.getEpoch().getWatermark(),
                                    aecRecord.getInputIndex()));
                }
            }
            recoveredStreamElements.update(storedData);
        }
        return super.snapshotState(checkpointId, timestamp, checkpointOptions, factory);
    }

    private void clearLegacyState() {
        List<Object> allKeys = getAllStateKeys();
        for (Object key : allKeys) {
            setCurrentKey(key);
            recoveredStreamElements.clear();
        }
    }

    private List<Object> getAllStateKeys() {
        return getKeyedStateBackend()
                .getKeys(STATE_NAME, VoidNamespace.INSTANCE)
                .collect(Collectors.toList());
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

    @Override
    public void endInput(int inputId) throws Exception {
        isInputEnded[inputId - 1] = true;
        if (!allInputEnded()) {
            return;
        }
        // we should wait here for the data in flight to be finished. the reason is that the
        // timer not in running will be forbidden to fire after this, so that when the async
        // operation is stuck, it results in deadlock due to what the timeout timer is not fired
        waitInFlightInputsFinished();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (allResultHandlers != null) {
            for (ReusableKeyedResultHandler handler : allResultHandlers) {
                handler.close();
            }
        }
    }

    public void waitInFlightInputsFinished() throws InterruptedException {
        while (!allInflightFinished()) {
            mailboxExecutor.yield();
        }
    }

    @VisibleForTesting
    public TableAsyncExecutionController<RowData, RowData, RowData> getAsyncExecutionController() {
        return asyncExecutionController;
    }

    @VisibleForTesting
    public void setAsyncExecutionController(
            TableAsyncExecutionController<RowData, RowData, RowData> asyncExecutionController) {
        this.asyncExecutionController = asyncExecutionController;
    }

    private boolean allInputEnded() {
        return isInputEnded[0] && isInputEnded[1];
    }

    private static boolean isLeft(int inputIndex) {
        if (LEFT_INPUT_INDEX == inputIndex) {
            return true;
        } else if (RIGHT_INPUT_INDEX == inputIndex) {
            return false;
        }
        throw new IllegalArgumentException("Unknown input index: " + inputIndex);
    }

    private class ReusableKeyedResultHandler implements ResultFuture<RowData> {

        /**
         * A guard against ill-written AsyncFunction. Additional (parallel) invokations of {@link
         * #complete(Collection)} or {@link #completeExceptionally(Throwable)} will be ignored. This
         * guard also helps for cases where proper results and timeouts happen at the same time.
         */
        private final AtomicBoolean completed = new AtomicBoolean(false);

        private final BlockingQueue<ReusableKeyedResultHandler> resultHandlerBuffer;

        /** Optional timeout timer used to signal the timeout to the AsyncFunction. */
        private ScheduledFuture<?> timeoutTimer;

        /** Record for which this result handler exists. */
        private AecRecord<RowData, RowData> inputRecord;

        /**
         * The handle received from the queue to update the entry. Should only be used to inject the
         * result; exceptions are handled here.
         */
        private final InputIndexAwareStreamRecordQueueEntry reusedQueueEntry;

        private volatile long startTime;

        ReusableKeyedResultHandler(BlockingQueue<ReusableKeyedResultHandler> resultHandlerBuffer) {
            this.resultHandlerBuffer = resultHandlerBuffer;
            this.reusedQueueEntry = new InputIndexAwareStreamRecordQueueEntry();
        }

        public void reset(AecRecord<RowData, RowData> inputAecRecord) {
            this.inputRecord = inputAecRecord;
            this.startTime = System.currentTimeMillis();
            this.timeoutTimer = null;
            this.reusedQueueEntry.reset(inputAecRecord.getRecord(), inputRecord.getInputIndex());
            this.completed.set(false);
        }

        @Override
        public void complete(Collection<RowData> results) {
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

            LOG.error("An error happen when doing delta join", error);

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

        public void close() {
            if (timeoutTimer != null) {
                timeoutTimer.cancel(true);
            }
        }

        /**
         * Unsupported, because the containing classes are AsyncFunctions which don't have access to
         * the mailbox to invoke from the caller thread.
         */
        @Override
        public void complete(CollectionSupplier<RowData> supplier) {
            throw new UnsupportedOperationException();
        }

        private void processInMailbox(Collection<RowData> results) {
            // move further processing into the mailbox thread
            mailboxExecutor.execute(
                    () -> processResults(results),
                    "Result in AsyncWaitOperator of input %s",
                    results);
        }

        private void processResults(Collection<RowData> results) throws Exception {
            asyncIOTime.set(System.currentTimeMillis() - startTime);

            // Cancel the timer once we've completed the stream record buffer entry. This will
            // remove the registered timer task
            if (timeoutTimer != null) {
                // canceling in mailbox thread avoids
                // https://issues.apache.org/jira/browse/FLINK-13635
                timeoutTimer.cancel(true);
            }

            // update the queue entry with the result
            reusedQueueEntry.complete(results);
            asyncExecutionController.completeRecord(reusedQueueEntry, inputRecord);

            try {
                // put this handler to the queue to avoid this handler is used
                // again before results in the handler is not consumed.
                resultHandlerBuffer.put(this);
            } catch (InterruptedException e) {
                completeExceptionally(e);
            }
        }

        private void registerTimeout(
                ProcessingTimeService processingTimeService, long timeout, boolean isLeft) {
            timeoutTimer =
                    registerTimer(processingTimeService, timeout, VOID -> timerTriggered(isLeft));
        }

        private void timerTriggered(boolean isLeft) throws Exception {
            if (!completed.get()) {
                if (isLeft) {
                    leftTriggeredUserFunction.timeout(inputRecord.getRecord().getValue(), this);
                } else {
                    rightTriggeredUserFunction.timeout(inputRecord.getRecord().getValue(), this);
                }
            }
        }
    }

    /** A {@link StreamElementQueueEntry} with the input index. */
    @VisibleForTesting
    public static class InputIndexAwareStreamRecordQueueEntry
            implements StreamElementQueueEntry<RowData> {

        private StreamRecord<?> inputRecord;

        private Collection<RowData> completedElements;

        private int inputIndex;

        public InputIndexAwareStreamRecordQueueEntry() {}

        public void reset(StreamRecord<?> inputRecord, int inputIndex) {
            this.inputRecord = Preconditions.checkNotNull(inputRecord);
            this.inputIndex = inputIndex;
            this.completedElements = null;
        }

        @Override
        public boolean isDone() {
            return completedElements != null;
        }

        @Nonnull
        @Override
        public StreamRecord<?> getInputElement() {
            return inputRecord;
        }

        @Override
        public void emitResult(TimestampedCollector<RowData> output) {
            output.setTimestamp(inputRecord);
            for (RowData r : completedElements) {
                output.collect(r);
            }
        }

        @Override
        public void complete(Collection<RowData> result) {
            this.completedElements = Preconditions.checkNotNull(result);
        }

        public int getInputIndex() {
            return inputIndex;
        }
    }
}
