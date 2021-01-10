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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.JavaSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.RunnableWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The operator that reads the {@link TimestampedFileInputSplit splits} received from the preceding
 * {@link ContinuousFileMonitoringFunction}. Contrary to the {@link
 * ContinuousFileMonitoringFunction} which has a parallelism of 1, this operator can have DOP > 1.
 *
 * <p>This implementation uses {@link MailboxExecutor} to execute each action and state machine
 * approach. The workflow is the following:
 *
 * <ol>
 *   <li>start in {@link ReaderState#IDLE IDLE}
 *   <li>upon receiving a split add it to the queue, switch to {@link ReaderState#OPENING OPENING}
 *       and enqueue a {@link org.apache.flink.streaming.runtime.tasks.mailbox.Mail mail} to process
 *       it
 *   <li>open file, switch to {@link ReaderState#READING READING}, read one record, re-enqueue self
 *   <li>if no more records or splits available, switch back to {@link ReaderState#IDLE IDLE}
 * </ol>
 *
 * <p>On close:
 *
 * <ol>
 *   <li>if {@link ReaderState#IDLE IDLE} then close immediately
 *   <li>otherwise switch to {@link ReaderState#CLOSING CLOSING}, call {@link
 *       MailboxExecutor#yield() yield} in a loop until state is {@link ReaderState#CLOSED CLOSED}
 *   <li>{@link MailboxExecutor#yield() yield()} causes remaining records (and splits) to be
 *       processed in the same way as above
 * </ol>
 *
 * <p>Using {@link MailboxExecutor} allows to avoid explicit synchronization. At most one mail
 * should be enqueued at any given time.
 *
 * <p>Using FSM approach allows to explicitly define states and enforce {@link
 * ReaderState#VALID_TRANSITIONS transitions} between them.
 */
@Internal
public class ContinuousFileReaderOperator<OUT, T extends TimestampedInputSplit>
        extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<T, OUT>, OutputTypeConfigurable<OUT> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ContinuousFileReaderOperator.class);

    private enum ReaderState {
        IDLE {
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op) {
                throw new IllegalStateException("not processing any records in IDLE state");
            }
        },
        /** A message is enqueued to process split, but no split is opened. */
        OPENING { // the split was added and message to itself was enqueued to process it
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op)
                    throws IOException {
                if (op.splits.isEmpty()) {
                    op.switchState(ReaderState.IDLE);
                    return false;
                } else {
                    ((ContinuousFileReaderOperator) op).loadSplit(op.splits.poll());
                    op.switchState(ReaderState.READING);
                    return true;
                }
            }
        },
        /** A message is enqueued to process split and its processing was started. */
        READING {
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op) {
                return true;
            }

            @Override
            public void onNoMoreData(ContinuousFileReaderOperator<?, ?> op) {
                op.switchState(ReaderState.IDLE);
            }
        },
        /**
         * No further processing can be done; only state disposal transition to {@link #CLOSED}
         * allowed.
         */
        FAILED {
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op) {
                throw new IllegalStateException("not processing any records in ERRORED state");
            }
        },
        /**
         * {@link #close()} was called but unprocessed data (records and splits) remains and needs
         * to be processed. {@link #close()} caller is blocked.
         */
        CLOSING {
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op)
                    throws IOException {
                if (op.currentSplit == null && !op.splits.isEmpty()) {
                    ((ContinuousFileReaderOperator) op).loadSplit(op.splits.poll());
                }
                return true;
            }

            @Override
            public void onNoMoreData(ContinuousFileReaderOperator<?, ?> op) {
                // need one more mail to unblock possible yield() in close() method (todo: wait with
                // timeout in yield)
                op.enqueueProcessRecord();
                op.switchState(CLOSED);
            }
        },
        CLOSED {
            @Override
            public boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op) {
                LOG.warn("not processing any records while closed");
                return false;
            }
        };

        private static final Set<ReaderState> ACCEPT_SPLITS = EnumSet.of(IDLE, OPENING, READING);
        /** Possible transition FROM each state. */
        private static final Map<ReaderState, Set<ReaderState>> VALID_TRANSITIONS;

        static {
            Map<ReaderState, Set<ReaderState>> tmpTransitions = new HashMap<>();
            tmpTransitions.put(IDLE, EnumSet.of(OPENING, CLOSED, FAILED));
            tmpTransitions.put(OPENING, EnumSet.of(READING, CLOSING, FAILED));
            tmpTransitions.put(READING, EnumSet.of(IDLE, OPENING, CLOSING, FAILED));
            tmpTransitions.put(CLOSING, EnumSet.of(CLOSED, FAILED));
            tmpTransitions.put(FAILED, EnumSet.of(CLOSED));
            tmpTransitions.put(CLOSED, EnumSet.noneOf(ReaderState.class));
            VALID_TRANSITIONS = new EnumMap<>(tmpTransitions);
        }

        public boolean isAcceptingSplits() {
            return ACCEPT_SPLITS.contains(this);
        }

        public final boolean isTerminal() {
            return this == CLOSED;
        }

        public boolean canSwitchTo(ReaderState next) {
            return VALID_TRANSITIONS
                    .getOrDefault(this, EnumSet.noneOf(ReaderState.class))
                    .contains(next);
        }

        /**
         * Prepare to process new record OR split.
         *
         * @return true if should read the record
         */
        public abstract boolean prepareToProcessRecord(ContinuousFileReaderOperator<?, ?> op)
                throws IOException;

        public void onNoMoreData(ContinuousFileReaderOperator<?, ?> op) {}
    }

    private transient InputFormat<OUT, ? super T> format;
    private TypeSerializer<OUT> serializer;
    private transient MailboxExecutorImpl executor;
    private transient OUT reusedRecord;
    private transient SourceFunction.SourceContext<OUT> sourceContext;
    private transient ListState<T> checkpointedState;
    /** MUST only be changed via {@link #switchState(ReaderState) switchState}. */
    private transient ReaderState state = ReaderState.IDLE;

    private transient PriorityQueue<T> splits = new PriorityQueue<>();
    private transient T
            currentSplit; // can't work just on queue tail because it can change because it's PQ
    private transient Counter completedSplitsCounter;

    private final transient RunnableWithException processRecordAction =
            () -> {
                try {
                    processRecord();
                } catch (Exception e) {
                    switchState(ReaderState.FAILED);
                    throw e;
                }
            };

    ContinuousFileReaderOperator(
            InputFormat<OUT, ? super T> format,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {

        this.format = checkNotNull(format);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.executor = (MailboxExecutorImpl) checkNotNull(mailboxExecutor);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        checkState(checkpointedState == null, "The reader state has already been initialized.");

        // We are using JavaSerializer from the flink-runtime module here. This is very naughty and
        // we shouldn't be doing it because ideally nothing in the API modules/connector depends
        // directly on flink-runtime. We are doing it here because we need to maintain backwards
        // compatibility with old state and because we will have to rework/remove this code soon.
        checkpointedState =
                context.getOperatorStateStore()
                        .getListState(new ListStateDescriptor<>("splits", new JavaSerializer<>()));

        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        if (!context.isRestored()) {
            LOG.info(
                    "No state to restore for the {} (taskIdx={}).",
                    getClass().getSimpleName(),
                    subtaskIdx);
            return;
        }

        LOG.info(
                "Restoring state for the {} (taskIdx={}).", getClass().getSimpleName(), subtaskIdx);

        splits = splits == null ? new PriorityQueue<>() : splits;
        for (T split : checkpointedState.get()) {
            splits.add(split);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} (taskIdx={}) restored {}.", getClass().getSimpleName(), subtaskIdx, splits);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();

        checkState(
                this.serializer != null,
                "The serializer has not been set. "
                        + "Probably the setOutputType() was not called. Please report it.");

        this.state = ReaderState.IDLE;
        if (this.format instanceof RichInputFormat) {
            ((RichInputFormat) this.format).setRuntimeContext(getRuntimeContext());
        }
        this.format.configure(new Configuration());

        this.sourceContext =
                StreamSourceContexts.getSourceContext(
                        getOperatorConfig().getTimeCharacteristic(),
                        getProcessingTimeService(),
                        new Object(), // no actual locking needed
                        getContainingTask().getStreamStatusMaintainer(),
                        output,
                        getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
                        -1);

        this.reusedRecord = serializer.createInstance();
        this.completedSplitsCounter = getMetricGroup().counter("numSplitsProcessed");
        this.splits = this.splits == null ? new PriorityQueue<>() : this.splits;

        if (!splits.isEmpty()) {
            enqueueProcessRecord();
        }
    }

    @Override
    public void processElement(StreamRecord<T> element) throws Exception {
        Preconditions.checkState(state.isAcceptingSplits());
        splits.offer(element.getValue());
        if (state == ReaderState.IDLE) {
            enqueueProcessRecord();
        }
    }

    private void enqueueProcessRecord() {
        Preconditions.checkState(
                !state.isTerminal(), "can't enqueue mail in terminal state %s", state);
        executor.execute(processRecordAction, "ContinuousFileReaderOperator");
        if (state == ReaderState.IDLE) {
            switchState(ReaderState.OPENING);
        }
    }

    private void processRecord() throws IOException {
        do {
            if (!state.prepareToProcessRecord(this)) {
                return;
            }

            readAndCollectRecord();

            if (format.reachedEnd()) {
                onSplitProcessed();
                return;
            }
        } while (executor
                .isIdle()); // todo: consider moving this loop into MailboxProcessor (return boolean
        // "re-execute" from enqueued action)
        enqueueProcessRecord();
    }

    private void onSplitProcessed() throws IOException {
        completedSplitsCounter.inc();
        LOG.debug("split {} processed: {}", completedSplitsCounter.getCount(), currentSplit);
        format.close();
        currentSplit = null;

        if (splits.isEmpty()) {
            state.onNoMoreData(this);
            return;
        }

        if (state == ReaderState.READING) {
            switchState(ReaderState.OPENING);
        }

        enqueueProcessRecord();
    }

    private void readAndCollectRecord() throws IOException {
        Preconditions.checkState(
                state == ReaderState.READING || state == ReaderState.CLOSING,
                "can't process record in state %s",
                state);
        if (format.reachedEnd()) {
            return;
        }
        OUT out = format.nextRecord(this.reusedRecord);
        if (out != null) {
            sourceContext.collect(out);
        }
    }

    private void loadSplit(T split) throws IOException {
        Preconditions.checkState(
                state != ReaderState.READING && state != ReaderState.CLOSED,
                "can't load split in state %s",
                state);
        Preconditions.checkNotNull(split, "split is null");
        LOG.debug("load split: {}", split);
        currentSplit = split;
        if (this.format instanceof RichInputFormat) {
            ((RichInputFormat) this.format).openInputFormat();
        }
        if (format instanceof CheckpointableInputFormat && currentSplit.getSplitState() != null) {
            // recovering after a node failure with an input
            // format that supports resetting the offset
            ((CheckpointableInputFormat<T, Serializable>) format)
                    .reopen(currentSplit, currentSplit.getSplitState());
        } else {
            // we either have a new split, or we recovered from a node
            // failure but the input format does not support resetting the offset.
            format.open(currentSplit);
        }

        // reset the restored state to null for the next iteration
        currentSplit.resetSplitState();
    }

    private void switchState(ReaderState newState) {
        if (state != newState) {
            Preconditions.checkState(
                    state.canSwitchTo(newState),
                    "can't switch state from terminal state %s to %s",
                    state,
                    newState);
            LOG.debug("switch state: {} -> {}", state, newState);
            state = newState;
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // we do nothing because we emit our own watermarks if needed.
    }

    @Override
    public void dispose() throws Exception {
        Exception e = null;
        if (state != ReaderState.CLOSED) {
            try {
                cleanUp();
            } catch (Exception ex) {
                e = ex;
            }
        }
        {
            checkpointedState = null;
            completedSplitsCounter = null;
            currentSplit = null;
            executor = null;
            format = null;
            sourceContext = null;
            reusedRecord = null;
            serializer = null;
            splits = null;
        }
        try {
            super.dispose();
        } catch (Exception ex) {
            e = ExceptionUtils.firstOrSuppressed(ex, e);
        }
        if (e != null) {
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        LOG.debug("closing");
        super.close();

        switch (state) {
            case IDLE:
                switchState(ReaderState.CLOSED);
                break;
            case CLOSED:
                LOG.warn("operator is already closed, doing nothing");
                return;
            default:
                switchState(ReaderState.CLOSING);
                while (!state.isTerminal()) {
                    executor.yield();
                }
        }

        try {
            sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
        } catch (Exception e) {
            LOG.warn("unable to emit watermark while closing", e);
        }

        cleanUp();
    }

    private void cleanUp() throws Exception {
        LOG.debug("cleanup, state={}", state);

        RunnableWithException[] runClose = {
            () -> sourceContext.close(),
            () -> output.close(),
            () -> format.close(),
            () -> {
                if (this.format instanceof RichInputFormat) {
                    ((RichInputFormat) this.format).closeInputFormat();
                }
            }
        };
        Exception firstException = null;

        for (RunnableWithException r : runClose) {
            try {
                r.run();
            } catch (Exception e) {
                firstException = ExceptionUtils.firstOrSuppressed(e, firstException);
            }
        }
        currentSplit = null;
        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        checkState(
                checkpointedState != null, "The operator state has not been properly initialized.");

        int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

        checkpointedState.clear();

        List<T> readerState = getReaderState();

        try {
            for (T split : readerState) {
                checkpointedState.add(split);
            }
        } catch (Exception e) {
            checkpointedState.clear();

            throw new Exception(
                    "Could not add timestamped file input splits to to operator "
                            + "state backend of operator "
                            + getOperatorName()
                            + '.',
                    e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} (taskIdx={}) checkpointed {} splits: {}.",
                    getClass().getSimpleName(),
                    subtaskIdx,
                    readerState.size(),
                    readerState);
        }
    }

    private List<T> getReaderState() throws IOException {
        List<T> snapshot = new ArrayList<>(splits.size());
        if (currentSplit != null) {
            if (this.format instanceof CheckpointableInputFormat && state == ReaderState.READING) {
                Serializable formatState =
                        ((CheckpointableInputFormat<T, Serializable>) this.format)
                                .getCurrentState();
                this.currentSplit.setSplitState(formatState);
            }
            snapshot.add(this.currentSplit);
        }
        snapshot.addAll(splits);
        return snapshot;
    }

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        this.serializer = outTypeInfo.createSerializer(executionConfig);
    }
}
