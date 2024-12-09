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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.SupportsWriterState;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink2.Sink}. It also has a way to process committables with the
 * same parallelism or send them downstream to a {@link CommitterOperator} with a different
 * parallelism.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <InputT> the type of the committable
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
class SinkWriterOperator<InputT, CommT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<InputT, CommittableMessage<CommT>>, BoundedOneInput {

    /**
     * To support state migrations from 1.14 where the sinkWriter and committer where part of the
     * same operator.
     */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    @Nullable private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final List<CommT> legacyCommittables = new ArrayList<>();

    /**
     * Used to remember that EOI has already happened so that we don't emit the last committables of
     * the final checkpoints twice.
     */
    private static final ListStateDescriptor<Boolean> END_OF_INPUT_STATE_DESC =
            new ListStateDescriptor<>("end_of_input_state", BooleanSerializer.INSTANCE);

    /** The runtime information of the input element. */
    private final Context<InputT> context;

    private final boolean emitDownstream;

    // ------------------------------- runtime fields ---------------------------------------

    /** We listen to this ourselves because we don't have an {@link InternalTimerService}. */
    private Long currentWatermark = Long.MIN_VALUE;

    private SinkWriter<InputT> sinkWriter;

    private final SinkWriterStateHandler<InputT> writerStateHandler;

    private final MailboxExecutor mailboxExecutor;

    private boolean endOfInput = false;

    /**
     * Remembers the endOfInput state for (final) checkpoints iff the operator emits committables.
     */
    @Nullable private ListState<Boolean> endOfInputState;

    SinkWriterOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters,
            Sink<InputT> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {
        super(parameters);
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.context = new Context<>();
        this.emitDownstream = sink instanceof SupportsCommitter;

        if (sink instanceof SupportsWriterState) {
            writerStateHandler =
                    new StatefulSinkWriterStateHandler<>((SupportsWriterState<InputT, ?>) sink);
        } else {
            writerStateHandler = new StatelessSinkWriterStateHandler<>(sink);
        }

        if (sink instanceof SupportsCommitter) {
            committableSerializer = ((SupportsCommitter<CommT>) sink).getCommittableSerializer();
        } else {
            committableSerializer = null;
        }
    }

    @Override
    protected void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        // Metric "numRecordsOut" & "numBytesOut" is defined as the total number of records/bytes
        // written to the external system in FLIP-33, reuse them for task to account for traffic
        // with external system
        this.metrics.getIOMetricGroup().reuseOutputMetricsForTask();
        this.metrics.getIOMetricGroup().reuseBytesOutputMetricsForTask();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        WriterInitContext initContext = createInitContext(context.getRestoredCheckpointId());
        if (context.isRestored()) {
            if (committableSerializer != null) {
                final ListState<List<CommT>> legacyCommitterState =
                        new SimpleVersionedListState<>(
                                context.getOperatorStateStore()
                                        .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                                new SinkV1WriterCommittableSerializer<>(committableSerializer));
                legacyCommitterState.get().forEach(legacyCommittables::addAll);
                // FLINK-33437: clear legacy state
                legacyCommitterState.clear();
            }
        }

        sinkWriter = writerStateHandler.createWriter(initContext, context);

        if (emitDownstream) {
            // Figure out if we have seen end of input before and if we can suppress creating
            // transactions and sending them downstream to the CommitterOperator. We have the
            // following
            // cases:
            // 1. state is empty:
            //   - First time initialization
            //   - Restoring from a previous version of Flink that didn't handle EOI
            //   - Upscaled from a final or regular checkpoint
            // In all cases, we regularly handle EOI, potentially resulting in duplicate summaries
            // that the CommitterOperator needs to handle.
            // 2. state is not empty:
            //   - This implies Flink restores from a version that handles EOI.
            //   - If there is one entry, no rescaling happened (for this subtask), so if it's true,
            //     we recover from a final checkpoint (for this subtask) and can ignore another EOI
            //     else we have a regular checkpoint.
            //   - If there are multiple entries, Flink downscaled, and we need to check if all are
            //     true and do the same as above. As soon as one entry is false, we regularly start
            //     the writer and potentially emit duplicate summaries if we indeed recovered from a
            //     final checkpoint.
            endOfInputState = context.getOperatorStateStore().getListState(END_OF_INPUT_STATE_DESC);
            ArrayList<Boolean> previousState = Lists.newArrayList(endOfInputState.get());
            endOfInput = !previousState.isEmpty() && !previousState.contains(false);
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        writerStateHandler.snapshotState(context.getCheckpointId());
        if (endOfInputState != null) {
            endOfInputState.clear();
            endOfInputState.add(this.endOfInput);
        }
    }

    @Override
    public void processElement(StreamRecord<InputT> element) throws Exception {
        checkState(!endOfInput, "Received element after endOfInput: %s", element);
        context.element = element;
        sinkWriter.write(element.getValue(), context);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        if (!endOfInput) {
            sinkWriter.flush(false);
            emitCommittables(checkpointId);
        }
        // no records are expected to emit after endOfInput
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        super.processWatermark(mark);
        this.currentWatermark = mark.getTimestamp();
        sinkWriter.writeWatermark(
                new org.apache.flink.api.common.eventtime.Watermark(mark.getTimestamp()));
    }

    @Override
    public void endInput() throws Exception {
        if (!endOfInput) {
            endOfInput = true;
            if (endOfInputState != null) {
                endOfInputState.add(true);
            }
            sinkWriter.flush(true);
            emitCommittables(CommittableMessage.EOI);
        }
    }

    private void emitCommittables(long checkpointId) throws IOException, InterruptedException {
        if (!emitDownstream) {
            // To support SinkV1 topologies with only a writer we have to call prepareCommit
            // although no committables are forwarded
            if (sinkWriter instanceof CommittingSinkWriter) {
                ((CommittingSinkWriter<?, ?>) sinkWriter).prepareCommit();
            }
            return;
        }
        Collection<CommT> committables =
                ((CommittingSinkWriter<?, CommT>) sinkWriter).prepareCommit();
        StreamingRuntimeContext runtimeContext = getRuntimeContext();
        final int indexOfThisSubtask = runtimeContext.getTaskInfo().getIndexOfThisSubtask();
        final int numberOfParallelSubtasks =
                runtimeContext.getTaskInfo().getNumberOfParallelSubtasks();

        // Emit only committable summary if there are legacy committables
        if (!legacyCommittables.isEmpty()) {
            checkState(checkpointId > WriterInitContext.INITIAL_CHECKPOINT_ID);
            emit(
                    indexOfThisSubtask,
                    numberOfParallelSubtasks,
                    WriterInitContext.INITIAL_CHECKPOINT_ID,
                    legacyCommittables);
            legacyCommittables.clear();
        }
        emit(indexOfThisSubtask, numberOfParallelSubtasks, checkpointId, committables);
    }

    @Override
    public void close() throws Exception {
        closeAll(sinkWriter, super::close);
    }

    private void emit(
            int indexOfThisSubtask,
            int numberOfParallelSubtasks,
            long checkpointId,
            Collection<CommT> committables) {
        emit(
                new StreamRecord<>(
                        new CommittableSummary<>(
                                indexOfThisSubtask,
                                numberOfParallelSubtasks,
                                checkpointId,
                                committables.size(),
                                committables.size(),
                                0)));
        for (CommT committable : committables) {
            emit(
                    new StreamRecord<>(
                            new CommittableWithLineage<>(
                                    committable, checkpointId, indexOfThisSubtask)));
        }
    }

    private void emit(StreamRecord<CommittableMessage<CommT>> message) {
        LOG.debug("Sending message to committer: {}", message);
        output.collect(message);
    }

    private WriterInitContext createInitContext(OptionalLong restoredCheckpointId) {
        return new InitContextImpl(
                getRuntimeContext(),
                processingTimeService,
                mailboxExecutor,
                InternalSinkWriterMetricGroup.wrap(getMetricGroup()),
                getOperatorConfig(),
                restoredCheckpointId);
    }

    private class Context<IN> implements SinkWriter.Context {

        private StreamRecord<IN> element;

        @Override
        public long currentWatermark() {
            return currentWatermark;
        }

        @Override
        public Long timestamp() {
            if (element.hasTimestamp()
                    && element.getTimestamp() != TimestampAssigner.NO_TIMESTAMP) {
                return element.getTimestamp();
            }
            return null;
        }
    }

    private static class InitContextImpl extends InitContextBase implements WriterInitContext {

        private final ProcessingTimeService processingTimeService;

        private final MailboxExecutor mailboxExecutor;

        private final SinkWriterMetricGroup metricGroup;

        private final StreamConfig operatorConfig;

        public InitContextImpl(
                StreamingRuntimeContext runtimeContext,
                ProcessingTimeService processingTimeService,
                MailboxExecutor mailboxExecutor,
                SinkWriterMetricGroup metricGroup,
                StreamConfig operatorConfig,
                OptionalLong restoredCheckpointId) {
            super(runtimeContext, restoredCheckpointId);
            this.mailboxExecutor = checkNotNull(mailboxExecutor);
            this.processingTimeService = checkNotNull(processingTimeService);
            this.metricGroup = checkNotNull(metricGroup);
            this.operatorConfig = checkNotNull(operatorConfig);
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return getRuntimeContext().getUserCodeClassLoader();
                }

                @Override
                public void registerReleaseHookIfAbsent(
                        String releaseHookName, Runnable releaseHook) {
                    getRuntimeContext()
                            .registerUserCodeClassLoaderReleaseHookIfAbsent(
                                    releaseHookName, releaseHook);
                }
            };
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return mailboxExecutor;
        }

        @Override
        public org.apache.flink.api.common.operators.ProcessingTimeService
                getProcessingTimeService() {
            return processingTimeService;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public InitializationContext asSerializationSchemaInitializationContext() {
            return new InitContextInitializationContextAdapter(
                    getUserCodeClassLoader(), () -> metricGroup.addGroup("user"));
        }

        @Override
        public boolean isObjectReuseEnabled() {
            return getRuntimeContext().isObjectReuseEnabled();
        }

        @Override
        public <IN> TypeSerializer<IN> createInputSerializer() {
            return operatorConfig
                    .<IN>getTypeSerializerIn(0, getRuntimeContext().getUserCodeClassLoader())
                    .duplicate();
        }
    }
}
