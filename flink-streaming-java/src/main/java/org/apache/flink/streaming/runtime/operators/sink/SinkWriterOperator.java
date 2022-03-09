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
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.UserCodeClassLoader;

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
 * org.apache.flink.api.connector.sink.Sink}. It also has a way to process committables with the
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

    SinkWriterOperator(
            Sink<InputT> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor) {
        this.processingTimeService = checkNotNull(processingTimeService);
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.context = new Context<>();
        this.emitDownstream = sink instanceof TwoPhaseCommittingSink;

        if (sink instanceof StatefulSink) {
            writerStateHandler =
                    new StatefulSinkWriterStateHandler<>((StatefulSink<InputT, ?>) sink);
        } else {
            writerStateHandler = new StatelessSinkWriterStateHandler<>(sink);
        }

        if (sink instanceof TwoPhaseCommittingSink) {
            committableSerializer =
                    ((TwoPhaseCommittingSink<InputT, CommT>) sink).getCommittableSerializer();
        } else {
            committableSerializer = null;
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OptionalLong checkpointId = context.getRestoredCheckpointId();
        InitContext initContext =
                createInitContext(checkpointId.isPresent() ? checkpointId.getAsLong() : null);
        if (context.isRestored()) {
            if (committableSerializer != null) {
                final ListState<List<CommT>> legacyCommitterState =
                        new SimpleVersionedListState<>(
                                context.getOperatorStateStore()
                                        .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                                new SinkV1WriterCommittableSerializer<>(committableSerializer));
                legacyCommitterState.get().forEach(legacyCommittables::addAll);
            }
        }
        sinkWriter = writerStateHandler.createWriter(initContext, context);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        writerStateHandler.snapshotState(context.getCheckpointId());
    }

    @Override
    public void processElement(StreamRecord<InputT> element) throws Exception {
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
        endOfInput = true;
        sinkWriter.flush(true);
        emitCommittables(Long.MAX_VALUE);
    }

    private void emitCommittables(Long checkpointId) throws IOException, InterruptedException {
        if (!emitDownstream) {
            // To support SinkV1 topologies with only a writer we have to call prepareCommit
            // although no committables are forwarded
            if (sinkWriter instanceof PrecommittingSinkWriter) {
                ((PrecommittingSinkWriter<?, ?>) sinkWriter).prepareCommit();
            }
            return;
        }
        Collection<CommT> committables =
                ((PrecommittingSinkWriter<?, CommT>) sinkWriter).prepareCommit();
        StreamingRuntimeContext runtimeContext = getRuntimeContext();
        final int indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        final int numberOfParallelSubtasks = runtimeContext.getNumberOfParallelSubtasks();

        // Emit only committable summary if there are legacy committables
        if (!legacyCommittables.isEmpty()) {
            checkState(checkpointId > InitContext.INITIAL_CHECKPOINT_ID);
            emit(
                    indexOfThisSubtask,
                    numberOfParallelSubtasks,
                    InitContext.INITIAL_CHECKPOINT_ID,
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
        output.collect(
                new StreamRecord<>(
                        new CommittableSummary<>(
                                indexOfThisSubtask,
                                numberOfParallelSubtasks,
                                checkpointId,
                                committables.size(),
                                committables.size(),
                                0)));
        for (CommT committable : committables) {
            output.collect(
                    new StreamRecord<>(
                            new CommittableWithLineage<>(
                                    committable, checkpointId, indexOfThisSubtask)));
        }
    }

    private Sink.InitContext createInitContext(@Nullable Long restoredCheckpointId) {
        return new InitContextImpl(
                getRuntimeContext(),
                processingTimeService,
                mailboxExecutor,
                InternalSinkWriterMetricGroup.wrap(getMetricGroup()),
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

    private static class InitContextImpl implements Sink.InitContext {

        private final ProcessingTimeService processingTimeService;

        private final MailboxExecutor mailboxExecutor;

        private final SinkWriterMetricGroup metricGroup;

        @Nullable private final Long restoredCheckpointId;

        private final StreamingRuntimeContext runtimeContext;

        public InitContextImpl(
                StreamingRuntimeContext runtimeContext,
                ProcessingTimeService processingTimeService,
                MailboxExecutor mailboxExecutor,
                SinkWriterMetricGroup metricGroup,
                @Nullable Long restoredCheckpointId) {
            this.runtimeContext = checkNotNull(runtimeContext);
            this.mailboxExecutor = checkNotNull(mailboxExecutor);
            this.processingTimeService = checkNotNull(processingTimeService);
            this.metricGroup = checkNotNull(metricGroup);
            this.restoredCheckpointId = restoredCheckpointId;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return runtimeContext.getUserCodeClassLoader();
                }

                @Override
                public void registerReleaseHookIfAbsent(
                        String releaseHookName, Runnable releaseHook) {
                    runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
                            releaseHookName, releaseHook);
                }
            };
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return runtimeContext.getNumberOfParallelSubtasks();
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
        public int getSubtaskId() {
            return runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return restoredCheckpointId == null
                    ? OptionalLong.empty()
                    : OptionalLong.of(restoredCheckpointId);
        }

        @Override
        public InitializationContext asSerializationSchemaInitializationContext() {
            return new InitContextInitializationContextAdapter(
                    getUserCodeClassLoader(), () -> metricGroup.addGroup("user"));
        }
    }
}
