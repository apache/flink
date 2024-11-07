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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.configuration.SinkOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.operators.sink.committables.CheckpointCommittableManager;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.function.FunctionWithException;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.OptionalLong;

import static org.apache.flink.streaming.api.connector.sink2.CommittableMessage.EOI;
import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that processes committables of a {@link org.apache.flink.api.connector.sink2.Sink}.
 *
 * <p>The operator may be part of a sink pipeline, and it always follows {@link SinkWriterOperator},
 * which initially outputs the committables.
 *
 * @param <CommT> the type of the committable
 */
class CommitterOperator<CommT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<CommittableMessage<CommT>, CommittableMessage<CommT>>,
                BoundedOneInput {

    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final FunctionWithException<CommitterInitContext, Committer<CommT>, IOException>
            committerSupplier;
    private final boolean emitDownstream;
    private final boolean isBatchMode;
    private final boolean isCheckpointingEnabled;
    private SinkCommitterMetricGroup metricGroup;
    private Committer<CommT> committer;
    private CommittableCollector<CommT> committableCollector;
    private long lastCompletedCheckpointId = -1;
    private int maxRetries;

    private boolean endInput = false;

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** The operator's state. */
    private ListState<CommittableCollector<CommT>> committableCollectorState;

    public CommitterOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters,
            ProcessingTimeService processingTimeService,
            SimpleVersionedSerializer<CommT> committableSerializer,
            FunctionWithException<CommitterInitContext, Committer<CommT>, IOException>
                    committerSupplier,
            boolean emitDownstream,
            boolean isBatchMode,
            boolean isCheckpointingEnabled) {
        super(parameters);
        this.emitDownstream = emitDownstream;
        this.isBatchMode = isBatchMode;
        this.isCheckpointingEnabled = isCheckpointingEnabled;
        this.processingTimeService = checkNotNull(processingTimeService);
        this.committableSerializer = checkNotNull(committableSerializer);
        this.committerSupplier = checkNotNull(committerSupplier);
    }

    @Override
    protected void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        metricGroup = InternalSinkCommitterMetricGroup.wrap(getMetricGroup());
        committableCollector = CommittableCollector.of(metricGroup);
        maxRetries = config.getConfiguration().get(SinkOptions.COMMITTER_RETRIES);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        OptionalLong checkpointId = context.getRestoredCheckpointId();
        CommitterInitContext initContext = createInitContext(checkpointId);
        committer = committerSupplier.apply(initContext);
        committableCollectorState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        new CommittableCollectorSerializer<>(
                                committableSerializer,
                                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                                getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks(),
                                metricGroup));
        if (context.isRestored()) {
            committableCollectorState.get().forEach(cc -> committableCollector.merge(cc));
            lastCompletedCheckpointId = checkpointId.getAsLong();
            // try to re-commit recovered transactions as quickly as possible
            commitAndEmitCheckpoints();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // It is important to copy the collector to not mutate the state.
        committableCollectorState.update(Collections.singletonList(committableCollector.copy()));
    }

    @Override
    public void endInput() throws Exception {
        endInput = true;
        if (!isCheckpointingEnabled || isBatchMode) {
            // There will be no final checkpoint, all committables should be committed here
            commitAndEmitCheckpoints();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointId);
        commitAndEmitCheckpoints();
    }

    private void commitAndEmitCheckpoints() throws IOException, InterruptedException {
        long completedCheckpointId = endInput ? EOI : lastCompletedCheckpointId;
        for (CheckpointCommittableManager<CommT> checkpointManager :
                committableCollector.getCheckpointCommittablesUpTo(completedCheckpointId)) {
            // ensure that all committables of the first checkpoint are fully committed before
            // attempting the next committable
            commitAndEmit(checkpointManager);
            committableCollector.remove(checkpointManager);
        }
    }

    private void commitAndEmit(CheckpointCommittableManager<CommT> committableManager)
            throws IOException, InterruptedException {
        committableManager.commit(committer, maxRetries);
        if (emitDownstream) {
            emit(committableManager);
        }
    }

    private void emit(CheckpointCommittableManager<CommT> committableManager) {
        int subtaskId = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
        int numberOfSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
        long checkpointId = committableManager.getCheckpointId();
        Collection<CommT> committables = committableManager.getSuccessfulCommittables();
        output.collect(
                new StreamRecord<>(
                        new CommittableSummary<>(
                                subtaskId,
                                numberOfSubtasks,
                                checkpointId,
                                committables.size(),
                                committableManager.getNumFailed())));
        for (CommT committable : committables) {
            output.collect(
                    new StreamRecord<>(
                            new CommittableWithLineage<>(committable, checkpointId, subtaskId)));
        }
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        committableCollector.addMessage(element.getValue());
    }

    @Override
    public void close() throws Exception {
        closeAll(committer, super::close);
    }

    private CommitterInitContext createInitContext(OptionalLong restoredCheckpointId) {
        return new CommitterInitContextImp(getRuntimeContext(), metricGroup, restoredCheckpointId);
    }

    private static class CommitterInitContextImp extends InitContextBase
            implements CommitterInitContext {

        private final SinkCommitterMetricGroup metricGroup;

        public CommitterInitContextImp(
                StreamingRuntimeContext runtimeContext,
                SinkCommitterMetricGroup metricGroup,
                OptionalLong restoredCheckpointId) {
            super(runtimeContext, restoredCheckpointId);
            this.metricGroup = checkNotNull(metricGroup);
        }

        @Override
        public SinkCommitterMetricGroup metricGroup() {
            return metricGroup;
        }
    }
}
