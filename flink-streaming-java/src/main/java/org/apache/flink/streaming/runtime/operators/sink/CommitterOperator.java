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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.operators.sink.committables.CheckpointCommittableManager;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableManager;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.OptionalLong;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that processes committables of a {@link org.apache.flink.api.connector.sink.Sink}.
 *
 * <p>The operator may be part of a sink pipeline, and it always follows {@link SinkWriterOperator},
 * which initially outputs the committables.
 *
 * @param <CommT> the type of the committable
 */
class CommitterOperator<CommT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<CommittableMessage<CommT>, CommittableMessage<CommT>>,
                BoundedOneInput {

    private static final long RETRY_DELAY = 1000;
    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final Committer<CommT> committer;
    private final boolean emitDownstream;
    private final boolean isBatchMode;
    private final boolean isCheckpointingEnabled;
    private CommittableCollector<CommT> committableCollector;
    private long lastCompletedCheckpointId = -1;

    private boolean endInput = false;

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** The operator's state. */
    private ListState<CommittableCollector<CommT>> committableCollectorState;

    public CommitterOperator(
            ProcessingTimeService processingTimeService,
            SimpleVersionedSerializer<CommT> committableSerializer,
            Committer<CommT> committer,
            boolean emitDownstream,
            boolean isBatchMode,
            boolean isCheckpointingEnabled) {
        this.emitDownstream = emitDownstream;
        this.isBatchMode = isBatchMode;
        this.isCheckpointingEnabled = isCheckpointingEnabled;
        this.processingTimeService = checkNotNull(processingTimeService);
        this.committableSerializer = checkNotNull(committableSerializer);
        this.committer = checkNotNull(committer);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        committableCollector = CommittableCollector.of(getRuntimeContext());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        committableCollectorState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        new CommittableCollectorSerializer<>(
                                committableSerializer,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getNumberOfParallelSubtasks()));
        if (context.isRestored()) {
            committableCollectorState.get().forEach(cc -> committableCollector.merge(cc));
            lastCompletedCheckpointId = context.getRestoredCheckpointId().getAsLong();
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
            notifyCheckpointComplete(Long.MAX_VALUE);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        if (endInput) {
            // This is the final checkpoint, all committables should be committed
            lastCompletedCheckpointId = Long.MAX_VALUE;
        } else {
            lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointId);
        }
        commitAndEmitCheckpoints();
    }

    private void commitAndEmitCheckpoints() throws IOException, InterruptedException {
        do {
            for (CheckpointCommittableManager<CommT> manager :
                    committableCollector.getCheckpointCommittablesUpTo(lastCompletedCheckpointId)) {
                // wait for all committables of the current manager before submission
                boolean fullyReceived =
                        !endInput && manager.getCheckpointId() == lastCompletedCheckpointId;
                commitAndEmit(manager, fullyReceived);
            }
            // !committableCollector.isFinished() indicates that we should retry
            // Retry should be done here if this is a final checkpoint (indicated by endInput)
            // WARN: this is an endless retry, may make the job stuck while finishing
        } while (!committableCollector.isFinished() && endInput);

        if (!committableCollector.isFinished()) {
            // if not endInput, we can schedule retrying later
            retryWithDelay();
        }
    }

    private void commitAndEmit(CommittableManager<CommT> committableManager, boolean fullyReceived)
            throws IOException, InterruptedException {
        Collection<CommittableWithLineage<CommT>> committed =
                committableManager.commit(fullyReceived, committer);
        if (emitDownstream && !committed.isEmpty()) {
            output.collect(new StreamRecord<>(committableManager.getSummary()));
            for (CommittableWithLineage<CommT> committable : committed) {
                output.collect(new StreamRecord<>(committable));
            }
        }
    }

    private void retryWithDelay() {
        processingTimeService.registerTimer(
                processingTimeService.getCurrentProcessingTime() + RETRY_DELAY,
                ts -> commitAndEmitCheckpoints());
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        committableCollector.addMessage(element.getValue());

        // in case of unaligned checkpoint, we may receive notifyCheckpointComplete before the
        // committables
        OptionalLong checkpointId = element.getValue().getCheckpointId();
        if (checkpointId.isPresent() && checkpointId.getAsLong() <= lastCompletedCheckpointId) {
            commitAndEmitCheckpoints();
        }
    }

    @Override
    public void close() throws Exception {
        closeAll(committer, super::close);
    }
}
