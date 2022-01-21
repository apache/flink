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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
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
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

class GlobalCommitterOperator<CommT> extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CommT>, Void>, BoundedOneInput {

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> GLOBAL_COMMITTER_OPERATOR_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "global_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    private final SerializableSupplier<Committer<CommT>> committerFactory;
    private final SerializableSupplier<SimpleVersionedSerializer<CommT>>
            committableSerializerFactory;

    private ListState<CommittableCollector<CommT>> committableCollectorState;
    private Committer<CommT> committer;
    private CommittableCollector<CommT> committableCollector;
    private long lastCompletedCheckpointId = -1;
    private SimpleVersionedSerializer<CommT> committableSerializer;

    GlobalCommitterOperator(
            SerializableSupplier<Committer<CommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializerFactory) {
        this.committerFactory = checkNotNull(committerFactory);
        this.committableSerializerFactory = checkNotNull(committableSerializerFactory);
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        committer = committerFactory.get();
        committableCollector = CommittableCollector.of(getRuntimeContext());
        committableSerializer = committableSerializerFactory.get();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // It is important to copy the collector to not mutate the state.
        committableCollectorState.update(Collections.singletonList(committableCollector.copy()));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        committableCollectorState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(GLOBAL_COMMITTER_OPERATOR_RAW_STATES_DESC),
                        new CommittableCollectorSerializer<>(
                                committableSerializer,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                getRuntimeContext().getMaxNumberOfParallelSubtasks()));
        if (context.isRestored()) {
            committableCollectorState.get().forEach(cc -> committableCollector.merge(cc));
            lastCompletedCheckpointId = context.getRestoredCheckpointId().getAsLong();
            // try to re-commit recovered transactions as quickly as possible
            commit(lastCompletedCheckpointId);
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointId);
        commit(lastCompletedCheckpointId);
    }

    private Collection<? extends CheckpointCommittableManager<CommT>> getCommittables() {
        final Collection<? extends CheckpointCommittableManager<CommT>> committables =
                committableCollector.getEndOfInputCommittables();
        if (committables == null) {
            return Collections.emptyList();
        }
        return committables;
    }

    private Collection<? extends CheckpointCommittableManager<CommT>> getCommittables(
            long checkpointId) {
        final Collection<? extends CheckpointCommittableManager<CommT>> committables =
                committableCollector.getCheckpointCommittablesUpTo(checkpointId);
        if (committables == null) {
            return Collections.emptyList();
        }
        return committables;
    }

    private void commit(long checkpointId) throws IOException, InterruptedException {
        for (CheckpointCommittableManager<CommT> committable : getCommittables(checkpointId)) {
            boolean fullyReceived = committable.getCheckpointId() == lastCompletedCheckpointId;
            committable.commit(fullyReceived, committer);
        }
    }

    @Override
    public void endInput() throws Exception {
        do {
            for (CommittableManager<CommT> committable : getCommittables()) {
                committable.commit(false, committer);
            }
        } while (!committableCollector.isFinished());
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        committableCollector.addMessage(element.getValue());
    }
}
