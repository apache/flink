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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.configuration.SinkOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSinkCommitterMetricGroup;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.operators.sink.committables.CheckpointCommittableManager;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollectorSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.function.SerializableSupplier;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implements the {@code GlobalCommitter}.
 *
 * <p>This operator usually trails behind a {@code CommitterOperator}. In this case, the global
 * committer will receive committables from the committer operator through {@link
 * #processElement(StreamRecord)}. Once all committables from all subtasks have been received, the
 * global committer will commit them. This approach also works for any number of intermediate custom
 * operators between the committer and the global committer in a custom post-commit topology.
 *
 * <p>That means that the global committer will not wait for {@link
 * #notifyCheckpointComplete(long)}. In many cases, it receives the callback before the actual
 * committables anyway. So it would effectively globally commit one checkpoint later.
 *
 * <p>However, we can leverage the following observation: the global committer will only receive
 * committables iff the respective checkpoint was completed and upstream committers received the
 * {@link #notifyCheckpointComplete(long)}. So by waiting for all committables of a given
 * checkpoint, we implicitly know that the checkpoint was successful and the global committer is
 * supposed to globally commit.
 *
 * <p>Note that committables of checkpoint X are not checkpointed in X because the global committer
 * is trailing behind the checkpoint. They are replayed from the committer state in case of an
 * error. The state only includes incomplete checkpoints coming from upstream committers not
 * receiving {@link #notifyCheckpointComplete(long)}. All committables received are successful.
 *
 * <p>In rare cases, the GlobalCommitterOperator may not be connected (in)directly to a committer
 * but instead is connected (in)directly to a writer. In this case, the global committer needs to
 * perform the 2PC protocol instead of the committer. Thus, we absolutely need to use {@link
 * #notifyCheckpointComplete(long)} similarly to the {@code CommitterOperator}. Hence, {@link
 * #commitOnInput} is set to false in this case. In particular, the following three prerequisites
 * must be met:
 *
 * <ul>
 *   <li>No committer is upstream of which we could implicitly infer {@link
 *       #notifyCheckpointComplete(long)} as sketched above.
 *   <li>The application runs in streaming mode.
 *   <li>Checkpointing is enabled.
 * </ul>
 *
 * <p>In all other cases (batch or upstream committer or checkpointing is disabled), the global
 * committer commits on input.
 */
@Internal
public class GlobalCommitterOperator<CommT, GlobalCommT> extends AbstractStreamOperator<Void>
        implements OneInputStreamOperator<CommittableMessage<CommT>, Void> {

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> GLOBAL_COMMITTER_OPERATOR_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    private final SerializableSupplier<Committer<CommT>> committerFactory;
    private final SerializableSupplier<SimpleVersionedSerializer<CommT>>
            committableSerializerFactory;
    /**
     * Depending on whether there is an upstream committer or it's connected to a writer, we may
     * either wait for notifyCheckpointCompleted or not.
     */
    private final boolean commitOnInput;

    private ListState<GlobalCommittableWrapper<CommT, GlobalCommT>> globalCommitterState;
    private Committer<CommT> committer;
    private CommittableCollector<CommT> committableCollector;
    private long lastCompletedCheckpointId = -1;
    private SimpleVersionedSerializer<CommT> committableSerializer;
    private SinkCommitterMetricGroup metricGroup;
    private int maxRetries;

    @Nullable private SimpleVersionedSerializer<GlobalCommT> globalCommittableSerializer;
    private List<GlobalCommT> sinkV1State = new ArrayList<>();

    public GlobalCommitterOperator(
            SerializableSupplier<Committer<CommT>> committerFactory,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> committableSerializerFactory,
            boolean commitOnInput) {
        this.committerFactory = checkNotNull(committerFactory);
        this.committableSerializerFactory = checkNotNull(committableSerializerFactory);
        this.commitOnInput = commitOnInput;
    }

    @Override
    protected void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<Void>> output) {
        super.setup(containingTask, config, output);
        committer = committerFactory.get();
        metricGroup = InternalSinkCommitterMetricGroup.wrap(metrics);
        committableCollector = CommittableCollector.of(metricGroup);
        committableSerializer = committableSerializerFactory.get();
        maxRetries = config.getConfiguration().get(SinkOptions.COMMITTER_RETRIES);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        // It is important to copy the collector to not mutate the state.
        globalCommitterState.update(
                Collections.singletonList(
                        new GlobalCommittableWrapper<>(
                                committableCollector.copy(), new ArrayList<>(sinkV1State))));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        globalCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(GLOBAL_COMMITTER_OPERATOR_RAW_STATES_DESC),
                        getCommitterStateSerializer());
        if (context.isRestored()) {
            globalCommitterState
                    .get()
                    .forEach(
                            cc -> {
                                sinkV1State.addAll(cc.getGlobalCommittables());
                                committableCollector.merge(cc.getCommittableCollector());
                            });
            // try to re-commit recovered transactions as quickly as possible
            if (context.getRestoredCheckpointId().isPresent()) {
                commit(context.getRestoredCheckpointId().getAsLong());
            }
        }
    }

    private SimpleVersionedSerializer<GlobalCommittableWrapper<CommT, GlobalCommT>>
            getCommitterStateSerializer() {
        final CommittableCollectorSerializer<CommT> committableCollectorSerializer =
                new CommittableCollectorSerializer<>(
                        committableSerializer,
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                        getRuntimeContext().getTaskInfo().getMaxNumberOfParallelSubtasks(),
                        metricGroup);
        return new GlobalCommitterSerializer<>(
                committableCollectorSerializer, globalCommittableSerializer, metricGroup);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        if (!commitOnInput) {
            commit(checkpointId);
        }
    }

    private void commit(long checkpointIdOrEOI) throws IOException, InterruptedException {
        lastCompletedCheckpointId = Math.max(lastCompletedCheckpointId, checkpointIdOrEOI);
        for (CheckpointCommittableManager<CommT> checkpointManager :
                committableCollector.getCheckpointCommittablesUpTo(lastCompletedCheckpointId)) {
            if (!checkpointManager.hasGloballyReceivedAll()) {
                return;
            }
            checkpointManager.commit(committer, maxRetries);
            committableCollector.remove(checkpointManager);
        }
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        committableCollector.addMessage(element.getValue());

        // commitOnInput implies that the global committer is not using notifyCheckpointComplete.
        // Instead, it commits as soon as it receives all committables of a specific checkpoint.
        // For commitOnInput=false, lastCompletedCheckpointId is only updated on
        // notifyCheckpointComplete.
        if (commitOnInput) {
            commit(element.getValue().getCheckpointIdOrEOI());
        }
    }
}
