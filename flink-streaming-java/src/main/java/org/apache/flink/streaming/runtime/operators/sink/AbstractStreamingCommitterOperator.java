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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Abstract base class for operators that work with a {@link Committer} or a {@link
 * org.apache.flink.api.connector.sink.GlobalCommitter} in the streaming execution mode.
 *
 * <p>Sub-classes are responsible for implementing {@link #recoveredCommittables(List)}, {@link
 * #prepareCommit(List)} and {@link #commit(List)}.
 *
 * @param <InputT> The input type of the {@link Committer}.
 * @param <CommT> The committable type of the {@link Committer}.
 */
abstract class AbstractStreamingCommitterOperator<InputT, CommT>
        extends AbstractStreamOperator<CommT> implements OneInputStreamOperator<InputT, CommT> {

    private static final long serialVersionUID = 1L;

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** Group the committable by the checkpoint id. */
    private final NavigableMap<Long, List<CommT>> committablesPerCheckpoint;

    /** The committable's serializer. */
    private final StreamingCommitterStateSerializer<CommT> streamingCommitterStateSerializer;

    /** The operator's state. */
    private ListState<StreamingCommitterState<CommT>> streamingCommitterState;

    /** Inputs collected between every pre-commit. */
    private List<InputT> currentInputs;

    /**
     * Notifies a list of committables that might need to be committed again after recovering from a
     * failover.
     *
     * @param committables A list of committables
     */
    abstract void recoveredCommittables(List<CommT> committables) throws IOException;

    /**
     * Prepares a commit.
     *
     * @param input A list of input elements received since last pre-commit
     * @return A list of committables that could be committed in the following checkpoint complete.
     */
    abstract List<CommT> prepareCommit(List<InputT> input) throws IOException;

    /**
     * Commits a list of committables.
     *
     * @param committables A list of committables that is ready for committing.
     * @return A list of committables needed to re-commit.
     */
    abstract List<CommT> commit(List<CommT> committables) throws Exception;

    AbstractStreamingCommitterOperator(SimpleVersionedSerializer<CommT> committableSerializer) {
        this.streamingCommitterStateSerializer =
                new StreamingCommitterStateSerializer<>(committableSerializer);
        this.committablesPerCheckpoint = new TreeMap<>();
        this.currentInputs = new ArrayList<>();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        streamingCommitterStateSerializer);
        final List<CommT> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(s -> restored.addAll(s.getCommittables()));
        recoveredCommittables(restored);
    }

    @Override
    public void processElement(StreamRecord<InputT> element) throws Exception {
        currentInputs.add(element.getValue());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        committablesPerCheckpoint.put(context.getCheckpointId(), prepareCommit(currentInputs));
        currentInputs = new ArrayList<>();

        streamingCommitterState.update(
                Collections.singletonList(
                        new StreamingCommitterState<>(committablesPerCheckpoint)));
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        commitUpTo(checkpointId);
    }

    private void commitUpTo(long checkpointId) throws Exception {
        final Iterator<Map.Entry<Long, List<CommT>>> it =
                committablesPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

        final List<CommT> readyCommittables = new ArrayList<>();

        while (it.hasNext()) {
            final Map.Entry<Long, List<CommT>> entry = it.next();
            final List<CommT> committables = entry.getValue();

            readyCommittables.addAll(committables);
            it.remove();
        }

        LOG.info("Committing the state for checkpoint {}", checkpointId);
        final List<CommT> neededToRetryCommittables = commit(readyCommittables);
        if (!neededToRetryCommittables.isEmpty()) {
            throw new UnsupportedOperationException("Currently does not support the re-commit!");
        }

        // TODO fix :: send only for the committer, not for the global COMMITTER
        for (CommT committable : readyCommittables) {
            output.collect(new StreamRecord<>(committable));
        }
    }
}
