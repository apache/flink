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
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <p>This class adds checkpointing to the {@link AbstractCommitterHandler}, such that state is
 * maintained per checkpoint and cumulatively committed on successful checkpoint.
 *
 * <p>Sub-classes are responsible for implementing {@link #recoveredCommittables(List)}, {@link
 * #prepareCommit(List)} and {@link #commitInternal(List)}.
 *
 * @param <CommT> The input and output type of the {@link Committer}.
 * @param <StateT> The type of the internal state.
 */
abstract class AbstractStreamingCommitterHandler<CommT, StateT>
        extends AbstractCommitterHandler<CommT, StateT> {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractStreamingCommitterHandler.class);

    /** The operator's state descriptor. */
    private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);

    /** Group the committable by the checkpoint id. */
    private final NavigableMap<Long, List<CommittableWrapper<StateT>>> committablesPerCheckpoint;

    /** The committable's serializer. */
    private final StreamingCommitterStateSerializer<CommittableWrapper<StateT>>
            streamingCommitterStateSerializer;

    /** The operator's state. */
    private ListState<StreamingCommitterState<CommittableWrapper<StateT>>> streamingCommitterState;

    /**
     * Prepares a commit.
     *
     * @param input A list of input elements received since last pre-commit
     * @return A list of committables that could be committed in the following checkpoint complete.
     */
    abstract List<CommittableWrapper<StateT>> prepareCommit(List<CommittableWrapper<CommT>> input)
            throws IOException;

    AbstractStreamingCommitterHandler(
            SimpleVersionedSerializer<CommittableWrapper<StateT>> committableSerializer) {
        this.streamingCommitterStateSerializer =
                new StreamingCommitterStateSerializer<>(committableSerializer);
        this.committablesPerCheckpoint = new TreeMap<>();
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        streamingCommitterState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                        streamingCommitterStateSerializer);
        final List<CommittableWrapper<StateT>> restored = new ArrayList<>();
        streamingCommitterState.get().forEach(s -> restored.addAll(s.getCommittables()));
        recoveredCommittables(restored);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        committablesPerCheckpoint.put(context.getCheckpointId(), prepareCommit(pollCommittables()));

        streamingCommitterState.update(
                Collections.singletonList(StreamingCommitterState.of(committablesPerCheckpoint)));
    }

    /**
     * Commits all pending committables up to the given checkpointId and returns a list of
     * successful committables.
     *
     * @return
     */
    protected CommitResult<CommittableWrapper<StateT>> commitUpTo(long checkpointId)
            throws IOException, InterruptedException {
        NavigableMap<Long, List<CommittableWrapper<StateT>>> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        final List<CommittableWrapper<StateT>> readyCommittables;
        if (headMap.size() == 1) {
            readyCommittables = headMap.pollFirstEntry().getValue();
        } else {

            readyCommittables = new ArrayList<>();

            final Iterator<Map.Entry<Long, List<CommittableWrapper<StateT>>>> it =
                    headMap.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Long, List<CommittableWrapper<StateT>>> entry = it.next();
                final List<CommittableWrapper<StateT>> committables = entry.getValue();

                readyCommittables.addAll(committables);
                it.remove();
            }
        }

        LOG.info("Committing the state for checkpoint {}", checkpointId);
        return commit(readyCommittables);
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        super.close();
    }
}
