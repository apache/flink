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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
abstract class AbstractStreamingCommitterHandler<InputT, CommT>
        extends AbstractCommitterHandler<InputT, CommT, CommT> {
    private static final Logger LOG =
            LoggerFactory.getLogger(AbstractStreamingCommitterHandler.class);

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
    abstract List<CommT> commit(List<CommT> committables) throws IOException, InterruptedException;

    AbstractStreamingCommitterHandler(SimpleVersionedSerializer<CommT> committableSerializer) {
        this.streamingCommitterStateSerializer =
                new StreamingCommitterStateSerializer<>(committableSerializer);
        this.committablesPerCheckpoint = new TreeMap<>();
    }

    @Override
    protected void retry(List<CommT> recoveredCommittables)
            throws IOException, InterruptedException {
        recoveredCommittables(commit(recoveredCommittables));
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
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        committablesPerCheckpoint.put(context.getCheckpointId(), prepareCommit(pollCommittables()));

        streamingCommitterState.update(
                Collections.singletonList(
                        new StreamingCommitterState<>(committablesPerCheckpoint)));
    }

    protected List<CommT> commitUpTo(long checkpointId) throws IOException, InterruptedException {
        NavigableMap<Long, List<CommT>> headMap =
                committablesPerCheckpoint.headMap(checkpointId, true);
        final List<CommT> readyCommittables;
        if (headMap.size() == 1) {
            readyCommittables = headMap.pollFirstEntry().getValue();
        } else {

            readyCommittables = new ArrayList<>();

            final Iterator<Map.Entry<Long, List<CommT>>> it = headMap.entrySet().iterator();
            while (it.hasNext()) {
                final Map.Entry<Long, List<CommT>> entry = it.next();
                final List<CommT> committables = entry.getValue();

                readyCommittables.addAll(committables);
                it.remove();
            }
        }

        LOG.info("Committing the state for checkpoint {}", checkpointId);
        final List<CommT> failedCommittables = commit(readyCommittables);
        recoveredCommittables(failedCommittables);

        // Do not forward failed committables
        final Set<CommT> failedCommittableSet = Collections.newSetFromMap(new IdentityHashMap<>());
        failedCommittableSet.addAll(failedCommittables);

        // Retain ordering of the committables
        return readyCommittables.stream()
                .filter(c -> !failedCommittableSet.contains(c))
                .collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        committablesPerCheckpoint.clear();
        super.close();
    }
}
