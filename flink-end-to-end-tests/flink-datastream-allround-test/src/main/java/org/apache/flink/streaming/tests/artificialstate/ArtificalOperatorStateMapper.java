/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.artificialstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A self-verifiable {@link RichMapFunction} used to verify checkpointing and restore semantics for
 * various kinds of operator state.
 *
 * <p>For verifying broadcast state, the each subtask stores as broadcast state a map of (Integer,
 * String) entries, key being the subtask index, and value being a String that corresponds to the
 * subtask index. The total number of subtasks is also stored as broadcast state. On restore, each
 * subtask should be restored with exactly the same broadcast state, with one entry for each subtask
 * in the previous run.
 *
 * <p>For verifying union state, each subtask of this operator stores its own subtask index as a
 * subset of the whole union state. On restore, each subtask's restored union state should have one
 * entry for each subtask in the previous run.
 *
 * <p>All input elements to the operator arre simply passed through a user-provided map function and
 * emitted.
 */
public class ArtificalOperatorStateMapper<IN, OUT> extends RichMapFunction<IN, OUT>
        implements CheckpointedFunction {

    private static final long serialVersionUID = -1741298597425077761L;

    // ============================================================================
    //  State names
    // ============================================================================

    private static final String LAST_NUM_SUBTASKS_STATE_NAME = "lastNumSubtasksState";
    private static final String BROADCAST_STATE_NAME = "broadcastState";
    private static final String UNION_STATE_NAME = "unionState";

    // ============================================================================
    //  Keys used in broadcast states
    // ============================================================================

    private static final String LAST_NUM_SUBTASKS_STATE_KEY = "lastNumSubtasks";
    private static final String BROADCAST_STATE_ENTRY_VALUE_PREFIX = "broadcastStateEntry-";

    private final MapFunction<IN, OUT> mapFunction;

    private transient BroadcastState<String, Integer> lastNumSubtasksBroadcastState;

    private transient BroadcastState<Integer, String> broadcastElementsState;
    private transient ListState<Integer> unionElementsState;

    public ArtificalOperatorStateMapper(MapFunction<IN, OUT> mapFunction) {
        this.mapFunction = Preconditions.checkNotNull(mapFunction);
    }

    @Override
    public OUT map(IN value) throws Exception {
        return mapFunction.map(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        this.lastNumSubtasksBroadcastState =
                context.getOperatorStateStore()
                        .getBroadcastState(
                                new MapStateDescriptor<>(
                                        LAST_NUM_SUBTASKS_STATE_NAME,
                                        StringSerializer.INSTANCE,
                                        IntSerializer.INSTANCE));

        this.broadcastElementsState =
                context.getOperatorStateStore()
                        .getBroadcastState(
                                new MapStateDescriptor<>(
                                        BROADCAST_STATE_NAME,
                                        IntSerializer.INSTANCE,
                                        StringSerializer.INSTANCE));

        this.unionElementsState =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>(
                                        UNION_STATE_NAME, IntSerializer.INSTANCE));

        if (context.isRestored()) {
            Integer lastNumSubtasks =
                    lastNumSubtasksBroadcastState.get(LAST_NUM_SUBTASKS_STATE_KEY);
            Preconditions.checkState(lastNumSubtasks != null);

            // -- verification for broadcast state --
            Set<Integer> expected = new HashSet<>();
            for (int i = 0; i < lastNumSubtasks; i++) {
                expected.add(i);
            }

            for (Map.Entry<Integer, String> broadcastElementEntry :
                    broadcastElementsState.entries()) {
                int key = broadcastElementEntry.getKey();
                Preconditions.checkState(
                        expected.remove(key), "Unexpected keys in restored broadcast state.");
                Preconditions.checkState(
                        broadcastElementEntry.getValue().equals(getBroadcastStateEntryValue(key)),
                        "Incorrect value in restored broadcast state.");
            }

            Preconditions.checkState(
                    expected.size() == 0, "Missing keys in restored broadcast state.");

            // -- verification for union state --
            for (int i = 0; i < lastNumSubtasks; i++) {
                expected.add(i);
            }

            for (Integer subtaskIndex : unionElementsState.get()) {
                Preconditions.checkState(
                        expected.remove(subtaskIndex),
                        "Unexpected element in restored union state.");
            }
            Preconditions.checkState(
                    expected.size() == 0, "Missing elements in restored union state.");
        } else {
            // verify that the broadcast / union state is actually empty if this is not a restored
            // run, as told by the state context;
            // this catches incorrect logic with the context.isRestored() when using broadcast state
            // / union state.

            Preconditions.checkState(!lastNumSubtasksBroadcastState.iterator().hasNext());
            Preconditions.checkState(!broadcastElementsState.iterator().hasNext());
            Preconditions.checkState(!unionElementsState.get().iterator().hasNext());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        final int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        final int thisSubtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        // store total number of subtasks as broadcast state
        lastNumSubtasksBroadcastState.clear();
        lastNumSubtasksBroadcastState.put(LAST_NUM_SUBTASKS_STATE_KEY, numSubtasks);

        // populate broadcast state (identical across all subtasks)
        broadcastElementsState.clear();
        for (int i = 0; i < numSubtasks; i++) {
            broadcastElementsState.put(i, getBroadcastStateEntryValue(i));
        }

        // each subtask only stores its own subtask index as a subset of the union set
        unionElementsState.update(Collections.singletonList(thisSubtaskIndex));
    }

    private String getBroadcastStateEntryValue(int thisSubtaskIndex) {
        return BROADCAST_STATE_ENTRY_VALUE_PREFIX + thisSubtaskIndex;
    }
}
