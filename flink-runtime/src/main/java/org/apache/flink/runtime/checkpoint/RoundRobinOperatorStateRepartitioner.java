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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Current default implementation of {@link OperatorStateRepartitioner} that redistributes state in
 * round robin fashion.
 */
@Internal
public class RoundRobinOperatorStateRepartitioner
        implements OperatorStateRepartitioner<OperatorStateHandle> {

    public static final OperatorStateRepartitioner<OperatorStateHandle> INSTANCE =
            new RoundRobinOperatorStateRepartitioner();
    private static final boolean OPTIMIZE_MEMORY_USE = false;

    @Override
    public List<List<OperatorStateHandle>> repartitionState(
            List<List<OperatorStateHandle>> previousParallelSubtaskStates,
            int oldParallelism,
            int newParallelism) {

        Preconditions.checkNotNull(previousParallelSubtaskStates);
        Preconditions.checkArgument(newParallelism > 0);
        Preconditions.checkArgument(
                previousParallelSubtaskStates.size() == oldParallelism,
                "This method still depends on the order of the new and old operators");

        // Assemble result from all merge maps
        List<List<OperatorStateHandle>> result = new ArrayList<>(newParallelism);

        List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList;

        // We only round-robin repartition UNION state if new parallelism equals to the old one.
        if (newParallelism == oldParallelism) {
            Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    unionStates = collectUnionStates(previousParallelSubtaskStates);

            Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    partlyFinishedBroadcastStates =
                            collectPartlyFinishedBroadcastStates(previousParallelSubtaskStates);

            if (unionStates.isEmpty() && partlyFinishedBroadcastStates.isEmpty()) {
                return previousParallelSubtaskStates;
            }

            // Initialize
            mergeMapList = initMergeMapList(previousParallelSubtaskStates);

            repartitionUnionState(unionStates, mergeMapList);

            // TODO: Currently if some tasks is finished, we would rescale the
            // remaining state. A better solution would be not touch the non-empty
            // subtask state and only fix the empty ones.
            repartitionBroadcastState(partlyFinishedBroadcastStates, mergeMapList);
        } else {

            // Reorganize: group by (State Name -> StreamStateHandle + Offsets)
            GroupByStateNameResults nameToStateByMode =
                    groupByStateMode(previousParallelSubtaskStates);

            if (OPTIMIZE_MEMORY_USE) {
                previousParallelSubtaskStates
                        .clear(); // free for GC at to cost that old handles are no longer available
            }

            // Do the actual repartitioning for all named states
            mergeMapList = repartition(nameToStateByMode, newParallelism);
        }

        for (int i = 0; i < mergeMapList.size(); ++i) {
            result.add(i, new ArrayList<>(mergeMapList.get(i).values()));
        }

        return result;
    }

    /**
     * Init the list of StreamStateHandle -> OperatorStateHandle map with given
     * parallelSubtaskStates when parallelism not changed.
     */
    private List<Map<StreamStateHandle, OperatorStateHandle>> initMergeMapList(
            List<List<OperatorStateHandle>> parallelSubtaskStates) {

        int parallelism = parallelSubtaskStates.size();

        final List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList =
                new ArrayList<>(parallelism);

        for (List<OperatorStateHandle> previousParallelSubtaskState : parallelSubtaskStates) {
            mergeMapList.add(
                    previousParallelSubtaskState.stream()
                            .collect(
                                    Collectors.toMap(
                                            OperatorStateHandle::getDelegateStateHandle,
                                            Function.identity())));
        }

        return mergeMapList;
    }

    private Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
            collectUnionStates(List<List<OperatorStateHandle>> parallelSubtaskStates) {
        return collectStates(parallelSubtaskStates, OperatorStateHandle.Mode.UNION).entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().entries));
    }

    private Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
            collectPartlyFinishedBroadcastStates(
                    List<List<OperatorStateHandle>> parallelSubtaskStates) {
        return collectStates(parallelSubtaskStates, OperatorStateHandle.Mode.BROADCAST).entrySet()
                .stream()
                .filter(e -> e.getValue().isPartiallyReported())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().entries));
    }

    /** Collect the states from given parallelSubtaskStates with the specific {@code mode}. */
    private Map<String, StateEntry> collectStates(
            List<List<OperatorStateHandle>> parallelSubtaskStates, OperatorStateHandle.Mode mode) {

        Map<String, StateEntry> states =
                CollectionUtil.newHashMapWithExpectedSize(parallelSubtaskStates.size());

        for (int i = 0; i < parallelSubtaskStates.size(); ++i) {
            final int subtaskIndex = i;
            List<OperatorStateHandle> subTaskState = parallelSubtaskStates.get(i);
            for (OperatorStateHandle operatorStateHandle : subTaskState) {
                if (operatorStateHandle == null) {
                    continue;
                }

                final Set<Map.Entry<String, OperatorStateHandle.StateMetaInfo>>
                        partitionOffsetEntries =
                                operatorStateHandle.getStateNameToPartitionOffsets().entrySet();

                partitionOffsetEntries.stream()
                        .filter(entry -> entry.getValue().getDistributionMode().equals(mode))
                        .forEach(
                                entry -> {
                                    StateEntry stateEntry =
                                            states.computeIfAbsent(
                                                    entry.getKey(),
                                                    k ->
                                                            new StateEntry(
                                                                    parallelSubtaskStates.size()
                                                                            * partitionOffsetEntries
                                                                                    .size(),
                                                                    parallelSubtaskStates.size()));
                                    stateEntry.addEntry(
                                            subtaskIndex,
                                            Tuple2.of(
                                                    operatorStateHandle.getDelegateStateHandle(),
                                                    entry.getValue()));
                                });
            }
        }

        return states;
    }

    /** Group by the different named states. */
    @SuppressWarnings("unchecked, rawtype")
    private GroupByStateNameResults groupByStateMode(
            List<List<OperatorStateHandle>> previousParallelSubtaskStates) {

        // Reorganize: group by (State Name -> StreamStateHandle + StateMetaInfo)
        EnumMap<
                        OperatorStateHandle.Mode,
                        Map<
                                String,
                                List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>>
                nameToStateByMode = new EnumMap<>(OperatorStateHandle.Mode.class);

        for (OperatorStateHandle.Mode mode : OperatorStateHandle.Mode.values()) {

            nameToStateByMode.put(mode, new HashMap<>());
        }

        for (List<OperatorStateHandle> previousParallelSubtaskState :
                previousParallelSubtaskStates) {
            for (OperatorStateHandle operatorStateHandle : previousParallelSubtaskState) {

                if (operatorStateHandle == null) {
                    continue;
                }

                final Set<Map.Entry<String, OperatorStateHandle.StateMetaInfo>>
                        partitionOffsetEntries =
                                operatorStateHandle.getStateNameToPartitionOffsets().entrySet();

                for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> e :
                        partitionOffsetEntries) {
                    OperatorStateHandle.StateMetaInfo metaInfo = e.getValue();

                    Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                            nameToState = nameToStateByMode.get(metaInfo.getDistributionMode());

                    List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>
                            stateLocations =
                                    nameToState.computeIfAbsent(
                                            e.getKey(),
                                            k ->
                                                    new ArrayList<>(
                                                            previousParallelSubtaskStates.size()
                                                                    * partitionOffsetEntries
                                                                            .size()));

                    stateLocations.add(
                            Tuple2.of(operatorStateHandle.getDelegateStateHandle(), e.getValue()));
                }
            }
        }

        return new GroupByStateNameResults(nameToStateByMode);
    }

    /** Repartition all named states. */
    private List<Map<StreamStateHandle, OperatorStateHandle>> repartition(
            GroupByStateNameResults nameToStateByMode, int newParallelism) {

        // We will use this to merge w.r.t. StreamStateHandles for each parallel subtask inside the
        // maps
        List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList =
                new ArrayList<>(newParallelism);

        // Initialize
        for (int i = 0; i < newParallelism; ++i) {
            mergeMapList.add(new HashMap<>());
        }

        // Start with the state handles we distribute round robin by splitting by offsets
        Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                nameToDistributeState =
                        nameToStateByMode.getByMode(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);

        repartitionSplitState(nameToDistributeState, newParallelism, mergeMapList);

        // Now we also add the state handles marked for union to all parallel instances
        Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                nameToUnionState = nameToStateByMode.getByMode(OperatorStateHandle.Mode.UNION);

        repartitionUnionState(nameToUnionState, mergeMapList);

        // Now we also add the state handles marked for uniform broadcast to all parallel instances
        Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                nameToBroadcastState =
                        nameToStateByMode.getByMode(OperatorStateHandle.Mode.BROADCAST);

        repartitionBroadcastState(nameToBroadcastState, mergeMapList);

        return mergeMapList;
    }

    /** Repartition SPLIT_DISTRIBUTE state. */
    private void repartitionSplitState(
            Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    nameToDistributeState,
            int newParallelism,
            List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList) {

        int startParallelOp = 0;
        // Iterate all named states and repartition one named state at a time per iteration
        for (Map.Entry<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                e : nameToDistributeState.entrySet()) {

            List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> current =
                    e.getValue();

            // Determine actual number of partitions for this named state
            int totalPartitions = 0;
            for (Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> offsets : current) {
                totalPartitions += offsets.f1.getOffsets().length;
            }

            // Repartition the state across the parallel operator instances
            int lstIdx = 0;
            int offsetIdx = 0;
            int baseFraction = totalPartitions / newParallelism;
            int remainder = totalPartitions % newParallelism;

            int newStartParallelOp = startParallelOp;

            for (int i = 0; i < newParallelism; ++i) {

                // Preparation: calculate the actual index considering wrap around
                int parallelOpIdx = (i + startParallelOp) % newParallelism;

                // Now calculate the number of partitions we will assign to the parallel instance in
                // this round ...
                int numberOfPartitionsToAssign = baseFraction;

                // ... and distribute odd partitions while we still have some, one at a time
                if (remainder > 0) {
                    ++numberOfPartitionsToAssign;
                    --remainder;
                } else if (remainder == 0) {
                    // We are out of odd partitions now and begin our next redistribution round with
                    // the current
                    // parallel operator to ensure fair load balance
                    newStartParallelOp = parallelOpIdx;
                    --remainder;
                }

                // Now start collection the partitions for the parallel instance into this list

                while (numberOfPartitionsToAssign > 0) {
                    Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> handleWithOffsets =
                            current.get(lstIdx);

                    long[] offsets = handleWithOffsets.f1.getOffsets();
                    int remaining = offsets.length - offsetIdx;
                    // Repartition offsets
                    long[] offs;
                    if (remaining > numberOfPartitionsToAssign) {
                        offs =
                                Arrays.copyOfRange(
                                        offsets, offsetIdx, offsetIdx + numberOfPartitionsToAssign);
                        offsetIdx += numberOfPartitionsToAssign;
                    } else {
                        if (OPTIMIZE_MEMORY_USE) {
                            handleWithOffsets.f1 = null; // GC
                        }
                        offs = Arrays.copyOfRange(offsets, offsetIdx, offsets.length);
                        offsetIdx = 0;
                        ++lstIdx;
                    }

                    numberOfPartitionsToAssign -= remaining;

                    // As a last step we merge partitions that use the same StreamStateHandle in a
                    // single
                    // OperatorStateHandle
                    Map<StreamStateHandle, OperatorStateHandle> mergeMap =
                            mergeMapList.get(parallelOpIdx);
                    OperatorStateHandle operatorStateHandle = mergeMap.get(handleWithOffsets.f0);
                    if (operatorStateHandle == null) {
                        operatorStateHandle =
                                new OperatorStreamStateHandle(
                                        CollectionUtil.newHashMapWithExpectedSize(
                                                nameToDistributeState.size()),
                                        handleWithOffsets.f0);
                        mergeMap.put(handleWithOffsets.f0, operatorStateHandle);
                    }
                    operatorStateHandle
                            .getStateNameToPartitionOffsets()
                            .put(
                                    e.getKey(),
                                    new OperatorStateHandle.StateMetaInfo(
                                            offs, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
                }
            }
            startParallelOp = newStartParallelOp;
            e.setValue(null);
        }
    }

    /** Repartition UNION state. */
    private void repartitionUnionState(
            Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    unionState,
            List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList) {

        for (Map<StreamStateHandle, OperatorStateHandle> mergeMap : mergeMapList) {
            for (Map.Entry<
                            String,
                            List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    e : unionState.entrySet()) {

                for (Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>
                        handleWithMetaInfo : e.getValue()) {
                    OperatorStateHandle operatorStateHandle = mergeMap.get(handleWithMetaInfo.f0);
                    if (operatorStateHandle == null) {
                        operatorStateHandle =
                                new OperatorStreamStateHandle(
                                        CollectionUtil.newHashMapWithExpectedSize(
                                                unionState.size()),
                                        handleWithMetaInfo.f0);
                        mergeMap.put(handleWithMetaInfo.f0, operatorStateHandle);
                    }
                    operatorStateHandle
                            .getStateNameToPartitionOffsets()
                            .put(e.getKey(), handleWithMetaInfo.f1);
                }
            }
        }
    }

    /** Repartition BROADCAST state. */
    private void repartitionBroadcastState(
            Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    broadcastState,
            List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList) {

        int newParallelism = mergeMapList.size();
        for (int i = 0; i < newParallelism; ++i) {

            final Map<StreamStateHandle, OperatorStateHandle> mergeMap = mergeMapList.get(i);

            // for each name, pick the i-th entry
            for (Map.Entry<
                            String,
                            List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                    e : broadcastState.entrySet()) {

                int previousParallelism = e.getValue().size();

                Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> handleWithMetaInfo =
                        e.getValue().get(i % previousParallelism);

                OperatorStateHandle operatorStateHandle = mergeMap.get(handleWithMetaInfo.f0);
                if (operatorStateHandle == null) {
                    operatorStateHandle =
                            new OperatorStreamStateHandle(
                                    CollectionUtil.newHashMapWithExpectedSize(
                                            broadcastState.size()),
                                    handleWithMetaInfo.f0);
                    mergeMap.put(handleWithMetaInfo.f0, operatorStateHandle);
                }
                operatorStateHandle
                        .getStateNameToPartitionOffsets()
                        .put(e.getKey(), handleWithMetaInfo.f1);
            }
        }
    }

    private static final class GroupByStateNameResults {
        private final EnumMap<
                        OperatorStateHandle.Mode,
                        Map<
                                String,
                                List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>>
                byMode;

        GroupByStateNameResults(
                EnumMap<
                                OperatorStateHandle.Mode,
                                Map<
                                        String,
                                        List<
                                                Tuple2<
                                                        StreamStateHandle,
                                                        OperatorStateHandle.StateMetaInfo>>>>
                        byMode) {
            this.byMode = Preconditions.checkNotNull(byMode);
        }

        public Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>
                getByMode(OperatorStateHandle.Mode mode) {
            return byMode.get(mode);
        }
    }

    private static final class StateEntry {
        final List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> entries;
        final BitSet reportedSubtaskIndices;

        public StateEntry(int estimatedEntrySize, int parallelism) {
            this.entries = new ArrayList<>(estimatedEntrySize);
            this.reportedSubtaskIndices = new BitSet(parallelism);
        }

        void addEntry(
                int subtaskIndex,
                Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> entry) {
            this.entries.add(entry);
            reportedSubtaskIndices.set(subtaskIndex);
        }

        boolean isPartiallyReported() {
            return reportedSubtaskIndices.cardinality() > 0
                    && reportedSubtaskIndices.cardinality() < reportedSubtaskIndices.size();
        }
    }
}
