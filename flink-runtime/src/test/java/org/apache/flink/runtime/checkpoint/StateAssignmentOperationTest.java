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

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor.MappingType.RESCALING;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.array;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.rescalingDescriptor;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewKeyedStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewOperatorStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.ARBITRARY;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.RANGE;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.ROUND_ROBIN;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests to verify state assignment operation. */
class StateAssignmentOperationTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static final int MAX_P = 256;

    @Test
    void testRepartitionSplitDistributeStates() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(1);
        metaInfoMap1.put(
                "t-1",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[30]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(1);
        metaInfoMap2.put(
                "t-2",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 15}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
        OperatorStateHandle osh2 =
                new OperatorStreamStateHandle(
                        metaInfoMap2, new ByteStreamStateHandle("test2", new byte[40]));
        operatorState.putState(
                1, OperatorSubtaskState.builder().setManagedOperatorState(osh2).build());

        verifyOneKindPartitionableStateRescale(operatorState, operatorID);
    }

    @Test
    void testRepartitionUnionState() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
        metaInfoMap1.put(
                "t-3",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.UNION));
        metaInfoMap1.put(
                "t-4",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {22, 44}, OperatorStateHandle.Mode.UNION));
        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[50]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(1);
        metaInfoMap2.put(
                "t-3",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.UNION));
        OperatorStateHandle osh2 =
                new OperatorStreamStateHandle(
                        metaInfoMap2, new ByteStreamStateHandle("test2", new byte[20]));
        operatorState.putState(
                1, OperatorSubtaskState.builder().setManagedOperatorState(osh2).build());

        verifyOneKindPartitionableStateRescale(operatorState, operatorID);
    }

    @Test
    void testRepartitionBroadcastState() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
        metaInfoMap1.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap1.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[60]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(2);
        metaInfoMap2.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap2.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
        OperatorStateHandle osh2 =
                new OperatorStreamStateHandle(
                        metaInfoMap2, new ByteStreamStateHandle("test2", new byte[60]));
        operatorState.putState(
                1, OperatorSubtaskState.builder().setManagedOperatorState(osh2).build());

        verifyOneKindPartitionableStateRescale(operatorState, operatorID);
    }

    @Test
    void testRepartitionBroadcastStateWithNullSubtaskState() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        // Only the subtask 0 reports the states.
        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
        metaInfoMap1.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap1.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[60]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        verifyOneKindPartitionableStateRescale(operatorState, operatorID);
    }

    @Test
    void testRepartitionBroadcastStateWithEmptySubtaskState() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        // Only the subtask 0 reports the states.
        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(2);
        metaInfoMap1.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0, 10, 20}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap1.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 40, 50}, OperatorStateHandle.Mode.BROADCAST));
        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[60]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        // The subtask 1 report an empty snapshot.
        operatorState.putState(1, OperatorSubtaskState.builder().build());

        verifyOneKindPartitionableStateRescale(operatorState, operatorID);
    }

    /** Verify repartition logic on partitionable states with all modes. */
    @Test
    void testReDistributeCombinedPartitionableStates() {
        OperatorID operatorID = new OperatorID();
        OperatorState operatorState = new OperatorState(operatorID, 2, 4);

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap1 = new HashMap<>(6);
        metaInfoMap1.put(
                "t-1",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.UNION));
        metaInfoMap1.put(
                "t-2",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {22, 44}, OperatorStateHandle.Mode.UNION));
        metaInfoMap1.put(
                "t-3",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {52, 63}, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
        metaInfoMap1.put(
                "t-4",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {67, 74, 75}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap1.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {77, 88, 92}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap1.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {101, 123, 127}, OperatorStateHandle.Mode.BROADCAST));

        OperatorStateHandle osh1 =
                new OperatorStreamStateHandle(
                        metaInfoMap1, new ByteStreamStateHandle("test1", new byte[130]));
        operatorState.putState(
                0, OperatorSubtaskState.builder().setManagedOperatorState(osh1).build());

        Map<String, OperatorStateHandle.StateMetaInfo> metaInfoMap2 = new HashMap<>(3);
        metaInfoMap2.put(
                "t-1",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {0}, OperatorStateHandle.Mode.UNION));
        metaInfoMap2.put(
                "t-4",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {20, 27, 28}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap2.put(
                "t-5",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {30, 44, 48}, OperatorStateHandle.Mode.BROADCAST));
        metaInfoMap2.put(
                "t-6",
                new OperatorStateHandle.StateMetaInfo(
                        new long[] {57, 79, 83}, OperatorStateHandle.Mode.BROADCAST));

        OperatorStateHandle osh2 =
                new OperatorStreamStateHandle(
                        metaInfoMap2, new ByteStreamStateHandle("test2", new byte[86]));
        operatorState.putState(
                1, OperatorSubtaskState.builder().setManagedOperatorState(osh2).build());

        // rescale up case, parallelism 2 --> 3
        verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 3);

        // rescale down case, parallelism 2 --> 1
        verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 1);

        // not rescale
        verifyCombinedPartitionableStateRescale(operatorState, operatorID, 2, 2);
    }

    // ------------------------------------------------------------------------

    /**
     * Verify that after repartition states, state of different modes works as expected and collect
     * the information of state-name -> how many operator stat handles would be used for new
     * sub-tasks to initialize in total.
     */
    private void verifyAndCollectStateInfo(
            OperatorState operatorState,
            OperatorID operatorID,
            int oldParallelism,
            int newParallelism,
            Map<String, Integer> stateInfoCounts) {
        final Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
                new HashMap<>();
        StateAssignmentOperation.reDistributePartitionableStates(
                Collections.singletonMap(operatorID, operatorState),
                newParallelism,
                OperatorSubtaskState::getManagedOperatorState,
                RoundRobinOperatorStateRepartitioner.INSTANCE,
                newManagedOperatorStates);

        // Verify the repartitioned managed operator states per sub-task.
        for (List<OperatorStateHandle> operatorStateHandles : newManagedOperatorStates.values()) {

            final EnumMap<OperatorStateHandle.Mode, Map<String, Integer>> stateModeOffsets =
                    new EnumMap<>(OperatorStateHandle.Mode.class);
            for (OperatorStateHandle.Mode mode : OperatorStateHandle.Mode.values()) {
                stateModeOffsets.put(mode, new HashMap<>());
            }

            for (OperatorStateHandle operatorStateHandle : operatorStateHandles) {
                for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> stateNameToMetaInfo :
                        operatorStateHandle.getStateNameToPartitionOffsets().entrySet()) {

                    String stateName = stateNameToMetaInfo.getKey();
                    stateInfoCounts.merge(stateName, 1, Integer::sum);

                    OperatorStateHandle.StateMetaInfo stateMetaInfo =
                            stateNameToMetaInfo.getValue();

                    stateModeOffsets
                            .get(stateMetaInfo.getDistributionMode())
                            .merge(stateName, stateMetaInfo.getOffsets().length, Integer::sum);
                }
            }

            for (Map.Entry<OperatorStateHandle.Mode, Map<String, Integer>> modeMapEntry :
                    stateModeOffsets.entrySet()) {

                OperatorStateHandle.Mode mode = modeMapEntry.getKey();
                Map<String, Integer> stateOffsets = modeMapEntry.getValue();
                if (OperatorStateHandle.Mode.SPLIT_DISTRIBUTE.equals(mode)) {
                    if (oldParallelism < newParallelism) {
                        // SPLIT_DISTRIBUTE: when rescale up, split the state and re-distribute it
                        // -> each one will go to one task
                        stateOffsets.values().forEach(length -> assertThat((int) length).isOne());
                    } else {
                        // SPLIT_DISTRIBUTE: when rescale down to 1 or not rescale, not
                        // re-distribute them.
                        stateOffsets
                                .values()
                                .forEach(length -> assertThat((int) length).isEqualTo(2));
                    }
                } else if (OperatorStateHandle.Mode.UNION.equals(mode)) {
                    // UNION: all to all
                    stateOffsets.values().forEach(length -> assertThat((int) length).isEqualTo(2));
                } else {
                    // BROADCAST: so all to all
                    stateOffsets.values().forEach(length -> assertThat((int) length).isEqualTo(3));
                }
            }
        }
    }

    private void verifyOneKindPartitionableStateRescale(
            OperatorState operatorState, OperatorID operatorID) {
        // rescale up case, parallelism 2 --> 3
        verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 3);

        // rescale down case, parallelism 2 --> 1
        verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 1);

        // not rescale
        verifyOneKindPartitionableStateRescale(operatorState, operatorID, 2, 2);
    }

    private void verifyOneKindPartitionableStateRescale(
            OperatorState operatorState,
            OperatorID operatorID,
            int oldParallelism,
            int newParallelism) {

        final Map<String, Integer> stateInfoCounts = new HashMap<>();

        verifyAndCollectStateInfo(
                operatorState, operatorID, oldParallelism, newParallelism, stateInfoCounts);

        assertThat(stateInfoCounts).hasSize(2);

        // t-1 and t-2 are SPLIT_DISTRIBUTE state, when rescale up, they will be split to
        // re-distribute.
        if (stateInfoCounts.containsKey("t-1")) {
            if (oldParallelism < newParallelism) {
                assertThat(stateInfoCounts.get("t-1").intValue()).isEqualTo(2);
                assertThat(stateInfoCounts.get("t-2").intValue()).isEqualTo(2);
            } else {
                assertThat(stateInfoCounts.get("t-1").intValue()).isOne();
                assertThat(stateInfoCounts.get("t-2").intValue()).isOne();
            }
        }

        // t-3 and t-4 are UNION state.
        if (stateInfoCounts.containsKey("t-3")) {
            // original two sub-tasks both contain one "t-3" state
            assertThat(stateInfoCounts.get("t-3").intValue()).isEqualTo(2 * newParallelism);
            // only one original sub-task contains one "t-4" state
            assertThat(stateInfoCounts.get("t-4").intValue()).isEqualTo(newParallelism);
        }

        // t-5 and t-6 are BROADCAST state.
        if (stateInfoCounts.containsKey("t-5")) {
            assertThat(stateInfoCounts.get("t-5").intValue()).isEqualTo(newParallelism);
            assertThat(stateInfoCounts.get("t-6").intValue()).isEqualTo(newParallelism);
        }
    }

    private void verifyCombinedPartitionableStateRescale(
            OperatorState operatorState,
            OperatorID operatorID,
            int oldParallelism,
            int newParallelism) {

        final Map<String, Integer> stateInfoCounts = new HashMap<>();

        verifyAndCollectStateInfo(
                operatorState, operatorID, oldParallelism, newParallelism, stateInfoCounts);

        assertThat(stateInfoCounts.size()).isEqualTo(6);
        // t-1 is UNION state and original two sub-tasks both contains one.
        assertThat(stateInfoCounts.get("t-1").intValue()).isEqualTo(2 * newParallelism);
        assertThat(stateInfoCounts.get("t-2").intValue()).isEqualTo(newParallelism);

        // t-3 is SPLIT_DISTRIBUTE state, when rescale up, they will be split to re-distribute.
        if (oldParallelism < newParallelism) {
            assertThat(stateInfoCounts.get("t-3").intValue()).isEqualTo(2);
        } else {
            assertThat(stateInfoCounts.get("t-3").intValue()).isOne();
        }
        assertThat(stateInfoCounts.get("t-4").intValue()).isEqualTo(newParallelism);
        assertThat(stateInfoCounts.get("t-5").intValue()).isEqualTo(newParallelism);
        assertThat(stateInfoCounts.get("t-6").intValue()).isEqualTo(newParallelism);
    }

    /** Check that channel and operator states are assigned to the same tasks on recovery. */
    @Test
    void testChannelStateAssignmentStability() throws JobException, JobExecutionException {
        int numOperators = 10; // note: each operator is places into a separate vertex
        int numSubTasks = 100;

        List<OperatorID> operatorIds = buildOperatorIds(numOperators);
        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, numSubTasks, RANGE, ROUND_ROBIN);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, numSubTasks);

        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        for (OperatorID operatorId : operatorIds) {
            for (int subtaskIdx = 0; subtaskIdx < numSubTasks; subtaskIdx++) {
                assertThat(getAssignedState(vertices.get(operatorId), operatorId, subtaskIdx))
                        .isEqualTo(states.get(operatorId).getState(subtaskIdx));
            }
        }
    }

    @Test
    void testChannelStateAssignmentDownscalingTwoDifferentGates()
            throws JobException, JobExecutionException {
        JobVertex upstream1 = createJobVertex(new OperatorID(), 2);
        JobVertex upstream2 = createJobVertex(new OperatorID(), 2);
        JobVertex downstream = createJobVertex(new OperatorID(), 2);
        List<OperatorID> operatorIds =
                Stream.of(upstream1, upstream2, downstream)
                        .map(v -> v.getOperatorIDs().get(0).getGeneratedOperatorID())
                        .collect(Collectors.toList());
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, 3);

        connectVertices(upstream1, downstream, ARBITRARY, RANGE);
        connectVertices(upstream2, downstream, ROUND_ROBIN, ROUND_ROBIN);

        Map<OperatorID, ExecutionJobVertex> vertices =
                toExecutionVertices(upstream1, upstream2, downstream);

        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(2)), operatorIds.get(2), 0)
                                .getInputRescalingDescriptor())
                .isEqualTo(
                        new InflightDataRescalingDescriptor(
                                array(
                                        gate(
                                                to(0, 1),
                                                mappings(to(0, 2), to(1)),
                                                set(1),
                                                RESCALING),
                                        gate(
                                                to(0, 2),
                                                mappings(to(0, 2), to(1)),
                                                emptySet(),
                                                RESCALING))));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(2)), operatorIds.get(2), 0)
                                .getInputRescalingDescriptor())
                .isEqualTo(
                        new InflightDataRescalingDescriptor(
                                array(
                                        gate(
                                                to(0, 1),
                                                mappings(to(0, 2), to(1)),
                                                set(1),
                                                RESCALING),
                                        gate(
                                                to(0, 2),
                                                mappings(to(0, 2), to(1)),
                                                emptySet(),
                                                RESCALING))));
    }

    private InflightDataGateOrPartitionRescalingDescriptor gate(
            int[] oldIndices,
            RescaleMappings rescaleMapping,
            Set<Integer> ambiguousSubtaskIndexes,
            InflightDataGateOrPartitionRescalingDescriptor.MappingType mappingType) {
        return new InflightDataGateOrPartitionRescalingDescriptor(
                oldIndices, rescaleMapping, ambiguousSubtaskIndexes, mappingType);
    }

    @Test
    void testChannelStateAssignmentDownscaling() throws JobException, JobExecutionException {
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, 3);

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, 2, RANGE, ROUND_ROBIN);

        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        for (OperatorID operatorId : operatorIds) {
            // input is range partitioned, so there is an overlap
            assertState(
                    vertices,
                    operatorId,
                    states,
                    0,
                    OperatorSubtaskState::getInputChannelState,
                    0,
                    1);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    1,
                    OperatorSubtaskState::getInputChannelState,
                    1,
                    2);
            // output is round robin redistributed
            assertState(
                    vertices,
                    operatorId,
                    states,
                    0,
                    OperatorSubtaskState::getResultSubpartitionState,
                    0,
                    2);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    1,
                    OperatorSubtaskState::getResultSubpartitionState,
                    1);
        }

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                                .getOutputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(to(0, 2), array(mappings(to(0, 1), to(1, 2))), set()));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                                .getOutputRescalingDescriptor())
                .isEqualTo(rescalingDescriptor(to(1), array(mappings(to(0, 1), to(1, 2))), set()));

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                                .getInputRescalingDescriptor())
                .isEqualTo(rescalingDescriptor(to(0, 1), array(mappings(to(0, 2), to(1))), set(1)));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                                .getInputRescalingDescriptor())
                .isEqualTo(rescalingDescriptor(to(1, 2), array(mappings(to(0, 2), to(1))), set(1)));
    }

    @Test
    void testChannelStateAssignmentNoRescale() throws JobException, JobExecutionException {
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, 2);

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, 2, RANGE, ROUND_ROBIN);

        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        for (OperatorID operatorId : operatorIds) {
            // input is range partitioned, so there is an overlap
            assertState(
                    vertices, operatorId, states, 0, OperatorSubtaskState::getInputChannelState, 0);
            assertState(
                    vertices, operatorId, states, 1, OperatorSubtaskState::getInputChannelState, 1);
            // output is round robin redistributed
            assertState(
                    vertices,
                    operatorId,
                    states,
                    0,
                    OperatorSubtaskState::getResultSubpartitionState,
                    0);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    1,
                    OperatorSubtaskState::getResultSubpartitionState,
                    1);
        }

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                                .getOutputRescalingDescriptor())
                .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                                .getOutputRescalingDescriptor())
                .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                                .getInputRescalingDescriptor())
                .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                                .getInputRescalingDescriptor())
                .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);
    }

    @Test
    void testChannelStateAssignmentUpscaling() throws JobException, JobExecutionException {
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, 2);

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, 3, RANGE, ROUND_ROBIN);

        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        for (OperatorID operatorId : operatorIds) {
            // input is range partitioned, so there is an overlap
            assertState(
                    vertices, operatorId, states, 0, OperatorSubtaskState::getInputChannelState, 0);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    1,
                    OperatorSubtaskState::getInputChannelState,
                    0,
                    1);
            assertState(
                    vertices, operatorId, states, 2, OperatorSubtaskState::getInputChannelState, 1);
            // output is round robin redistributed
            assertState(
                    vertices,
                    operatorId,
                    states,
                    0,
                    OperatorSubtaskState::getResultSubpartitionState,
                    0);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    1,
                    OperatorSubtaskState::getResultSubpartitionState,
                    1);
            assertState(
                    vertices,
                    operatorId,
                    states,
                    2,
                    OperatorSubtaskState::getResultSubpartitionState);
        }

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                                .getOutputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(to(0), array(mappings(to(0), to(0, 1), to(1))), set()));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                                .getOutputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(to(1), array(mappings(to(0), to(0, 1), to(1))), set()));
        // unmapped subtask index, so nothing to do
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 2)
                                .getOutputRescalingDescriptor())
                .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                                .getInputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(to(0), array(mappings(to(0), to(1), to())), set(0, 1)));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                                .getInputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(
                                to(0, 1), array(mappings(to(0), to(1), to())), set(0, 1)));
        assertThat(
                        getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 2)
                                .getInputRescalingDescriptor())
                .isEqualTo(
                        rescalingDescriptor(to(1), array(mappings(to(0), to(1), to())), set(0, 1)));
    }

    @Test
    void testOnlyUpstreamChannelStateAssignment() throws JobException, JobExecutionException {
        // given: There is only input channel state for one subpartition.
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states = new HashMap<>();
        Random random = new Random();
        OperatorState upstreamState = new OperatorState(operatorIds.get(0), 2, MAX_P);
        OperatorSubtaskState state =
                OperatorSubtaskState.builder()
                        .setResultSubpartitionState(
                                new StateObjectCollection<>(
                                        asList(
                                                createNewResultSubpartitionStateHandle(10, random),
                                                createNewResultSubpartitionStateHandle(
                                                        10, random))))
                        .build();
        upstreamState.putState(0, state);

        states.put(operatorIds.get(0), upstreamState);

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, 3, RANGE, ROUND_ROBIN);

        // when: States are assigned.
        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        // then: All subtask have not null TaskRestore information(even if it is empty).
        ExecutionJobVertex jobVertexWithFinishedOperator = vertices.get(operatorIds.get(0));
        for (ExecutionVertex task : jobVertexWithFinishedOperator.getTaskVertices()) {
            assertThat(task.getCurrentExecutionAttempt().getTaskRestore()).isNotNull();
        }

        ExecutionJobVertex jobVertexWithoutFinishedOperator = vertices.get(operatorIds.get(1));
        for (ExecutionVertex task : jobVertexWithoutFinishedOperator.getTaskVertices()) {
            assertThat(task.getCurrentExecutionAttempt().getTaskRestore()).isNotNull();
        }
    }

    /** FLINK-31963: Tests rescaling for stateless operators and upstream result partition state. */
    @Test
    void testOnlyUpstreamChannelRescaleStateAssignment()
            throws JobException, JobExecutionException {
        Random random = new Random();
        OperatorSubtaskState upstreamOpState =
                OperatorSubtaskState.builder()
                        .setResultSubpartitionState(
                                new StateObjectCollection<>(
                                        asList(
                                                createNewResultSubpartitionStateHandle(10, random),
                                                createNewResultSubpartitionStateHandle(
                                                        10, random))))
                        .build();
        testOnlyUpstreamOrDownstreamRescalingInternal(upstreamOpState, null, 5, 7);
    }

    /** FLINK-31963: Tests rescaling for stateless operators and downstream input channel state. */
    @Test
    void testOnlyDownstreamChannelRescaleStateAssignment()
            throws JobException, JobExecutionException {
        Random random = new Random();
        OperatorSubtaskState downstreamOpState =
                OperatorSubtaskState.builder()
                        .setInputChannelState(
                                new StateObjectCollection<>(
                                        asList(
                                                createNewInputChannelStateHandle(10, random),
                                                createNewInputChannelStateHandle(10, random))))
                        .build();
        testOnlyUpstreamOrDownstreamRescalingInternal(null, downstreamOpState, 5, 5);
    }

    private void testOnlyUpstreamOrDownstreamRescalingInternal(
            @Nullable OperatorSubtaskState upstreamOpState,
            @Nullable OperatorSubtaskState downstreamOpState,
            int expectedUpstreamCount,
            int expectedDownstreamCount)
            throws JobException, JobExecutionException {

        checkArgument(
                upstreamOpState != downstreamOpState
                        && (upstreamOpState == null || downstreamOpState == null),
                "Either upstream or downstream state must exist, but not both");

        // Start from parallelism 5 for both operators
        int upstreamParallelism = 5;
        int downstreamParallelism = 5;

        // Build states
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states = new HashMap<>();
        OperatorState upstreamState =
                new OperatorState(operatorIds.get(0), upstreamParallelism, MAX_P);
        OperatorState downstreamState =
                new OperatorState(operatorIds.get(1), downstreamParallelism, MAX_P);

        states.put(operatorIds.get(0), upstreamState);
        states.put(operatorIds.get(1), downstreamState);

        if (upstreamOpState != null) {
            upstreamState.putState(0, upstreamOpState);
            // rescale downstream 5 -> 3
            downstreamParallelism = 3;
        }

        if (downstreamOpState != null) {
            downstreamState.putState(0, downstreamOpState);
            // rescale upstream 5 -> 3
            upstreamParallelism = 3;
        }

        List<OperatorIdWithParallelism> opIdWithParallelism = new ArrayList<>(2);
        opIdWithParallelism.add(
                new OperatorIdWithParallelism(operatorIds.get(0), upstreamParallelism));
        opIdWithParallelism.add(
                new OperatorIdWithParallelism(operatorIds.get(1), downstreamParallelism));

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(opIdWithParallelism, RANGE, ROUND_ROBIN);

        // Run state assignment
        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        // Check results
        ExecutionJobVertex upstreamExecutionJobVertex = vertices.get(operatorIds.get(0));
        ExecutionJobVertex downstreamExecutionJobVertex = vertices.get(operatorIds.get(1));

        List<TaskStateSnapshot> upstreamTaskStateSnapshots =
                getTaskStateSnapshotFromVertex(upstreamExecutionJobVertex);
        List<TaskStateSnapshot> downstreamTaskStateSnapshots =
                getTaskStateSnapshotFromVertex(downstreamExecutionJobVertex);

        checkMappings(
                upstreamTaskStateSnapshots,
                TaskStateSnapshot::getOutputRescalingDescriptor,
                expectedUpstreamCount);

        checkMappings(
                downstreamTaskStateSnapshots,
                TaskStateSnapshot::getInputRescalingDescriptor,
                expectedDownstreamCount);
    }

    private void checkMappings(
            List<TaskStateSnapshot> taskStateSnapshots,
            Function<TaskStateSnapshot, InflightDataRescalingDescriptor> extractFun,
            int expectedCount) {
        assertThat(
                        taskStateSnapshots.stream()
                                .map(extractFun)
                                .mapToInt(
                                        x -> {
                                            int len = x.getOldSubtaskIndexes(0).length;
                                            // Assert that there is a mapping.
                                            assertThat(len).isGreaterThan(0);
                                            return len;
                                        })
                                .sum())
                .isEqualTo(expectedCount);
    }

    @Test
    void testStateWithFullyFinishedOperators() throws JobException, JobExecutionException {
        List<OperatorID> operatorIds = buildOperatorIds(2);
        Map<OperatorID, OperatorState> states =
                buildOperatorStates(Collections.singletonList(operatorIds.get(1)), 3);

        // Create an operator state marked as finished
        OperatorState operatorState = new FullyFinishedOperatorState(operatorIds.get(0), 3, 256);
        states.put(operatorIds.get(0), operatorState);

        Map<OperatorID, ExecutionJobVertex> vertices =
                buildVertices(operatorIds, 2, RANGE, ROUND_ROBIN);
        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false)
                .assignStates();

        // Check the job vertex with only finished operator.
        ExecutionJobVertex jobVertexWithFinishedOperator = vertices.get(operatorIds.get(0));
        for (ExecutionVertex task : jobVertexWithFinishedOperator.getTaskVertices()) {
            JobManagerTaskRestore taskRestore = task.getCurrentExecutionAttempt().getTaskRestore();
            assertThat(taskRestore.getTaskStateSnapshot().isTaskDeployedAsFinished()).isTrue();
        }

        // Check the job vertex without finished operator.
        ExecutionJobVertex jobVertexWithoutFinishedOperator = vertices.get(operatorIds.get(1));
        for (ExecutionVertex task : jobVertexWithoutFinishedOperator.getTaskVertices()) {
            JobManagerTaskRestore taskRestore = task.getCurrentExecutionAttempt().getTaskRestore();
            assertThat(taskRestore.getTaskStateSnapshot().isTaskDeployedAsFinished()).isFalse();
        }
    }

    private void assertState(
            Map<OperatorID, ExecutionJobVertex> vertices,
            OperatorID operatorId,
            Map<OperatorID, OperatorState> states,
            int newSubtaskIndex,
            Function<OperatorSubtaskState, StateObjectCollection<?>> extractor,
            int... oldSubtaskIndexes) {
        final OperatorSubtaskState subState =
                getAssignedState(vertices.get(operatorId), operatorId, newSubtaskIndex);

        assertThat(
                        extractor
                                .apply(subState)
                                .containsAll(
                                        Arrays.stream(oldSubtaskIndexes)
                                                .boxed()
                                                .flatMap(
                                                        oldIndex ->
                                                                extractor
                                                                        .apply(
                                                                                states.get(
                                                                                                operatorId)
                                                                                        .getState(
                                                                                                oldIndex))
                                                                        .stream())
                                                .collect(Collectors.toList())))
                .isTrue();
    }

    @Test
    void assigningStatesShouldWorkWithUserDefinedOperatorIdsAsWell() {
        int numSubTasks = 1;
        OperatorID operatorId = new OperatorID();
        OperatorID userDefinedOperatorId = new OperatorID();
        List<OperatorID> operatorIds = singletonList(userDefinedOperatorId);

        ExecutionJobVertex executionJobVertex =
                buildExecutionJobVertex(operatorId, userDefinedOperatorId, 1);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, numSubTasks);

        new StateAssignmentOperation(0, Collections.singleton(executionJobVertex), states, false)
                .assignStates();

        assertThat(getAssignedState(executionJobVertex, operatorId, 0))
                .isEqualTo(states.get(userDefinedOperatorId).getState(0));
    }

    @Test
    void assigningStateHandlesCanNotBeNull() {
        OperatorState state = new OperatorState(new OperatorID(), 1, MAX_P);

        List<KeyedStateHandle> managedKeyedStateHandles =
                StateAssignmentOperation.getManagedKeyedStateHandles(state, KeyGroupRange.of(0, 1));

        List<KeyedStateHandle> rawKeyedStateHandles =
                StateAssignmentOperation.getRawKeyedStateHandles(state, KeyGroupRange.of(0, 1));

        assertThat(managedKeyedStateHandles).isEmpty();
        assertThat(rawKeyedStateHandles).isEmpty();
    }

    private List<OperatorID> buildOperatorIds(int numOperators) {
        return IntStream.range(0, numOperators)
                .mapToObj(j -> new OperatorID())
                .collect(Collectors.toList());
    }

    private Map<OperatorID, OperatorState> buildOperatorStates(
            List<OperatorID> operatorIDs, int numSubTasks) {
        Random random = new Random();
        final OperatorID lastId = operatorIDs.get(operatorIDs.size() - 1);
        return operatorIDs.stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                operatorID -> {
                                    OperatorState state =
                                            new OperatorState(operatorID, numSubTasks, MAX_P);
                                    for (int i = 0; i < numSubTasks; i++) {
                                        state.putState(
                                                i,
                                                OperatorSubtaskState.builder()
                                                        .setManagedOperatorState(
                                                                new StateObjectCollection<>(
                                                                        asList(
                                                                                createNewOperatorStateHandle(
                                                                                        10, random),
                                                                                createNewOperatorStateHandle(
                                                                                        10,
                                                                                        random))))
                                                        .setRawOperatorState(
                                                                new StateObjectCollection<>(
                                                                        asList(
                                                                                createNewOperatorStateHandle(
                                                                                        10, random),
                                                                                createNewOperatorStateHandle(
                                                                                        10,
                                                                                        random))))
                                                        .setManagedKeyedState(
                                                                StateObjectCollection.singleton(
                                                                        createNewKeyedStateHandle(
                                                                                KeyGroupRange.of(
                                                                                        i, i))))
                                                        .setRawKeyedState(
                                                                StateObjectCollection.singleton(
                                                                        createNewKeyedStateHandle(
                                                                                KeyGroupRange.of(
                                                                                        i, i))))
                                                        .setInputChannelState(
                                                                operatorID == operatorIDs.get(0)
                                                                        ? StateObjectCollection
                                                                                .empty()
                                                                        : new StateObjectCollection<>(
                                                                                asList(
                                                                                        createNewInputChannelStateHandle(
                                                                                                10,
                                                                                                random),
                                                                                        createNewInputChannelStateHandle(
                                                                                                10,
                                                                                                random))))
                                                        .setResultSubpartitionState(
                                                                operatorID == lastId
                                                                        ? StateObjectCollection
                                                                                .empty()
                                                                        : new StateObjectCollection<>(
                                                                                asList(
                                                                                        createNewResultSubpartitionStateHandle(
                                                                                                10,
                                                                                                random),
                                                                                        createNewResultSubpartitionStateHandle(
                                                                                                10,
                                                                                                random))))
                                                        .build());
                                    }
                                    return state;
                                }));
    }

    private static class OperatorIdWithParallelism {
        private final OperatorID operatorID;
        private final int parallelism;

        public OperatorID getOperatorID() {
            return operatorID;
        }

        public int getParallelism() {
            return parallelism;
        }

        public OperatorIdWithParallelism(OperatorID operatorID, int parallelism) {
            this.operatorID = operatorID;
            this.parallelism = parallelism;
        }
    }

    private Map<OperatorID, ExecutionJobVertex> buildVertices(
            List<OperatorID> operatorIds,
            int parallelisms,
            SubtaskStateMapper downstreamRescaler,
            SubtaskStateMapper upstreamRescaler)
            throws JobException, JobExecutionException {
        List<OperatorIdWithParallelism> opIdsWithParallelism =
                operatorIds.stream()
                        .map(operatorID -> new OperatorIdWithParallelism(operatorID, parallelisms))
                        .collect(Collectors.toList());
        return buildVertices(opIdsWithParallelism, downstreamRescaler, upstreamRescaler);
    }

    private Map<OperatorID, ExecutionJobVertex> buildVertices(
            List<OperatorIdWithParallelism> operatorIdsAndParallelism,
            SubtaskStateMapper downstreamRescaler,
            SubtaskStateMapper upstreamRescaler)
            throws JobException, JobExecutionException {
        final JobVertex[] jobVertices =
                operatorIdsAndParallelism.stream()
                        .map(
                                idWithParallelism ->
                                        createJobVertex(
                                                idWithParallelism.getOperatorID(),
                                                idWithParallelism.getOperatorID(),
                                                idWithParallelism.getParallelism()))
                        .toArray(JobVertex[]::new);
        for (int index = 1; index < jobVertices.length; index++) {
            connectVertices(
                    jobVertices[index - 1],
                    jobVertices[index],
                    upstreamRescaler,
                    downstreamRescaler);
        }

        return toExecutionVertices(jobVertices);
    }

    private Map<OperatorID, ExecutionJobVertex> toExecutionVertices(JobVertex... jobVertices)
            throws JobException, JobExecutionException {
        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertices);
        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_EXTENSION.getExecutor());
        return Arrays.stream(jobVertices)
                .collect(
                        Collectors.toMap(
                                jobVertex ->
                                        jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID(),
                                jobVertex -> {
                                    try {
                                        return eg.getJobVertex(jobVertex.getID());
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                }));
    }

    private void connectVertices(
            JobVertex upstream,
            JobVertex downstream,
            SubtaskStateMapper upstreamRescaler,
            SubtaskStateMapper downstreamRescaler) {
        final JobEdge jobEdge =
                downstream.connectNewDataSetAsInput(
                        upstream, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        jobEdge.setDownstreamSubtaskStateMapper(downstreamRescaler);
        jobEdge.setUpstreamSubtaskStateMapper(upstreamRescaler);
    }

    private ExecutionJobVertex buildExecutionJobVertex(
            OperatorID operatorID, OperatorID userDefinedOperatorId, int parallelism) {
        try {
            JobVertex jobVertex = createJobVertex(operatorID, userDefinedOperatorId, parallelism);
            return ExecutionGraphTestUtils.getExecutionJobVertex(jobVertex);
        } catch (Exception e) {
            throw new AssertionError("Cannot create ExecutionJobVertex", e);
        }
    }

    private JobVertex createJobVertex(OperatorID operatorID, int parallelism) {
        return createJobVertex(operatorID, operatorID, parallelism);
    }

    private JobVertex createJobVertex(
            OperatorID operatorID, OperatorID userDefinedOperatorId, int parallelism) {
        JobVertex jobVertex =
                new JobVertex(
                        operatorID.toHexString(),
                        new JobVertexID(),
                        singletonList(OperatorIDPair.of(operatorID, userDefinedOperatorId)));
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(parallelism);
        return jobVertex;
    }

    private List<TaskStateSnapshot> getTaskStateSnapshotFromVertex(
            ExecutionJobVertex executionJobVertex) {
        return Arrays.stream(executionJobVertex.getTaskVertices())
                .map(ExecutionVertex::getCurrentExecutionAttempt)
                .map(Execution::getTaskRestore)
                .map(JobManagerTaskRestore::getTaskStateSnapshot)
                .collect(Collectors.toList());
    }

    private OperatorSubtaskState getAssignedState(
            ExecutionJobVertex executionJobVertex, OperatorID operatorId, int subtaskIdx) {
        return executionJobVertex
                .getTaskVertices()[subtaskIdx]
                .getCurrentExecutionAttempt()
                .getTaskRestore()
                .getTaskStateSnapshot()
                .getSubtaskStateByOperatorID(operatorId);
    }
}
