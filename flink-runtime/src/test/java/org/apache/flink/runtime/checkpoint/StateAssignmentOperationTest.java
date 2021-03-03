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
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
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
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.array;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewKeyedStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewOperatorStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.RANGE;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.ROUND_ROBIN;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests to verify state assignment operation. */
public class StateAssignmentOperationTest extends TestLogger {

    private static final int MAX_P = 256;

    @Test
    public void testRepartitionSplitDistributeStates() {
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
    public void testRepartitionUnionState() {
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
    public void testRepartitionBroadcastState() {
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

    /** Verify repartition logic on partitionable states with all modes. */
    @Test
    public void testReDistributeCombinedPartitionableStates() {
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
                    stateInfoCounts.merge(stateName, 1, (count, inc) -> count + inc);

                    OperatorStateHandle.StateMetaInfo stateMetaInfo =
                            stateNameToMetaInfo.getValue();

                    stateModeOffsets
                            .get(stateMetaInfo.getDistributionMode())
                            .merge(
                                    stateName,
                                    stateMetaInfo.getOffsets().length,
                                    (count, inc) -> count + inc);
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
                        stateOffsets
                                .values()
                                .forEach(length -> Assert.assertEquals(1, (int) length));
                    } else {
                        // SPLIT_DISTRIBUTE: when rescale down to 1 or not rescale, not
                        // re-distribute them.
                        stateOffsets
                                .values()
                                .forEach(length -> Assert.assertEquals(2, (int) length));
                    }
                } else if (OperatorStateHandle.Mode.UNION.equals(mode)) {
                    // UNION: all to all
                    stateOffsets.values().forEach(length -> Assert.assertEquals(2, (int) length));
                } else {
                    // BROADCAST: so all to all
                    stateOffsets.values().forEach(length -> Assert.assertEquals(3, (int) length));
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

        Assert.assertEquals(2, stateInfoCounts.size());

        // t-1 and t-2 are SPLIT_DISTRIBUTE state, when rescale up, they will be split to
        // re-distribute.
        if (stateInfoCounts.containsKey("t-1")) {
            if (oldParallelism < newParallelism) {
                Assert.assertEquals(2, stateInfoCounts.get("t-1").intValue());
                Assert.assertEquals(2, stateInfoCounts.get("t-2").intValue());
            } else {
                Assert.assertEquals(1, stateInfoCounts.get("t-1").intValue());
                Assert.assertEquals(1, stateInfoCounts.get("t-2").intValue());
            }
        }

        // t-3 and t-4 are UNION state.
        if (stateInfoCounts.containsKey("t-3")) {
            // original two sub-tasks both contain one "t-3" state
            Assert.assertEquals(2 * newParallelism, stateInfoCounts.get("t-3").intValue());
            // only one original sub-task contains one "t-4" state
            Assert.assertEquals(newParallelism, stateInfoCounts.get("t-4").intValue());
        }

        // t-5 and t-6 are BROADCAST state.
        if (stateInfoCounts.containsKey("t-5")) {
            Assert.assertEquals(newParallelism, stateInfoCounts.get("t-5").intValue());
            Assert.assertEquals(newParallelism, stateInfoCounts.get("t-6").intValue());
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

        Assert.assertEquals(6, stateInfoCounts.size());
        // t-1 is UNION state and original two sub-tasks both contains one.
        Assert.assertEquals(2 * newParallelism, stateInfoCounts.get("t-1").intValue());
        Assert.assertEquals(newParallelism, stateInfoCounts.get("t-2").intValue());

        // t-3 is SPLIT_DISTRIBUTE state, when rescale up, they will be split to re-distribute.
        if (oldParallelism < newParallelism) {
            Assert.assertEquals(2, stateInfoCounts.get("t-3").intValue());
        } else {
            Assert.assertEquals(1, stateInfoCounts.get("t-3").intValue());
        }
        Assert.assertEquals(newParallelism, stateInfoCounts.get("t-4").intValue());
        Assert.assertEquals(newParallelism, stateInfoCounts.get("t-5").intValue());
        Assert.assertEquals(newParallelism, stateInfoCounts.get("t-6").intValue());
    }

    /** Check that channel and operator states are assigned to the same tasks on recovery. */
    @Test
    public void testChannelStateAssignmentStability() throws JobException, JobExecutionException {
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
                Assert.assertEquals(
                        states.get(operatorId).getState(subtaskIdx),
                        getAssignedState(vertices.get(operatorId), operatorId, subtaskIdx));
            }
        }
    }

    @Test
    public void testChannelStateAssignmentDownscaling() throws JobException, JobExecutionException {
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

        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(0, 2), array(mappings(to(0, 1), to(1, 2))), set()),
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                        .getOutputRescalingDescriptor());
        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(1), array(mappings(to(0, 1), to(1, 2))), set()),
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                        .getOutputRescalingDescriptor());

        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(0, 1), array(mappings(to(0, 2), to(1))), set(1)),
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                        .getInputRescalingDescriptor());
        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(1, 2), array(mappings(to(0, 2), to(1))), set(1)),
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                        .getInputRescalingDescriptor());
    }

    @Test
    public void testChannelStateAssignmentNoRescale() throws JobException, JobExecutionException {
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

        assertEquals(
                InflightDataRescalingDescriptor.NO_RESCALE,
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                        .getOutputRescalingDescriptor());
        assertEquals(
                InflightDataRescalingDescriptor.NO_RESCALE,
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                        .getOutputRescalingDescriptor());

        assertEquals(
                InflightDataRescalingDescriptor.NO_RESCALE,
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                        .getInputRescalingDescriptor());
        assertEquals(
                InflightDataRescalingDescriptor.NO_RESCALE,
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                        .getInputRescalingDescriptor());
    }

    @Test
    public void testChannelStateAssignmentUpscaling() throws JobException, JobExecutionException {
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

        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(0), array(mappings(to(0), to(0, 1), to(1))), set()),
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 0)
                        .getOutputRescalingDescriptor());
        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(1), array(mappings(to(0), to(0, 1), to(1))), set()),
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 1)
                        .getOutputRescalingDescriptor());
        // unmapped subtask index, so nothing to do
        assertEquals(
                InflightDataRescalingDescriptor.NO_RESCALE,
                getAssignedState(vertices.get(operatorIds.get(0)), operatorIds.get(0), 2)
                        .getOutputRescalingDescriptor());

        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(0), array(mappings(to(0), to(1), to())), set(0, 1)),
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 0)
                        .getInputRescalingDescriptor());
        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(0, 1), array(mappings(to(0), to(1), to())), set(0, 1)),
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 1)
                        .getInputRescalingDescriptor());
        assertEquals(
                new InflightDataRescalingDescriptor(
                        to(1), array(mappings(to(0), to(1), to())), set(0, 1)),
                getAssignedState(vertices.get(operatorIds.get(1)), operatorIds.get(1), 2)
                        .getInputRescalingDescriptor());
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
                extractor.apply(subState),
                containsInAnyOrder(
                        Arrays.stream(oldSubtaskIndexes)
                                .boxed()
                                .flatMap(
                                        oldIndex ->
                                                extractor
                                                        .apply(
                                                                states.get(operatorId)
                                                                        .getState(oldIndex))
                                                        .stream())
                                .toArray()));
    }

    @Test
    public void assigningStatesShouldWorkWithUserDefinedOperatorIdsAsWell() {
        int numSubTasks = 1;
        OperatorID operatorId = new OperatorID();
        OperatorID userDefinedOperatorId = new OperatorID();
        List<OperatorID> operatorIds = singletonList(userDefinedOperatorId);

        ExecutionJobVertex executionJobVertex =
                buildExecutionJobVertex(operatorId, userDefinedOperatorId, 1);
        Map<OperatorID, OperatorState> states = buildOperatorStates(operatorIds, numSubTasks);

        new StateAssignmentOperation(0, Collections.singleton(executionJobVertex), states, false)
                .assignStates();

        Assert.assertEquals(
                states.get(userDefinedOperatorId).getState(0),
                getAssignedState(executionJobVertex, operatorId, 0));
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

    private Map<OperatorID, ExecutionJobVertex> buildVertices(
            List<OperatorID> operatorIds,
            int parallelism,
            SubtaskStateMapper downstreamRescaler,
            SubtaskStateMapper upstreamRescaler)
            throws JobException, JobExecutionException {
        final JobVertex[] jobVertices =
                operatorIds.stream()
                        .map(
                                id -> {
                                    final JobVertex jobVertex =
                                            createJobVertex(id, id, parallelism);
                                    return jobVertex;
                                })
                        .toArray(JobVertex[]::new);
        for (int index = 1; index < jobVertices.length; index++) {
            final JobEdge jobEdge =
                    jobVertices[index].connectNewDataSetAsInput(
                            jobVertices[index - 1],
                            DistributionPattern.ALL_TO_ALL,
                            ResultPartitionType.PIPELINED);
            jobEdge.setDownstreamSubtaskStateMapper(downstreamRescaler);
            jobEdge.setUpstreamSubtaskStateMapper(upstreamRescaler);
        }

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertices);
        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
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

    private ExecutionJobVertex buildExecutionJobVertex(
            OperatorID operatorID, OperatorID userDefinedOperatorId, int parallelism) {
        try {
            JobVertex jobVertex = createJobVertex(operatorID, userDefinedOperatorId, parallelism);
            return ExecutionGraphTestUtils.getExecutionJobVertex(jobVertex);
        } catch (Exception e) {
            throw new AssertionError("Cannot create ExecutionJobVertex", e);
        }
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
