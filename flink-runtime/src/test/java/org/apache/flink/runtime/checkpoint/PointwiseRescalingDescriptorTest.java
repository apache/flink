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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.JobVertexConnectionUtils;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper.ROUND_ROBIN;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for POINTWISE edge rescaling descriptor generation in {@link
 * StateAssignmentOperation} and {@link TaskStateAssignment}.
 */
class PointwiseRescalingDescriptorTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static final int MAX_P = 256;

    // ===== Test 1: POINTWISE same parallelism => IDENTITY =====
    @Test
    void testPointwiseSameParallelismIsIdentity() throws Exception {
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 3;
        int newPar = 3;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectPointwise(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.POINTWISE);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // Same parallelism + same pattern => IDENTITY (NO_RESCALE)
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            assertThat(sinkState.getInputRescalingDescriptor())
                    .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

            OperatorSubtaskState sourceState =
                    getAssignedState(vertices.get(sourceId), sourceId, subtask);
            assertThat(sourceState.getOutputRescalingDescriptor())
                    .isEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);
        }
    }

    // ===== Test 2: POINTWISE scale up - subtasks with old state get descriptors =====
    @Test
    void testPointwiseScaleUp() throws Exception {
        // old: source(2) --PW--> sink(2), new: source(4) --PW--> sink(4)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 2;
        int newPar = 4;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectPointwise(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.POINTWISE);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // Subtasks that absorb old state should get POINTWISE descriptors (newUpPar > 0)
        int subtasksWithDescriptor = 0;
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();

            if (!inputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                subtasksWithDescriptor++;
                InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                        inputDesc.getGateOrPartitionDescriptor(0);
                assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism())
                        .isEqualTo(newPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getNewDownParallelism())
                        .isEqualTo(newPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldUpParallelism())
                        .isEqualTo(oldPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldDownParallelism())
                        .isEqualTo(oldPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                        .isEqualTo(DistributionPattern.POINTWISE);
                assertThat(gateDesc.isIdentity()).isFalse();

                int[] oldSubtaskIndexes = inputDesc.getOldSubtaskIndexes(0);
                assertThat(oldSubtaskIndexes.length).isGreaterThan(0);
            }
        }
        // At least the subtasks that got old state must have descriptors
        assertThat(subtasksWithDescriptor).isGreaterThan(0);

        // Verify output side similarly
        int sourceSubtasksWithDescriptor = 0;
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sourceState =
                    getAssignedState(vertices.get(sourceId), sourceId, subtask);
            InflightDataRescalingDescriptor outputDesc = sourceState.getOutputRescalingDescriptor();

            if (!outputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                sourceSubtasksWithDescriptor++;
                InflightDataGateOrPartitionRescalingDescriptor partDesc =
                        outputDesc.getGateOrPartitionDescriptor(0);
                assertThat(partDesc.getPointwiseRescaleParams().getNewUpParallelism())
                        .isEqualTo(newPar);
                assertThat(partDesc.getPointwiseRescaleParams().getNewDownParallelism())
                        .isEqualTo(newPar);
                assertThat(partDesc.getPointwiseRescaleParams().getOldUpParallelism())
                        .isEqualTo(oldPar);
                assertThat(partDesc.getPointwiseRescaleParams().getOldDownParallelism())
                        .isEqualTo(oldPar);
                assertThat(partDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                        .isEqualTo(DistributionPattern.POINTWISE);
            }
        }
        assertThat(sourceSubtasksWithDescriptor).isGreaterThan(0);
    }

    // ===== Test 3: POINTWISE scale down - all old subtasks covered =====
    @Test
    void testPointwiseScaleDown() throws Exception {
        // old: source(4) --PW--> sink(4), new: source(2) --PW--> sink(2)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 4;
        int newPar = 2;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectPointwise(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.POINTWISE);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // Scale down: every new subtask absorbs state from multiple old subtasks
        boolean[] covered = new boolean[oldPar];
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();
            assertThat(inputDesc)
                    .as("subtask %d should have rescaling descriptor", subtask)
                    .isNotEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

            InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                    inputDesc.getGateOrPartitionDescriptor(0);
            assertThat(gateDesc.getPointwiseRescaleParams().getOldUpParallelism())
                    .isEqualTo(oldPar);
            assertThat(gateDesc.getPointwiseRescaleParams().getOldDownParallelism())
                    .isEqualTo(oldPar);
            assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism())
                    .isEqualTo(newPar);
            assertThat(gateDesc.getPointwiseRescaleParams().getNewDownParallelism())
                    .isEqualTo(newPar);
            assertThat(gateDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                    .isEqualTo(DistributionPattern.POINTWISE);

            int[] oldSubtaskIndexes = inputDesc.getOldSubtaskIndexes(0);
            assertThat(oldSubtaskIndexes.length).isGreaterThan(0);
            for (int oldIdx : oldSubtaskIndexes) {
                covered[oldIdx] = true;
            }
        }
        // Every old subtask must be assigned to some new subtask
        for (int i = 0; i < oldPar; i++) {
            assertThat(covered[i]).as("Old subtask %d should be covered", i).isTrue();
        }
    }

    // ===== Test 4: A2A -> POINTWISE migration =====
    @Test
    void testAllToAllToPointwiseMigration() throws Exception {
        // old: source(3) --A2A--> sink(3), new: source(3) --PW--> sink(3)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 3;
        int newPar = 3;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectPointwise(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        // Old pattern was A2A
        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.ALL_TO_ALL);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // A2A->PW same-par: new sink k reads from old sink k only (ROUND_ROBIN 1:1).
        // TM-side recoverPointwise then routes each old upstream's buffer to the correct new
        // upstream channel via computePrimaryUpstreamMapping, so reading the extra old sinks would
        // cause each inflight record to be delivered to all new subtasks (3x duplicates).
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();
            assertThat(inputDesc)
                    .as("subtask %d", subtask)
                    .isNotEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

            int[] oldSubtaskIndexes = inputDesc.getOldSubtaskIndexes(0);
            // ROUND_ROBIN 1:1 for same parallelism: new sink k -> old sink k
            assertThat(oldSubtaskIndexes).isEqualTo(new int[] {subtask});

            InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                    inputDesc.getGateOrPartitionDescriptor(0);
            assertThat(gateDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                    .isEqualTo(DistributionPattern.ALL_TO_ALL);
            assertThat(gateDesc.getPointwiseRescaleParams().getNewDistributionPattern())
                    .isEqualTo(DistributionPattern.POINTWISE);
            assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism())
                    .isEqualTo(newPar);
        }
    }

    // ===== Test 5: POINTWISE -> A2A migration =====
    @Test
    void testPointwiseToAllToAllMigration() throws Exception {
        // old: source(3) --PW--> sink(3), new: source(3) --A2A--> sink(3)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 3;
        int newPar = 3;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectAllToAll(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        // Old pattern was POINTWISE
        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.POINTWISE);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // PW->A2A: enters the pointwise path (oldPattern != A2A)
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();
            assertThat(inputDesc)
                    .as("subtask %d", subtask)
                    .isNotEqualTo(InflightDataRescalingDescriptor.NO_RESCALE);

            InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                    inputDesc.getGateOrPartitionDescriptor(0);
            assertThat(gateDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                    .isEqualTo(DistributionPattern.POINTWISE);
            assertThat(gateDesc.getPointwiseRescaleParams().getNewDistributionPattern())
                    .isEqualTo(DistributionPattern.ALL_TO_ALL);
            // PW->A2A: newUpParallelism > 0 confirms pointwise path used
            assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism())
                    .isEqualTo(newPar);
        }
    }

    // ===== Test 6: Pure A2A rescaling (regression - original path unchanged) =====
    @Test
    void testPureAllToAllRescalingRegression() throws Exception {
        // old: source(2) --A2A--> sink(2), new: source(3) --A2A--> sink(3)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 2;
        int newPar = 3;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectAllToAll(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        // Old pattern was A2A (same as new)
        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.ALL_TO_ALL);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // A2A->A2A: should use the original all-to-all path
        // The descriptor should have newUpParallelism == 0 (no pointwise fields)
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();

            if (!inputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                        inputDesc.getGateOrPartitionDescriptor(0);
                // A2A->A2A uses 4-arg constructor, scalars all 0
                assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism()).isEqualTo(0);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldUpParallelism()).isEqualTo(0);
            }
        }
    }

    // ===== Test 7: POINTWISE cross-parallelism =====
    @Test
    void testPointwiseCrossParallelism() throws Exception {
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldSourcePar = 3;
        int oldSinkPar = 2;
        int newSourcePar = 4;
        int newSinkPar = 3;

        JobVertex source = createJobVertex(sourceId, newSourcePar);
        JobVertex sink = createJobVertex(sinkId, newSinkPar);
        connectPointwise(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);

        Map<OperatorID, OperatorState> states = new HashMap<>();
        Random random = new Random(42);

        OperatorState sourceState = new OperatorState(null, null, sourceId, oldSourcePar, MAX_P);
        for (int i = 0; i < oldSourcePar; i++) {
            sourceState.putState(
                    i,
                    OperatorSubtaskState.builder()
                            .setResultSubpartitionState(
                                    new StateObjectCollection<>(
                                            Collections.singletonList(
                                                    createNewResultSubpartitionStateHandle(
                                                            10, i, 0, 0, random))))
                            .build());
        }
        states.put(sourceId, sourceState);

        OperatorState sinkState = new OperatorState(null, null, sinkId, oldSinkPar, MAX_P);
        for (int i = 0; i < oldSinkPar; i++) {
            sinkState.putState(
                    i,
                    OperatorSubtaskState.builder()
                            .setInputChannelState(
                                    new StateObjectCollection<>(
                                            Collections.singletonList(
                                                    createNewInputChannelStateHandle(
                                                            10, i, 0, random))))
                            .build());
        }
        states.put(sinkId, sinkState);

        EdgeDistributionPatternSnapshot oldEdgePatterns =
                buildEdgePatterns(sourceId, DistributionPattern.POINTWISE);

        new StateAssignmentOperation(
                        0, new HashSet<>(vertices.values()), states, false, true, oldEdgePatterns)
                .assignStates();

        // Verify input descriptors carry correct cross-parallelism values
        int subtasksWithDesc = 0;
        for (int subtask = 0; subtask < newSinkPar; subtask++) {
            OperatorSubtaskState assigned = getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = assigned.getInputRescalingDescriptor();

            if (!inputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                subtasksWithDesc++;
                InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                        inputDesc.getGateOrPartitionDescriptor(0);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldUpParallelism())
                        .isEqualTo(oldSourcePar);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldDownParallelism())
                        .isEqualTo(oldSinkPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism())
                        .isEqualTo(newSourcePar);
                assertThat(gateDesc.getPointwiseRescaleParams().getNewDownParallelism())
                        .isEqualTo(newSinkPar);
                assertThat(gateDesc.getPointwiseRescaleParams().getOldDistributionPattern())
                        .isEqualTo(DistributionPattern.POINTWISE);
            }
        }
        assertThat(subtasksWithDesc).isGreaterThan(0);

        // Verify output descriptors
        int sourceSubtasksWithDesc = 0;
        for (int subtask = 0; subtask < newSourcePar; subtask++) {
            OperatorSubtaskState assigned =
                    getAssignedState(vertices.get(sourceId), sourceId, subtask);
            InflightDataRescalingDescriptor outputDesc = assigned.getOutputRescalingDescriptor();

            if (!outputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                sourceSubtasksWithDesc++;
                InflightDataGateOrPartitionRescalingDescriptor partDesc =
                        outputDesc.getGateOrPartitionDescriptor(0);
                assertThat(partDesc.getPointwiseRescaleParams().getOldUpParallelism())
                        .isEqualTo(oldSourcePar);
                assertThat(partDesc.getPointwiseRescaleParams().getOldDownParallelism())
                        .isEqualTo(oldSinkPar);
                assertThat(partDesc.getPointwiseRescaleParams().getNewUpParallelism())
                        .isEqualTo(newSourcePar);
                assertThat(partDesc.getPointwiseRescaleParams().getNewDownParallelism())
                        .isEqualTo(newSinkPar);
            }
        }
        assertThat(sourceSubtasksWithDesc).isGreaterThan(0);
    }

    // ===== Test 8: No EdgeDistributionPatternSnapshot -> treated as A2A (backward compat) =====
    @Test
    void testNullEdgePatternsDefaultsToAllToAll() throws Exception {
        // old: source(2) --?--> sink(2), new: source(3) --A2A--> sink(3)
        OperatorID sourceId = new OperatorID();
        OperatorID sinkId = new OperatorID();
        int oldPar = 2;
        int newPar = 3;

        JobVertex source = createJobVertex(sourceId, newPar);
        JobVertex sink = createJobVertex(sinkId, newPar);
        connectAllToAll(source, sink);

        Map<OperatorID, ExecutionJobVertex> vertices = toExecutionVertices(source, sink);
        Map<OperatorID, OperatorState> states =
                buildStatesWithChannelState(sourceId, sinkId, oldPar);

        // No edge pattern snapshot (null) - backward compatible
        new StateAssignmentOperation(0, new HashSet<>(vertices.values()), states, false, true, null)
                .assignStates();

        // Should produce standard A2A descriptors (newUpParallelism == 0)
        for (int subtask = 0; subtask < newPar; subtask++) {
            OperatorSubtaskState sinkState =
                    getAssignedState(vertices.get(sinkId), sinkId, subtask);
            InflightDataRescalingDescriptor inputDesc = sinkState.getInputRescalingDescriptor();

            if (!inputDesc.equals(InflightDataRescalingDescriptor.NO_RESCALE)) {
                InflightDataGateOrPartitionRescalingDescriptor gateDesc =
                        inputDesc.getGateOrPartitionDescriptor(0);
                assertThat(gateDesc.getPointwiseRescaleParams().getNewUpParallelism()).isEqualTo(0);
            }
        }
    }

    // ===== Helper methods =====

    private JobVertex createJobVertex(OperatorID operatorID, int parallelism) {
        JobVertex jobVertex =
                new JobVertex(
                        operatorID.toHexString(),
                        new JobVertexID(),
                        Collections.singletonList(
                                OperatorIDPair.of(operatorID, operatorID, null, null)));
        jobVertex.setInvokableClass(NoOpInvokable.class);
        jobVertex.setParallelism(parallelism);
        return jobVertex;
    }

    private void connectPointwise(JobVertex upstream, JobVertex downstream) {
        final JobEdge jobEdge =
                JobVertexConnectionUtils.connectNewDataSetAsInput(
                        downstream,
                        upstream,
                        DistributionPattern.POINTWISE,
                        ResultPartitionType.PIPELINED);
        jobEdge.setDownstreamSubtaskStateMapper(ROUND_ROBIN);
        jobEdge.setUpstreamSubtaskStateMapper(ROUND_ROBIN);
    }

    private void connectAllToAll(JobVertex upstream, JobVertex downstream) {
        final JobEdge jobEdge =
                JobVertexConnectionUtils.connectNewDataSetAsInput(
                        downstream,
                        upstream,
                        DistributionPattern.ALL_TO_ALL,
                        ResultPartitionType.PIPELINED);
        jobEdge.setDownstreamSubtaskStateMapper(ROUND_ROBIN);
        jobEdge.setUpstreamSubtaskStateMapper(ROUND_ROBIN);
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

    private Map<OperatorID, OperatorState> buildStatesWithChannelState(
            OperatorID sourceId, OperatorID sinkId, int oldParallelism) {
        Map<OperatorID, OperatorState> states = new HashMap<>();
        Random random = new Random(42);

        // For POINTWISE same parallelism: each subtask connects to exactly one downstream
        // via subpartition 0
        OperatorState sourceState = new OperatorState(null, null, sourceId, oldParallelism, MAX_P);
        for (int i = 0; i < oldParallelism; i++) {
            sourceState.putState(
                    i,
                    OperatorSubtaskState.builder()
                            .setResultSubpartitionState(
                                    new StateObjectCollection<>(
                                            Collections.singletonList(
                                                    createNewResultSubpartitionStateHandle(
                                                            10, i, 0, 0, random))))
                            .build());
        }
        states.put(sourceId, sourceState);

        OperatorState sinkState = new OperatorState(null, null, sinkId, oldParallelism, MAX_P);
        for (int i = 0; i < oldParallelism; i++) {
            sinkState.putState(
                    i,
                    OperatorSubtaskState.builder()
                            .setInputChannelState(
                                    new StateObjectCollection<>(
                                            Collections.singletonList(
                                                    createNewInputChannelStateHandle(
                                                            10, i, 0, random))))
                            .build());
        }
        states.put(sinkId, sinkState);

        return states;
    }

    private EdgeDistributionPatternSnapshot buildEdgePatterns(
            OperatorID sourceId, DistributionPattern pattern) {
        Map<OperatorID, DistributionPattern[]> patterns = new HashMap<>();
        patterns.put(sourceId, new DistributionPattern[] {pattern});
        return new EdgeDistributionPatternSnapshot(patterns);
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
