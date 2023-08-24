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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for building {@link DistributionPattern#POINTWISE} connections in {@link
 * VertexInputInfoComputationUtils#computeVertexInputInfoForPointwise}.
 */
class PointwisePatternTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testNToN() throws Exception {
        final int N = 23;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(1);

            assertThat(ev.getParallelSubtaskIndex())
                    .isEqualTo(consumedPartitionGroup.getFirst().getPartitionNumber());
        }
    }

    @Test
    void test2NToN() throws Exception {
        final int N = 17;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(2 * N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(2);

            int idx = 0;
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                assertThat(ev.getParallelSubtaskIndex() * 2L + idx++)
                        .isEqualTo(partitionId.getPartitionNumber());
            }
        }
    }

    @Test
    void test3NToN() throws Exception {
        final int N = 17;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(3 * N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(3);

            int idx = 0;
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                assertThat(ev.getParallelSubtaskIndex() * 3L + idx++)
                        .isEqualTo(partitionId.getPartitionNumber());
            }
        }
    }

    @Test
    void testNTo2N() throws Exception {
        final int N = 41;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, 2 * N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(1);

            assertThat(ev.getParallelSubtaskIndex() / 2)
                    .isEqualTo(consumedPartitionGroup.getFirst().getPartitionNumber());
        }
    }

    @Test
    void testNTo7N() throws Exception {
        final int N = 11;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, 7 * N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(1);

            assertThat(ev.getParallelSubtaskIndex() / 7)
                    .isEqualTo(consumedPartitionGroup.getFirst().getPartitionNumber());
        }
    }

    @Test
    void testLowHighIrregular() throws Exception {
        testLowToHigh(3, 16);
        testLowToHigh(19, 21);
        testLowToHigh(15, 20);
        testLowToHigh(11, 31);
        testLowToHigh(11, 29);
    }

    @Test
    void testHighLowIrregular() throws Exception {
        testHighToLow(16, 3);
        testHighToLow(21, 19);
        testHighToLow(20, 15);
        testHighToLow(31, 11);
    }

    /**
     * Verify the connection sequences for POINTWISE edges is correct and make sure the descendant
     * logic of building POINTWISE edges follows the initial logic.
     */
    @Test
    void testPointwiseConnectionSequence() throws Exception {
        // upstream parallelism < downstream parallelism
        testConnections(3, 5, new int[][] {{0}, {0}, {1}, {1}, {2}});
        testConnections(3, 10, new int[][] {{0}, {0}, {0}, {0}, {1}, {1}, {1}, {2}, {2}, {2}});
        testConnections(4, 6, new int[][] {{0}, {0}, {1}, {2}, {2}, {3}});
        testConnections(6, 10, new int[][] {{0}, {0}, {1}, {1}, {2}, {3}, {3}, {4}, {4}, {5}});

        // upstream parallelism > downstream parallelism
        testConnections(5, 3, new int[][] {{0}, {1, 2}, {3, 4}});
        testConnections(10, 3, new int[][] {{0, 1, 2}, {3, 4, 5}, {6, 7, 8, 9}});
        testConnections(6, 4, new int[][] {{0}, {1, 2}, {3}, {4, 5}});
        testConnections(10, 6, new int[][] {{0}, {1, 2}, {3, 4}, {5}, {6, 7}, {8, 9}});
    }

    private void testLowToHigh(int lowDop, int highDop) throws Exception {
        if (highDop < lowDop) {
            throw new IllegalArgumentException();
        }

        final int factor = highDop / lowDop;
        final int delta = highDop % lowDop == 0 ? 0 : 1;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(lowDop, highDop);

        int[] timesUsed = new int[lowDop];

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);
            assertThat(consumedPartitionGroup).hasSize(1);

            timesUsed[consumedPartitionGroup.getFirst().getPartitionNumber()]++;
        }

        for (int used : timesUsed) {
            assertThat(used >= factor && used <= factor + delta).isTrue();
        }
    }

    private void testHighToLow(int highDop, int lowDop) throws Exception {
        if (highDop < lowDop) {
            throw new IllegalArgumentException();
        }

        final int factor = highDop / lowDop;
        final int delta = highDop % lowDop == 0 ? 0 : 1;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(highDop, lowDop);

        int[] timesUsed = new int[highDop];

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            List<IntermediateResultPartitionID> consumedPartitions = new ArrayList<>();
            for (ConsumedPartitionGroup partitionGroup : ev.getAllConsumedPartitionGroups()) {
                for (IntermediateResultPartitionID partitionId : partitionGroup) {
                    consumedPartitions.add(partitionId);
                }
            }

            assertThat(
                            consumedPartitions.size() >= factor
                                    && consumedPartitions.size() <= factor + delta)
                    .isTrue();

            for (IntermediateResultPartitionID consumedPartition : consumedPartitions) {
                timesUsed[consumedPartition.getPartitionNumber()]++;
            }
        }
        for (int used : timesUsed) {
            assertThat(used).isOne();
        }
    }

    private ExecutionJobVertex setUpExecutionGraphAndGetDownstreamVertex(
            int upstream, int downstream) throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(upstream);
        v2.setParallelism(downstream);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(ordered))
                        .build(EXECUTOR_RESOURCE.getExecutor());
        try {
            eg.attachJobGraph(
                    ordered, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        return eg.getAllVertices().get(v2.getID());
    }

    /** Verify the connections between upstream result partitions and downstream vertices. */
    private void testConnections(
            int sourceParallelism, int targetParallelism, int[][] expectedConsumedPartitionNumber)
            throws Exception {

        ExecutionJobVertex target =
                setUpExecutionGraphAndGetDownstreamVertex(sourceParallelism, targetParallelism);

        for (int vertexIndex = 0; vertexIndex < target.getTaskVertices().length; vertexIndex++) {

            ExecutionVertex ev = target.getTaskVertices()[vertexIndex];
            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitionGroup(0);

            assertThat(expectedConsumedPartitionNumber[vertexIndex].length)
                    .isEqualTo(consumedPartitionGroup.size());

            int partitionIndex = 0;
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                assertThat(expectedConsumedPartitionNumber[vertexIndex][partitionIndex++])
                        .isEqualTo(partitionId.getPartitionNumber());
            }
        }
    }
}
