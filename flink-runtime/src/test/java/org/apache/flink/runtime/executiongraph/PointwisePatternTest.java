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
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for building {@link DistributionPattern#POINTWISE} connections in {@link
 * EdgeManagerBuildUtil#connectVertexToResult}.
 */
public class PointwisePatternTest {

    @Test
    public void testNToN() throws Exception {
        final int N = 23;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitions(0);
            assertEquals(1, consumedPartitionGroup.size());

            assertEquals(
                    ev.getParallelSubtaskIndex(),
                    consumedPartitionGroup.getFirst().getPartitionNumber());
        }
    }

    @Test
    public void test2NToN() throws Exception {
        final int N = 17;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(2 * N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitions(0);
            assertEquals(2, consumedPartitionGroup.size());

            int idx = 0;
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                assertEquals(
                        ev.getParallelSubtaskIndex() * 2L + idx++,
                        partitionId.getPartitionNumber());
            }
        }
    }

    @Test
    public void test3NToN() throws Exception {
        final int N = 17;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(3 * N, N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitionGroup = ev.getConsumedPartitions(0);
            assertEquals(3, consumedPartitionGroup.size());

            int idx = 0;
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                assertEquals(
                        ev.getParallelSubtaskIndex() * 3L + idx++,
                        partitionId.getPartitionNumber());
            }
        }
    }

    @Test
    public void testNTo2N() throws Exception {
        final int N = 41;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, 2 * N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitions = ev.getConsumedPartitions(0);
            assertEquals(1, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() / 2,
                    consumedPartitions.getFirst().getPartitionNumber());
        }
    }

    @Test
    public void testNTo7N() throws Exception {
        final int N = 11;

        ExecutionJobVertex target = setUpExecutionGraphAndGetDownstreamVertex(N, 7 * N);

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitions = ev.getConsumedPartitions(0);
            assertEquals(1, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() / 7,
                    consumedPartitions.getFirst().getPartitionNumber());
        }
    }

    @Test
    public void testLowHighIrregular() throws Exception {
        testLowToHigh(3, 16);
        testLowToHigh(19, 21);
        testLowToHigh(15, 20);
        testLowToHigh(11, 31);
    }

    @Test
    public void testHighLowIrregular() throws Exception {
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
    public void testPointwiseConnectionSequence() throws Exception {
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

    private ExecutionGraph getDummyExecutionGraph() throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder().build();
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
            assertEquals(1, ev.getNumberOfInputs());

            ConsumedPartitionGroup consumedPartitions = ev.getConsumedPartitions(0);
            assertEquals(1, consumedPartitions.size());

            timesUsed[consumedPartitions.getFirst().getPartitionNumber()]++;
        }

        for (int used : timesUsed) {
            assertTrue(used >= factor && used <= factor + delta);
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
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions = new ArrayList<>();
            for (ConsumedPartitionGroup partitionGroup : ev.getAllConsumedPartitionGroups()) {
                for (IntermediateResultPartitionID partitionId : partitionGroup) {
                    consumedPartitions.add(partitionId);
                }
            }

            assertTrue(
                    consumedPartitions.size() >= factor
                            && consumedPartitions.size() <= factor + delta);

            for (IntermediateResultPartitionID consumedPartition : consumedPartitions) {
                timesUsed[consumedPartition.getPartitionNumber()]++;
            }
        }
        for (int used : timesUsed) {
            assertEquals(1, used);
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

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
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
            ConsumedPartitionGroup partitionIds = ev.getConsumedPartitions(0);

            assertEquals(expectedConsumedPartitionNumber[vertexIndex].length, partitionIds.size());

            int partitionIndex = 0;
            for (IntermediateResultPartitionID partitionId : partitionIds) {
                assertEquals(
                        expectedConsumedPartitionNumber[vertexIndex][partitionIndex++],
                        partitionId.getPartitionNumber());
            }
        }
    }
}
