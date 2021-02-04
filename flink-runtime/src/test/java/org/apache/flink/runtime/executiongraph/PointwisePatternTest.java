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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PointwisePatternTest {

    @Test
    public void testNToN() throws Exception {
        final int N = 23;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(N);
        v2.setParallelism(N);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(1, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex(), consumedPartitions.get(0).getPartitionNumber());
        }
    }

    @Test
    public void test2NToN() throws Exception {
        final int N = 17;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(2 * N);
        v2.setParallelism(N);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(2, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() * 2,
                    consumedPartitions.get(0).getPartitionNumber());
            assertEquals(
                    ev.getParallelSubtaskIndex() * 2 + 1,
                    consumedPartitions.get(1).getPartitionNumber());
        }
    }

    @Test
    public void test3NToN() throws Exception {
        final int N = 17;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(3 * N);
        v2.setParallelism(N);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(3, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() * 3,
                    consumedPartitions.get(0).getPartitionNumber());
            assertEquals(
                    ev.getParallelSubtaskIndex() * 3 + 1,
                    consumedPartitions.get(1).getPartitionNumber());
            assertEquals(
                    ev.getParallelSubtaskIndex() * 3 + 2,
                    consumedPartitions.get(2).getPartitionNumber());
        }
    }

    @Test
    public void testNTo2N() throws Exception {
        final int N = 41;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(N);
        v2.setParallelism(2 * N);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(1, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() / 2,
                    consumedPartitions.get(0).getPartitionNumber());
        }
    }

    @Test
    public void testNTo7N() throws Exception {
        final int N = 11;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(N);
        v2.setParallelism(7 * N);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(1, consumedPartitions.size());

            assertEquals(
                    ev.getParallelSubtaskIndex() / 7,
                    consumedPartitions.get(0).getPartitionNumber());
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

    private ExecutionGraph getDummyExecutionGraph() throws Exception {
        return TestingExecutionGraphBuilder.newBuilder().build();
    }

    private void testLowToHigh(int lowDop, int highDop) throws Exception {
        if (highDop < lowDop) {
            throw new IllegalArgumentException();
        }

        final int factor = highDop / lowDop;
        final int delta = highDop % lowDop == 0 ? 0 : 1;

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(lowDop);
        v2.setParallelism(highDop);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        int[] timesUsed = new int[lowDop];

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getConsumedPartitions(0).getResultPartitions();
            assertEquals(1, consumedPartitions.size());

            timesUsed[consumedPartitions.get(0).getPartitionNumber()]++;
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

        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(highDop);
        v2.setParallelism(lowDop);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2));

        ExecutionGraph eg = getDummyExecutionGraph();
        try {
            eg.attachJobGraph(ordered);
        } catch (JobException e) {
            e.printStackTrace();
            fail("Job failed with exception: " + e.getMessage());
        }

        ExecutionJobVertex target = eg.getAllVertices().get(v2.getID());

        int[] timesUsed = new int[highDop];

        for (ExecutionVertex ev : target.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            List<IntermediateResultPartitionID> consumedPartitions =
                    ev.getAllConsumedPartitions().stream()
                            .map(ConsumedPartitionGroup::getResultPartitions)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
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
}
