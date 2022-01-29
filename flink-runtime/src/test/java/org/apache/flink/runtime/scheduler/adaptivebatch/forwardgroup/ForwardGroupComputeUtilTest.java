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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.adaptivebatch.forwardgroup;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link ForwardGroupComputeUtil}. */
public class ForwardGroupComputeUtilTest extends TestLogger {
    /**
     * Tests that the computation of the job graph with isolated vertices works correctly.
     *
     * <pre>
     *     (v1)
     *
     *     (v2)
     *
     *     (v3)
     * </pre>
     */
    @Test
    public void testIsolatedVertices() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, 0);
    }

    /**
     * Tests that the computation of the vertices connected with edges which have various result
     * partition types works correctly.
     *
     * <pre>
     *
     *     (v1) -> (v2) -> (v3)
     *
     * </pre>
     */
    @Test
    public void testVariousResultPartitionTypesBetweenVertices() throws Exception {
        testThreeVerticesConnectSequentially(false, true, 1, 2);
        testThreeVerticesConnectSequentially(false, false, 0);
        testThreeVerticesConnectSequentially(true, true, 1, 3);
    }

    private void testThreeVerticesConnectSequentially(
            boolean isForward1, boolean isForward2, int numOfGroups, int... groupSizes)
            throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        if (isForward1) {
            v1.getProducedDataSets().get(0).getConsumer().setForward(true);
        }

        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        if (isForward2) {
            v2.getProducedDataSets().get(0).getConsumer().setForward(true);
        }

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, numOfGroups, groupSizes);
    }

    /**
     * Tests that the computation of the job graph where two upstream vertices connect with one
     * downstream vertex works correctly.
     *
     * <pre>
     *
     *     (v1) --
     *           |
     *           --> (v3) -> (v4)
     *           |
     *     (v2) --
     *
     * </pre>
     */
    @Test
    public void testTwoInputsMergesIntoOne() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v1.getProducedDataSets().get(0).getConsumer().setForward(true);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v2.getProducedDataSets().get(0).getConsumer().setForward(true);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    /**
     * Tests that the computation of the job graph where one upstream vertex connect with two
     * downstream vertices works correctly.
     *
     * <pre>
     *
     *                    --> (v3)
     *                    |
     *      (v1) -> (v2) --
     *                    |
     *                    --> (v4)
     *
     * </pre>
     */
    @Test
    public void testOneInputSplitsIntoTwo() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v2.getProducedDataSets().get(0).getConsumer().setForward(true);
        v2.getProducedDataSets().get(1).getConsumer().setForward(true);

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    private static Set<ForwardGroup> computeForwardGroups(JobVertex... vertices) throws Exception {
        Arrays.asList(vertices).forEach(vertex -> vertex.setInvokableClass(NoOpInvokable.class));
        ExecutionGraph executionGraph = createDynamicGraph(vertices);
        return new HashSet<>(
                ForwardGroupComputeUtil.computeForwardGroups(
                                Arrays.asList(vertices), executionGraph::getJobVertex)
                        .values());
    }

    private static void checkGroupSize(Set<ForwardGroup> groups, int numOfGroups, int... sizes) {
        assertEquals(numOfGroups, groups.size());
        containsInAnyOrder(
                groups.stream().map(ForwardGroup::size).collect(Collectors.toList()), sizes);
    }

    private static DefaultExecutionGraph createDynamicGraph(JobVertex... vertices)
            throws Exception {

        TestingDefaultExecutionGraphBuilder builder =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(new JobGraph(new JobID(), "TestJob", vertices))
                        .setVertexParallelismStore(
                                AdaptiveBatchScheduler.computeVertexParallelismStoreForDynamicGraph(
                                        Arrays.asList(vertices), 10));
        return builder.buildDynamicGraph();
    }
}
