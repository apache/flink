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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;

import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link LogicalPipelinedRegionComputeUtil}. */
public class LogicalPipelinedRegionComputeUtilTest {
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
    public void testIsolatedVertices() {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        Set<Set<LogicalVertex>> regions = computePipelinedRegions(v1, v2, v3);

        checkRegionSize(regions, 3, 1, 1, 1);
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
    public void testVariousResultPartitionTypesBetweenVertices() {
        testThreeVerticesConnectSequentially(
                ResultPartitionType.BLOCKING, ResultPartitionType.PIPELINED, 2, 1, 2);
        testThreeVerticesConnectSequentially(
                ResultPartitionType.BLOCKING, ResultPartitionType.BLOCKING, 3, 1, 1, 1);
        testThreeVerticesConnectSequentially(
                ResultPartitionType.PIPELINED, ResultPartitionType.PIPELINED, 1, 3);
    }

    private void testThreeVerticesConnectSequentially(
            ResultPartitionType resultPartitionType1,
            ResultPartitionType resultPartitionType2,
            int numOfRegions,
            int... regionSizes) {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, resultPartitionType1);
        v3.connectNewDataSetAsInput(v2, DistributionPattern.POINTWISE, resultPartitionType2);

        Set<Set<LogicalVertex>> regions = computePipelinedRegions(v1, v2, v3);

        checkRegionSize(regions, numOfRegions, regionSizes);
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
    public void testTwoInputsMergesIntoOne() {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        Set<Set<LogicalVertex>> regions = computePipelinedRegions(v1, v2, v3, v4);

        checkRegionSize(regions, 2, 3, 1);
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
    public void testOneInputSplitsIntoTwo() {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        Set<Set<LogicalVertex>> regions = computePipelinedRegions(v1, v2, v3, v4);

        checkRegionSize(regions, 2, 3, 1);
    }

    /**
     * Tests that the computation of vertices connected like a diamond with both PIPELINED and
     * BLOCKING edges works correctly.
     *
     * <pre>
     *
     *          (BLOCKING)
     *              |
     *              v
     *             --> (v2) ---
     *             |          |
     *      (v1) ---          --> (v4)
     *             |          |
     *             --> (v3) ---
     *
     * </pre>
     */
    @Test
    public void testDiamondWithMixedPipelinedAndBlockingEdges() {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        Set<Set<LogicalVertex>> regions = computePipelinedRegions(v1, v2, v3, v4);

        checkRegionSize(regions, 1, 4);
    }

    private static Set<Set<LogicalVertex>> computePipelinedRegions(JobVertex... vertices) {
        DefaultLogicalTopology topology =
                DefaultLogicalTopology.fromTopologicallySortedJobVertices(Arrays.asList(vertices));
        return LogicalPipelinedRegionComputeUtil.computePipelinedRegions(topology.getVertices());
    }

    private static void checkRegionSize(
            Set<Set<LogicalVertex>> regions, int numOfRegions, int... sizes) {
        assertEquals(numOfRegions, regions.size());
        containsInAnyOrder(regions.stream().map(Set::size).collect(Collectors.toList()), sizes);
    }
}
