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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ForwardGroupComputeUtil}. */
class ForwardGroupComputeUtilTest {

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
    void testIsolatedVertices() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, 0);
    }

    @Test
    void testIsolatedChainedStreamNodeGroups() throws Exception {
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodeByStartNode =
                new LinkedHashMap<>();
        Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = Collections.emptyMap();
        for (int i = 1; i <= 3; ++i) {
            StreamNode streamNode = createStreamNode(i);
            topologicallySortedChainedStreamNodeByStartNode.put(
                    streamNode, Collections.singletonList(streamNode));
        }

        Set<ForwardGroup> groups =
                computeForwardGroups(
                        topologicallySortedChainedStreamNodeByStartNode,
                        forwardProducersByStartNode);

        // Different from the job vertex forward group, the stream node forward group is allowed to
        // contain only one single stream node, as these groups may merge with other groups in the
        // future.
        checkGroupSize(groups, 3, 1);
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
    void testVariousResultPartitionTypesBetweenVertices() throws Exception {
        testThreeVerticesConnectSequentially(false, true, 1, 2);
        testThreeVerticesConnectSequentially(false, false, 0);
        testThreeVerticesConnectSequentially(true, true, 1, 3);
    }

    private void testThreeVerticesConnectSequentially(
            boolean isForward1, boolean isForward2, int numOfGroups, Integer... groupSizes)
            throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        if (isForward1) {
            v1.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        }

        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        if (isForward2) {
            v2.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        }

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, numOfGroups, groupSizes);
    }

    @Test
    void testVariousConnectTypesBetweenChainedStreamNodeGroup() throws Exception {
        testThreeChainedStreamNodeGroupsConnectSequentially(false, true, 2, 1, 2);
        testThreeChainedStreamNodeGroupsConnectSequentially(false, false, 3, 1);
        testThreeChainedStreamNodeGroupsConnectSequentially(true, true, 1, 3);
    }

    private void testThreeChainedStreamNodeGroupsConnectSequentially(
            boolean isForward1, boolean isForward2, int numOfGroups, Integer... groupSizes)
            throws Exception {
        List<StreamNode> startNodes = new ArrayList<>();
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodeByStartNode =
                new HashMap<>();
        Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = new HashMap<>();

        for (int i = 1; i <= 3; i++) {
            StreamNode streamNode = createStreamNode(i);
            startNodes.add(streamNode);
            topologicallySortedChainedStreamNodeByStartNode
                    .computeIfAbsent(streamNode, k -> new ArrayList<>())
                    .add(streamNode);
        }

        if (isForward1) {
            forwardProducersByStartNode
                    .computeIfAbsent(startNodes.get(1), k -> new HashSet<>())
                    .add(startNodes.get(0));
        }

        if (isForward2) {
            forwardProducersByStartNode
                    .computeIfAbsent(startNodes.get(2), k -> new HashSet<>())
                    .add(startNodes.get(1));
        }

        Set<ForwardGroup> groups =
                computeForwardGroups(
                        topologicallySortedChainedStreamNodeByStartNode,
                        forwardProducersByStartNode);

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
    void testTwoInputsMergesIntoOne() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        JobVertex v2 = new JobVertex("v2");
        JobVertex v3 = new JobVertex("v3");
        JobVertex v4 = new JobVertex("v4");

        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v1.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v2.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    @Test
    void testTwoInputsMergesIntoOneForStreamNodeForwardGroup() throws Exception {
        List<StreamNode> startNodes = new ArrayList<>();
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodeByStartNode =
                new LinkedHashMap<>();
        Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = new HashMap<>();

        for (int i = 1; i <= 4; i++) {
            StreamNode streamNode = createStreamNode(i);
            startNodes.add(streamNode);
            topologicallySortedChainedStreamNodeByStartNode
                    .computeIfAbsent(streamNode, k -> new ArrayList<>())
                    .add(streamNode);
        }

        forwardProducersByStartNode
                .computeIfAbsent(startNodes.get(2), k -> new HashSet<>())
                .add(startNodes.get(0));

        forwardProducersByStartNode
                .computeIfAbsent(startNodes.get(2), k -> new HashSet<>())
                .add(startNodes.get(1));

        Set<ForwardGroup> groups =
                computeForwardGroups(
                        topologicallySortedChainedStreamNodeByStartNode,
                        forwardProducersByStartNode);

        checkGroupSize(groups, 2, 3, 1);
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
    void testOneInputSplitsIntoTwo() throws Exception {
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
        v2.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        v2.getProducedDataSets().get(1).getConsumers().get(0).setForward(true);

        Set<ForwardGroup> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    @Test
    void testOneInputSplitsIntoTwoForStreamNodeForwardGroup() throws Exception {

        List<StreamNode> startNodes = new ArrayList<>();
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodeByStartNode =
                new LinkedHashMap<>();
        Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = new HashMap<>();

        for (int i = 1; i <= 4; i++) {
            StreamNode streamNode = createStreamNode(i);
            startNodes.add(streamNode);
            topologicallySortedChainedStreamNodeByStartNode
                    .computeIfAbsent(streamNode, k -> new ArrayList<>())
                    .add(streamNode);
        }

        forwardProducersByStartNode
                .computeIfAbsent(startNodes.get(3), k -> new HashSet<>())
                .add(startNodes.get(1));

        forwardProducersByStartNode
                .computeIfAbsent(startNodes.get(2), k -> new HashSet<>())
                .add(startNodes.get(1));

        Set<ForwardGroup> groups =
                computeForwardGroups(
                        topologicallySortedChainedStreamNodeByStartNode,
                        forwardProducersByStartNode);

        checkGroupSize(groups, 2, 3, 1);
    }

    private static Set<ForwardGroup> computeForwardGroups(JobVertex... vertices) {
        Arrays.asList(vertices).forEach(vertex -> vertex.setInvokableClass(NoOpInvokable.class));
        return new HashSet<>(
                ForwardGroupComputeUtil.computeForwardGroupsAndCheckParallelism(
                                Arrays.asList(vertices))
                        .values());
    }

    private static void checkGroupSize(
            Set<ForwardGroup> groups, int numOfGroups, Integer... sizes) {
        assertThat(groups.size()).isEqualTo(numOfGroups);
        assertThat(groups.stream().map(ForwardGroup::size).collect(Collectors.toList()))
                .contains(sizes);
    }

    private static StreamNode createStreamNode(int id) {
        return new StreamNode(id, null, null, (StreamOperator<?>) null, null, null);
    }

    private static Set<ForwardGroup> computeForwardGroups(
            Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodeByStartNode,
            Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode) {
        return new HashSet<>(
                ForwardGroupComputeUtil.computeStreamNodeForwardGroupAndCheckParallelism(
                                topologicallySortedChainedStreamNodeByStartNode,
                                startNodeId ->
                                        forwardProducersByStartNode.getOrDefault(
                                                startNodeId, Collections.emptySet()))
                        .values());
    }
}
