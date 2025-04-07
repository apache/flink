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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil.computeStreamNodeForwardGroup;
import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.apache.flink.util.Preconditions.checkState;
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

        Set<ForwardGroup<?>> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, 0);
    }

    @Test
    void testIsolatedChainedStreamNodeGroups() throws Exception {
        List<StreamNode> topologicallySortedStreamNodes = createStreamNodes(3);
        Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId = Collections.emptyMap();

        Set<ForwardGroup<?>> groups =
                computeForwardGroups(
                        topologicallySortedStreamNodes, forwardProducersByConsumerNodeId);

        // Different from the job vertex forward group, the stream node forward group is allowed to
        // contain only one single stream node, as these groups may merge with other groups in the
        // future.
        checkGroupSize(groups, 3, 1, 1, 1);
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

        connectNewDataSetAsInput(
                v2,
                v1,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING,
                false,
                isForward1);

        connectNewDataSetAsInput(
                v3,
                v2,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING,
                false,
                isForward2);

        Set<ForwardGroup<?>> groups = computeForwardGroups(v1, v2, v3);

        checkGroupSize(groups, numOfGroups, groupSizes);
    }

    @Test
    void testVariousConnectTypesBetweenChainedStreamNodeGroup() throws Exception {
        testThreeChainedStreamNodeGroupsConnectSequentially(false, true, 2, 1, 2);
        testThreeChainedStreamNodeGroupsConnectSequentially(false, false, 3, 1, 1, 1);
        testThreeChainedStreamNodeGroupsConnectSequentially(true, true, 1, 3);
    }

    private void testThreeChainedStreamNodeGroupsConnectSequentially(
            boolean isForward1, boolean isForward2, int numOfGroups, Integer... groupSizes)
            throws Exception {
        List<StreamNode> topologicallySortedStreamNodes = createStreamNodes(3);
        Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId = new HashMap<>();

        if (isForward1) {
            forwardProducersByConsumerNodeId
                    .computeIfAbsent(topologicallySortedStreamNodes.get(1), k -> new HashSet<>())
                    .add(topologicallySortedStreamNodes.get(0));
        }

        if (isForward2) {
            forwardProducersByConsumerNodeId
                    .computeIfAbsent(topologicallySortedStreamNodes.get(2), k -> new HashSet<>())
                    .add(topologicallySortedStreamNodes.get(1));
        }

        Set<ForwardGroup<?>> groups =
                computeForwardGroups(
                        topologicallySortedStreamNodes, forwardProducersByConsumerNodeId);

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

        connectNewDataSetAsInput(
                v3, v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING, false, true);

        connectNewDataSetAsInput(
                v3, v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING, false, true);
        connectNewDataSetAsInput(
                v4, v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        Set<ForwardGroup<?>> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    @Test
    void testTwoInputsMergesIntoOneForStreamNodeForwardGroup() throws Exception {
        List<StreamNode> topologicallySortedStreamNodes = createStreamNodes(4);
        Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId = new HashMap<>();

        forwardProducersByConsumerNodeId
                .computeIfAbsent(topologicallySortedStreamNodes.get(2), k -> new HashSet<>())
                .add(topologicallySortedStreamNodes.get(0));

        forwardProducersByConsumerNodeId
                .computeIfAbsent(topologicallySortedStreamNodes.get(2), k -> new HashSet<>())
                .add(topologicallySortedStreamNodes.get(1));

        Set<ForwardGroup<?>> groups =
                computeForwardGroups(
                        topologicallySortedStreamNodes, forwardProducersByConsumerNodeId);

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

        connectNewDataSetAsInput(
                v2, v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        connectNewDataSetAsInput(
                v3, v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING, false, true);
        connectNewDataSetAsInput(
                v4, v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING, false, true);

        Set<ForwardGroup<?>> groups = computeForwardGroups(v1, v2, v3, v4);

        checkGroupSize(groups, 1, 3);
    }

    @Test
    void testOneInputSplitsIntoTwoForStreamNodeForwardGroup() throws Exception {
        List<StreamNode> topologicallySortedStreamNodes = createStreamNodes(4);
        Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId = new HashMap<>();
        forwardProducersByConsumerNodeId
                .computeIfAbsent(topologicallySortedStreamNodes.get(3), k -> new HashSet<>())
                .add(topologicallySortedStreamNodes.get(1));
        forwardProducersByConsumerNodeId
                .computeIfAbsent(topologicallySortedStreamNodes.get(2), k -> new HashSet<>())
                .add(topologicallySortedStreamNodes.get(1));
        Set<ForwardGroup<?>> groups =
                computeForwardGroups(
                        topologicallySortedStreamNodes, forwardProducersByConsumerNodeId);
        checkGroupSize(groups, 2, 3, 1);
    }

    private static Set<ForwardGroup<?>> computeForwardGroups(JobVertex... vertices) {
        Arrays.asList(vertices).forEach(vertex -> vertex.setInvokableClass(NoOpInvokable.class));
        return new HashSet<>(
                ForwardGroupComputeUtil.computeForwardGroupsAndCheckParallelism(
                                Arrays.asList(vertices))
                        .values());
    }

    private static void checkGroupSize(
            Set<ForwardGroup<?>> groups, int numOfGroups, Integer... sizes) {
        assertThat(groups.size()).isEqualTo(numOfGroups);
        assertThat(
                        groups.stream()
                                .map(
                                        group -> {
                                            if (group instanceof JobVertexForwardGroup) {
                                                return ((JobVertexForwardGroup) group).size();
                                            } else {
                                                return ((StreamNodeForwardGroup) group).size();
                                            }
                                        })
                                .collect(Collectors.toList()))
                .contains(sizes);
    }

    private static StreamNode createStreamNode(int id) {
        return new StreamNode(id, null, null, (StreamOperator<?>) null, null, null);
    }

    private static List<StreamNode> createStreamNodes(int count) {
        List<StreamNode> streamNodes = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            streamNodes.add(new StreamNode(i, null, null, (StreamOperator<?>) null, null, null));
        }
        return streamNodes;
    }

    private static Set<ForwardGroup<?>> computeForwardGroups(
            List<StreamNode> topologicallySortedStreamNodes,
            Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId) {
        return new HashSet<>(
                computeStreamNodeForwardGroupAndCheckParallelism(
                                topologicallySortedStreamNodes,
                                id ->
                                        forwardProducersByConsumerNodeId.getOrDefault(
                                                id, Collections.emptySet()))
                        .values());
    }

    public static Map<Integer, StreamNodeForwardGroup>
            computeStreamNodeForwardGroupAndCheckParallelism(
                    final Iterable<StreamNode> topologicallySortedStreamNodes,
                    final Function<StreamNode, Set<StreamNode>> forwardProducersRetriever) {
        final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeId =
                computeStreamNodeForwardGroup(
                        topologicallySortedStreamNodes, forwardProducersRetriever);
        topologicallySortedStreamNodes.forEach(
                startNode -> {
                    StreamNodeForwardGroup forwardGroup =
                            forwardGroupsByStartNodeId.get(startNode.getId());
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        checkState(startNode.getParallelism() == forwardGroup.getParallelism());
                    }
                });
        return forwardGroupsByStartNodeId;
    }
}
