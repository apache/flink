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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.IntermediateResultPartitionTest.computeVertexParallelismStoreConsideringDynamicGraph;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link EdgeManagerBuildUtil} to verify the max number of connecting edges between
 * vertices for pattern of both {@link DistributionPattern#POINTWISE} and {@link
 * DistributionPattern#ALL_TO_ALL}.
 */
class EdgeManagerBuildUtilTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetMaxNumEdgesToTargetInPointwiseConnection() throws Exception {
        testGetMaxNumEdgesToTarget(17, 17, POINTWISE);
        testGetMaxNumEdgesToTarget(17, 23, POINTWISE);
        testGetMaxNumEdgesToTarget(17, 34, POINTWISE);
        testGetMaxNumEdgesToTarget(34, 17, POINTWISE);
        testGetMaxNumEdgesToTarget(23, 17, POINTWISE);
    }

    @Test
    void testGetMaxNumEdgesToTargetInAllToAllConnection() throws Exception {
        testGetMaxNumEdgesToTarget(17, 17, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(17, 23, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(17, 34, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(34, 17, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(23, 17, ALL_TO_ALL);
    }

    @Test
    void testConnectAllToAll() throws Exception {
        int upstream = 3;
        int downstream = 2;

        // use dynamic graph to specify the vertex input info
        ExecutionGraph eg = setupExecutionGraph(upstream, downstream, POINTWISE, true);

        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < downstream; i++) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(
                            i,
                            new IndexRange(0, upstream - 1),
                            // the subpartition range will not be used in edge manager, so set (0,
                            // 0)
                            new IndexRange(0, 0)));
        }
        final JobVertexInputInfo jobVertexInputInfo =
                new JobVertexInputInfo(executionVertexInputInfos);

        final Iterator<ExecutionJobVertex> vertexIterator =
                eg.getVerticesTopologically().iterator();
        final ExecutionJobVertex producer = vertexIterator.next();
        final ExecutionJobVertex consumer = vertexIterator.next();

        // initialize producer and consumer
        eg.initializeJobVertex(
                producer,
                1L,
                Collections.emptyMap(),
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        eg.initializeJobVertex(
                consumer,
                1L,
                Collections.singletonMap(
                        producer.getProducedDataSets()[0].getId(), jobVertexInputInfo),
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(producer.getJobVertexId()))
                        .getProducedDataSets()[0];
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];
        IntermediateResultPartition partition3 = result.getPartitions()[2];

        ExecutionVertex vertex1 = consumer.getTaskVertices()[0];
        ExecutionVertex vertex2 = consumer.getTaskVertices()[1];

        // check consumers of the partitions
        ConsumerVertexGroup consumerVertexGroup = partition1.getConsumerVertexGroups().get(0);
        assertThat(consumerVertexGroup).containsExactlyInAnyOrder(vertex1.getID(), vertex2.getID());
        assertThat(partition2.getConsumerVertexGroups().get(0)).isEqualTo(consumerVertexGroup);
        assertThat(partition3.getConsumerVertexGroups().get(0)).isEqualTo(consumerVertexGroup);

        // check inputs of the execution vertices
        ConsumedPartitionGroup consumedPartitionGroup = vertex1.getConsumedPartitionGroup(0);
        assertThat(consumedPartitionGroup)
                .containsExactlyInAnyOrder(
                        partition1.getPartitionId(),
                        partition2.getPartitionId(),
                        partition3.getPartitionId());
        assertThat(vertex2.getConsumedPartitionGroup(0)).isEqualTo(consumedPartitionGroup);

        // check the consumerVertexGroup and consumedPartitionGroup are set to each other
        assertThat(consumerVertexGroup.getConsumedPartitionGroup())
                .isEqualTo(consumedPartitionGroup);
        assertThat(consumedPartitionGroup.getConsumerVertexGroup()).isEqualTo(consumerVertexGroup);
    }

    @Test
    void testConnectPointwise() throws Exception {
        int upstream = 4;
        int downstream = 4;

        // use dynamic graph to specify the vertex input info
        ExecutionGraph eg = setupExecutionGraph(upstream, downstream, POINTWISE, true);

        // set partition ranges
        List<IndexRange> partitionRanges =
                Arrays.asList(
                        new IndexRange(0, 0),
                        new IndexRange(0, 0),
                        new IndexRange(1, 2),
                        new IndexRange(3, 3));
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < downstream; i++) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(
                            // the subpartition range will not be used in edge manager, so set (0,
                            // 0)
                            i, partitionRanges.get(i), new IndexRange(0, 0)));
        }
        final JobVertexInputInfo jobVertexInputInfo =
                new JobVertexInputInfo(executionVertexInputInfos);

        final Iterator<ExecutionJobVertex> vertexIterator =
                eg.getVerticesTopologically().iterator();
        final ExecutionJobVertex producer = vertexIterator.next();
        final ExecutionJobVertex consumer = vertexIterator.next();

        // initialize producer and consumer
        eg.initializeJobVertex(
                producer,
                1L,
                Collections.emptyMap(),
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        eg.initializeJobVertex(
                consumer,
                1L,
                Collections.singletonMap(
                        producer.getProducedDataSets()[0].getId(), jobVertexInputInfo),
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        IntermediateResult result =
                Objects.requireNonNull(eg.getJobVertex(producer.getJobVertexId()))
                        .getProducedDataSets()[0];
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];
        IntermediateResultPartition partition3 = result.getPartitions()[2];
        IntermediateResultPartition partition4 = result.getPartitions()[3];

        ExecutionVertex vertex1 = consumer.getTaskVertices()[0];
        ExecutionVertex vertex2 = consumer.getTaskVertices()[1];
        ExecutionVertex vertex3 = consumer.getTaskVertices()[2];
        ExecutionVertex vertex4 = consumer.getTaskVertices()[3];

        // check consumers of the partitions
        ConsumerVertexGroup consumerVertexGroup1 = partition1.getConsumerVertexGroups().get(0);
        ConsumerVertexGroup consumerVertexGroup2 = partition2.getConsumerVertexGroups().get(0);
        ConsumerVertexGroup consumerVertexGroup3 = partition4.getConsumerVertexGroups().get(0);
        assertThat(consumerVertexGroup1)
                .containsExactlyInAnyOrder(vertex1.getID(), vertex2.getID());
        assertThat(consumerVertexGroup2).containsExactlyInAnyOrder(vertex3.getID());
        assertThat(partition3.getConsumerVertexGroups().get(0)).isEqualTo(consumerVertexGroup2);
        assertThat(consumerVertexGroup3).containsExactlyInAnyOrder(vertex4.getID());

        // check inputs of the execution vertices
        ConsumedPartitionGroup consumedPartitionGroup1 = vertex1.getConsumedPartitionGroup(0);
        ConsumedPartitionGroup consumedPartitionGroup2 = vertex3.getConsumedPartitionGroup(0);
        ConsumedPartitionGroup consumedPartitionGroup3 = vertex4.getConsumedPartitionGroup(0);
        assertThat(consumedPartitionGroup1).containsExactlyInAnyOrder(partition1.getPartitionId());
        assertThat(vertex2.getConsumedPartitionGroup(0)).isEqualTo(consumedPartitionGroup1);
        assertThat(consumedPartitionGroup2)
                .containsExactlyInAnyOrder(
                        partition2.getPartitionId(), partition3.getPartitionId());
        assertThat(consumedPartitionGroup3).containsExactlyInAnyOrder(partition4.getPartitionId());

        // check the consumerVertexGroups and consumedPartitionGroups are properly set
        assertThat(consumerVertexGroup1.getConsumedPartitionGroup())
                .isEqualTo(consumedPartitionGroup1);
        assertThat(consumedPartitionGroup1.getConsumerVertexGroup())
                .isEqualTo(consumerVertexGroup1);
        assertThat(consumerVertexGroup2.getConsumedPartitionGroup())
                .isEqualTo(consumedPartitionGroup2);
        assertThat(consumedPartitionGroup2.getConsumerVertexGroup())
                .isEqualTo(consumerVertexGroup2);
        assertThat(consumerVertexGroup3.getConsumedPartitionGroup())
                .isEqualTo(consumedPartitionGroup3);
        assertThat(consumedPartitionGroup3.getConsumerVertexGroup())
                .isEqualTo(consumerVertexGroup3);
    }

    private void testGetMaxNumEdgesToTarget(
            int upstream, int downstream, DistributionPattern pattern) throws Exception {

        Pair<ExecutionJobVertex, ExecutionJobVertex> pair =
                setupExecutionGraph(upstream, downstream, pattern);
        ExecutionJobVertex upstreamEJV = pair.getLeft();
        ExecutionJobVertex downstreamEJV = pair.getRight();

        int calculatedMaxForUpstream =
                EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                        upstream, downstream, pattern);
        int actualMaxForUpstream = -1;
        for (ExecutionVertex ev : upstreamEJV.getTaskVertices()) {
            assertThat(ev.getProducedPartitions()).hasSize(1);

            IntermediateResultPartition partition =
                    ev.getProducedPartitions().values().iterator().next();
            ConsumerVertexGroup consumerVertexGroup = partition.getConsumerVertexGroups().get(0);
            int actual = consumerVertexGroup.size();
            if (actual > actualMaxForUpstream) {
                actualMaxForUpstream = actual;
            }
        }
        assertThat(actualMaxForUpstream).isEqualTo(calculatedMaxForUpstream);

        int calculatedMaxForDownstream =
                EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                        downstream, upstream, pattern);
        int actualMaxForDownstream = -1;
        for (ExecutionVertex ev : downstreamEJV.getTaskVertices()) {
            assertThat(ev.getNumberOfInputs()).isOne();

            int actual = ev.getConsumedPartitionGroup(0).size();
            if (actual > actualMaxForDownstream) {
                actualMaxForDownstream = actual;
            }
        }
        assertThat(actualMaxForDownstream).isEqualTo(calculatedMaxForDownstream);
    }

    private Pair<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraph(
            int upstream, int downstream, DistributionPattern pattern) throws Exception {
        Iterator<ExecutionJobVertex> jobVertices =
                setupExecutionGraph(upstream, downstream, pattern, false)
                        .getVerticesTopologically()
                        .iterator();
        return Pair.of(jobVertices.next(), jobVertices.next());
    }

    private ExecutionGraph setupExecutionGraph(
            int upstream, int downstream, DistributionPattern pattern, boolean isDynamicGraph)
            throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(upstream);
        v2.setParallelism(downstream);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(v1, pattern, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        TestingDefaultExecutionGraphBuilder builder =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                computeVertexParallelismStoreConsideringDynamicGraph(
                                        ordered, isDynamicGraph, 128));
        ExecutionGraph eg;
        if (isDynamicGraph) {
            eg = builder.buildDynamicGraph(EXECUTOR_RESOURCE.getExecutor());
        } else {
            eg = builder.build(EXECUTOR_RESOURCE.getExecutor());
        }

        eg.attachJobGraph(
                ordered, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        return eg;
    }
}
