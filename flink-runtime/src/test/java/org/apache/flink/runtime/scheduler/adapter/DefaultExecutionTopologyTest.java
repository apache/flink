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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.ResultPartitionState;
import org.apache.flink.runtime.scheduler.strategy.SchedulingPipelinedRegion;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.IterableUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionGraph;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link DefaultExecutionTopology}. */
class DefaultExecutionTopologyTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private DefaultExecutionGraph executionGraph;

    private DefaultExecutionTopology adapter;

    @BeforeEach
    void setUp() throws Exception {
        JobVertex[] jobVertices = new JobVertex[2];
        int parallelism = 3;
        jobVertices[0] = createNoOpVertex(parallelism);
        jobVertices[1] = createNoOpVertex(parallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);
        executionGraph = createExecutionGraph(EXECUTOR_RESOURCE.getExecutor(), jobVertices);
        adapter = DefaultExecutionTopology.fromExecutionGraph(executionGraph);
    }

    @Test
    void testConstructor() {
        // implicitly tests order constraint of getVertices()
        assertGraphEquals(executionGraph, adapter);
    }

    @Test
    void testGetResultPartition() {
        for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
            for (Map.Entry<IntermediateResultPartitionID, IntermediateResultPartition> entry :
                    vertex.getProducedPartitions().entrySet()) {
                IntermediateResultPartition partition = entry.getValue();
                DefaultResultPartition schedulingResultPartition =
                        adapter.getResultPartition(entry.getKey());

                assertPartitionEquals(partition, schedulingResultPartition);
            }
        }
    }

    @Test
    void testResultPartitionStateSupplier() throws Exception {
        final JobVertex[] jobVertices = createJobVertices(BLOCKING);
        executionGraph = createExecutionGraph(EXECUTOR_RESOURCE.getExecutor(), jobVertices);
        adapter = DefaultExecutionTopology.fromExecutionGraph(executionGraph);

        final ExecutionJobVertex ejv = executionGraph.getJobVertex(jobVertices[0].getID());
        ExecutionVertex ev = ejv.getTaskVertices()[0];
        IntermediateResultPartition intermediateResultPartition =
                ev.getProducedPartitions().values().stream().findAny().get();

        final DefaultResultPartition schedulingResultPartition =
                adapter.getResultPartition(intermediateResultPartition.getPartitionId());

        assertThat(schedulingResultPartition.getState()).isEqualTo(ResultPartitionState.CREATED);

        ev.finishPartitionsIfNeeded();
        assertThat(schedulingResultPartition.getState())
                .isEqualTo(ResultPartitionState.ALL_DATA_PRODUCED);
    }

    @Test
    void testGetVertexOrThrow() {
        assertThatThrownBy(() -> adapter.getVertex(new ExecutionVertexID(new JobVertexID(), 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testResultPartitionOrThrow() {
        assertThatThrownBy(() -> adapter.getResultPartition(new IntermediateResultPartitionID()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetAllPipelinedRegions() {
        final Iterable<DefaultSchedulingPipelinedRegion> allPipelinedRegions =
                adapter.getAllPipelinedRegions();
        assertThat(allPipelinedRegions).hasSize(1);
    }

    @Test
    void testGetPipelinedRegionOfVertex() {
        for (DefaultExecutionVertex vertex : adapter.getVertices()) {
            final DefaultSchedulingPipelinedRegion pipelinedRegion =
                    adapter.getPipelinedRegionOfVertex(vertex.getId());
            assertRegionContainsAllVertices(pipelinedRegion);
        }
    }

    @Test
    void testErrorIfCoLocatedTasksAreNotInSameRegion() throws Exception {
        int parallelism = 3;
        final JobVertex v1 = createNoOpVertex(parallelism);
        final JobVertex v2 = createNoOpVertex(parallelism);

        SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        v1.setSlotSharingGroup(slotSharingGroup);
        v2.setSlotSharingGroup(slotSharingGroup);
        v1.setStrictlyCoLocatedWith(v2);

        assertThatThrownBy(() -> createExecutionGraph(EXECUTOR_RESOURCE.getExecutor(), v1, v2))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testUpdateTopology() throws Exception {
        final JobVertex[] jobVertices = createJobVertices(BLOCKING);
        executionGraph = createDynamicGraph(jobVertices);
        adapter = DefaultExecutionTopology.fromExecutionGraph(executionGraph);

        final ExecutionJobVertex ejv1 = executionGraph.getJobVertex(jobVertices[0].getID());
        final ExecutionJobVertex ejv2 = executionGraph.getJobVertex(jobVertices[1].getID());

        executionGraph.initializeJobVertex(
                ejv1, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        adapter.notifyExecutionGraphUpdated(executionGraph, Collections.singletonList(ejv1));
        assertThat(adapter.getVertices()).hasSize(3);

        executionGraph.initializeJobVertex(
                ejv2, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        adapter.notifyExecutionGraphUpdated(executionGraph, Collections.singletonList(ejv2));
        assertThat(adapter.getVertices()).hasSize(6);

        assertGraphEquals(executionGraph, adapter);
    }

    @Test
    void testErrorIfUpdateTopologyWithNewVertexPipelinedConnectedToOldOnes() throws Exception {
        final JobVertex[] jobVertices = createJobVertices(PIPELINED);
        executionGraph = createDynamicGraph(jobVertices);
        adapter = DefaultExecutionTopology.fromExecutionGraph(executionGraph);

        final ExecutionJobVertex ejv1 = executionGraph.getJobVertex(jobVertices[0].getID());
        final ExecutionJobVertex ejv2 = executionGraph.getJobVertex(jobVertices[1].getID());

        executionGraph.initializeJobVertex(
                ejv1, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        adapter.notifyExecutionGraphUpdated(executionGraph, Collections.singletonList(ejv1));

        executionGraph.initializeJobVertex(
                ejv2, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        assertThatThrownBy(
                        () ->
                                adapter.notifyExecutionGraphUpdated(
                                        executionGraph, Collections.singletonList(ejv2)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testExistingRegionsAreNotAffectedDuringTopologyUpdate() throws Exception {
        final JobVertex[] jobVertices = createJobVertices(BLOCKING);
        executionGraph = createDynamicGraph(jobVertices);
        adapter = DefaultExecutionTopology.fromExecutionGraph(executionGraph);

        final ExecutionJobVertex ejv1 = executionGraph.getJobVertex(jobVertices[0].getID());
        final ExecutionJobVertex ejv2 = executionGraph.getJobVertex(jobVertices[1].getID());

        executionGraph.initializeJobVertex(
                ejv1, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        adapter.notifyExecutionGraphUpdated(executionGraph, Collections.singletonList(ejv1));
        SchedulingPipelinedRegion regionOld =
                adapter.getPipelinedRegionOfVertex(new ExecutionVertexID(ejv1.getJobVertexId(), 0));

        executionGraph.initializeJobVertex(
                ejv2, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        adapter.notifyExecutionGraphUpdated(executionGraph, Collections.singletonList(ejv2));
        SchedulingPipelinedRegion regionNew =
                adapter.getPipelinedRegionOfVertex(new ExecutionVertexID(ejv1.getJobVertexId(), 0));

        assertThat(regionNew).isSameAs(regionOld);
    }

    private JobVertex[] createJobVertices(ResultPartitionType resultPartitionType) {
        final JobVertex[] jobVertices = new JobVertex[2];
        final int parallelism = 3;
        jobVertices[0] = createNoOpVertex(parallelism);
        jobVertices[1] = createNoOpVertex(parallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, resultPartitionType);

        return jobVertices;
    }

    private DefaultExecutionGraph createDynamicGraph(JobVertex... jobVertices) throws Exception {
        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(new JobGraph(new JobID(), "TestJob", jobVertices))
                .buildDynamicGraph(EXECUTOR_RESOURCE.getExecutor());
    }

    private void assertRegionContainsAllVertices(
            final DefaultSchedulingPipelinedRegion pipelinedRegionOfVertex) {
        final Set<DefaultExecutionVertex> allVertices =
                Sets.newHashSet(pipelinedRegionOfVertex.getVertices());
        assertThat(allVertices).isEqualTo(Sets.newHashSet(adapter.getVertices()));
    }

    private static void assertGraphEquals(
            ExecutionGraph originalGraph, DefaultExecutionTopology adaptedTopology) {

        Iterator<ExecutionVertex> originalVertices =
                originalGraph.getAllExecutionVertices().iterator();
        Iterator<DefaultExecutionVertex> adaptedVertices = adaptedTopology.getVertices().iterator();

        while (originalVertices.hasNext()) {
            ExecutionVertex originalVertex = originalVertices.next();
            DefaultExecutionVertex adaptedVertex = adaptedVertices.next();

            assertThat(adaptedVertex.getId()).isEqualTo(originalVertex.getID());

            List<IntermediateResultPartition> originalConsumedPartitions = new ArrayList<>();
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    originalVertex.getAllConsumedPartitionGroups()) {
                for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                    IntermediateResultPartition partition =
                            originalVertex
                                    .getExecutionGraphAccessor()
                                    .getResultPartitionOrThrow(partitionId);
                    originalConsumedPartitions.add(partition);
                }
            }
            Iterable<DefaultResultPartition> adaptedConsumedPartitions =
                    adaptedVertex.getConsumedResults();

            assertPartitionsEquals(originalConsumedPartitions, adaptedConsumedPartitions);

            Collection<IntermediateResultPartition> originalProducedPartitions =
                    originalVertex.getProducedPartitions().values();
            Iterable<DefaultResultPartition> adaptedProducedPartitions =
                    adaptedVertex.getProducedResults();

            assertPartitionsEquals(originalProducedPartitions, adaptedProducedPartitions);
        }

        assertThat(adaptedVertices)
                .as("Number of adapted vertices exceeds number of original vertices.")
                .isExhausted();
    }

    private static void assertPartitionsEquals(
            Iterable<IntermediateResultPartition> originalResultPartitions,
            Iterable<DefaultResultPartition> adaptedResultPartitions) {

        assertThat(originalResultPartitions).hasSameSizeAs(adaptedResultPartitions);

        for (IntermediateResultPartition originalPartition : originalResultPartitions) {
            DefaultResultPartition adaptedPartition =
                    IterableUtils.toStream(adaptedResultPartitions)
                            .filter(
                                    adapted ->
                                            adapted.getId()
                                                    .equals(originalPartition.getPartitionId()))
                            .findAny()
                            .orElseThrow(
                                    () ->
                                            new AssertionError(
                                                    "Could not find matching adapted partition for "
                                                            + originalPartition));

            assertPartitionEquals(originalPartition, adaptedPartition);

            List<ExecutionVertexID> originalConsumerIds = new ArrayList<>();
            for (ConsumerVertexGroup consumerVertexGroup :
                    originalPartition.getConsumerVertexGroups()) {
                for (ExecutionVertexID executionVertexId : consumerVertexGroup) {
                    originalConsumerIds.add(executionVertexId);
                }
            }
            List<ConsumerVertexGroup> adaptedConsumers = adaptedPartition.getConsumerVertexGroups();
            assertThat(adaptedConsumers).isNotEmpty();
            for (ExecutionVertexID originalId : originalConsumerIds) {
                // it is sufficient to verify that some vertex exists with the correct ID here,
                // since deep equality is verified later in the main loop
                // this DOES rely on an implicit assumption that the vertices objects returned by
                // the topology are
                // identical to those stored in the partition
                assertThat(adaptedConsumers.stream().flatMap(IterableUtils::toStream))
                        .contains(originalId);
            }
        }
    }

    private static void assertPartitionEquals(
            IntermediateResultPartition originalPartition,
            DefaultResultPartition adaptedPartition) {

        assertThat(adaptedPartition.getId()).isEqualTo(originalPartition.getPartitionId());
        assertThat(adaptedPartition.getResultId())
                .isEqualTo(originalPartition.getIntermediateResult().getId());
        assertThat(adaptedPartition.getResultType()).isEqualTo(originalPartition.getResultType());
        assertThat(adaptedPartition.getProducer().getId())
                .isEqualTo(originalPartition.getProducer().getID());
    }
}
