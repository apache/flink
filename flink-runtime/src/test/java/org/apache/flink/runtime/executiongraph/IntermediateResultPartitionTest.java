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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link IntermediateResultPartition}. */
public class IntermediateResultPartitionTest {
    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testPartitionDataAllProduced() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not all data produced on init
        assertThat(partition1.hasDataAllProduced()).isFalse();
        assertThat(partition2.hasDataAllProduced()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Finished partition has produced all data
        partition1.markFinished();
        assertThat(partition1.hasDataAllProduced()).isTrue();
        assertThat(partition2.hasDataAllProduced()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Finished partition has produced all data
        partition2.markFinished();
        assertThat(partition1.hasDataAllProduced()).isTrue();
        assertThat(partition2.hasDataAllProduced()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isTrue();

        // Not all data produced if failover happens
        result.resetForNewExecution();
        assertThat(partition1.hasDataAllProduced()).isFalse();
        assertThat(partition2.hasDataAllProduced()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();
    }

    @Test
    void testBlockingPartitionResetting() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not all data data produced on init
        assertThat(partition1.hasDataAllProduced()).isFalse();
        assertThat(partition2.hasDataAllProduced()).isFalse();

        // Finished partition has produced all data
        partition1.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isOne();
        assertThat(partition1.hasDataAllProduced()).isTrue();
        assertThat(partition2.hasDataAllProduced()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // Reset the result and mark partition2 FINISHED, partition1's hasDataAllProduced should be
        // false, partition2's hasDataAllProduced should be true
        result.resetForNewExecution();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        partition2.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isOne();
        assertThat(partition1.hasDataAllProduced()).isFalse();
        assertThat(partition2.hasDataAllProduced()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();

        // All partition finished.
        partition1.markFinished();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isZero();
        assertThat(partition1.hasDataAllProduced()).isTrue();
        assertThat(partition2.hasDataAllProduced()).isTrue();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isTrue();

        // Not all data produced and not all partition finished again if failover happens
        result.resetForNewExecution();
        assertThat(consumedPartitionGroup.getNumberOfUnfinishedPartitions()).isEqualTo(2);
        assertThat(partition1.hasDataAllProduced()).isFalse();
        assertThat(partition2.hasDataAllProduced()).isFalse();
        assertThat(consumedPartitionGroup.areAllPartitionsFinished()).isFalse();
    }

    @Test
    void testReleasePartitionGroups() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);

        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];
        assertThat(partition1.canBeReleased()).isFalse();
        assertThat(partition2.canBeReleased()).isFalse();

        List<ConsumedPartitionGroup> consumedPartitionGroup1 =
                partition1.getConsumedPartitionGroups();
        List<ConsumedPartitionGroup> consumedPartitionGroup2 =
                partition2.getConsumedPartitionGroups();
        assertThat(consumedPartitionGroup1).isEqualTo(consumedPartitionGroup2);

        assertThat(consumedPartitionGroup1).hasSize(2);
        partition1.markPartitionGroupReleasable(consumedPartitionGroup1.get(0));
        assertThat(partition1.canBeReleased()).isFalse();

        partition1.markPartitionGroupReleasable(consumedPartitionGroup1.get(1));
        assertThat(partition1.canBeReleased()).isTrue();

        result.resetForNewExecution();
        assertThat(partition1.canBeReleased()).isFalse();
    }

    @Test
    void testGetNumberOfSubpartitionsForNonDynamicAllToAllGraph() throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.ALL_TO_ALL, false, Arrays.asList(7, 7));
    }

    @Test
    void testGetNumberOfSubpartitionsForNonDynamicPointwiseGraph() throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.POINTWISE, false, Arrays.asList(4, 3));
    }

    @Test
    void testGetNumberOfSubpartitionsFromConsumerParallelismForDynamicAllToAllGraph()
            throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.ALL_TO_ALL, true, Arrays.asList(7, 7));
    }

    @Test
    void testGetNumberOfSubpartitionsFromConsumerParallelismForDynamicPointwiseGraph()
            throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.POINTWISE, true, Arrays.asList(4, 4));
    }

    @Test
    void testGetNumberOfSubpartitionsFromConsumerMaxParallelismForDynamicAllToAllGraph()
            throws Exception {
        testGetNumberOfSubpartitions(
                -1, DistributionPattern.ALL_TO_ALL, true, Arrays.asList(13, 13));
    }

    @Test
    void testGetNumberOfSubpartitionsFromConsumerMaxParallelismForDynamicPointwiseGraph()
            throws Exception {
        testGetNumberOfSubpartitions(-1, DistributionPattern.POINTWISE, true, Arrays.asList(7, 7));
    }

    private void testGetNumberOfSubpartitions(
            int consumerParallelism,
            DistributionPattern distributionPattern,
            boolean isDynamicGraph,
            List<Integer> expectedNumSubpartitions)
            throws Exception {

        final int producerParallelism = 2;
        final int consumerMaxParallelism = 13;

        final ExecutionGraph eg =
                createExecutionGraph(
                        producerParallelism,
                        consumerParallelism,
                        consumerMaxParallelism,
                        distributionPattern,
                        isDynamicGraph,
                        EXECUTOR_RESOURCE.getExecutor());

        final Iterator<ExecutionJobVertex> vertexIterator =
                eg.getVerticesTopologically().iterator();
        final ExecutionJobVertex producer = vertexIterator.next();

        if (isDynamicGraph) {
            ExecutionJobVertexTest.initializeVertex(producer);
        }

        final IntermediateResult result = producer.getProducedDataSets()[0];

        assertThat(expectedNumSubpartitions).hasSize(producerParallelism);
        assertThat(
                        Arrays.stream(result.getPartitions())
                                .map(IntermediateResultPartition::getNumberOfSubpartitions)
                                .collect(Collectors.toList()))
                .isEqualTo(expectedNumSubpartitions);
    }

    public static ExecutionGraph createExecutionGraph(
            int producerParallelism,
            int consumerParallelism,
            int consumerMaxParallelism,
            DistributionPattern distributionPattern,
            boolean isDynamicGraph,
            ScheduledExecutorService scheduledExecutorService)
            throws Exception {

        final JobVertex v1 = new JobVertex("v1");
        v1.setInvokableClass(NoOpInvokable.class);
        v1.setParallelism(producerParallelism);

        final JobVertex v2 = new JobVertex("v2");
        v2.setInvokableClass(NoOpInvokable.class);
        if (consumerParallelism > 0) {
            v2.setParallelism(consumerParallelism);
        }
        if (consumerMaxParallelism > 0) {
            v2.setMaxParallelism(consumerMaxParallelism);
        }

        final JobVertex v3 = new JobVertex("v3");
        v3.setInvokableClass(NoOpInvokable.class);
        if (consumerParallelism > 0) {
            v3.setParallelism(consumerParallelism);
        }
        if (consumerMaxParallelism > 0) {
            v3.setMaxParallelism(consumerMaxParallelism);
        }

        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        v2.connectNewDataSetAsInput(
                v1, distributionPattern, ResultPartitionType.BLOCKING, dataSetId, false);
        v3.connectNewDataSetAsInput(
                v1, distributionPattern, ResultPartitionType.BLOCKING, dataSetId, false);

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .addJobVertices(Arrays.asList(v1, v2, v3))
                        .build();

        final Configuration configuration = new Configuration();

        TestingDefaultExecutionGraphBuilder builder =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setJobMasterConfig(configuration)
                        .setVertexParallelismStore(
                                computeVertexParallelismStoreConsideringDynamicGraph(
                                        jobGraph.getVertices(),
                                        isDynamicGraph,
                                        consumerMaxParallelism));
        if (isDynamicGraph) {
            return builder.buildDynamicGraph(scheduledExecutorService);
        } else {
            return builder.build(scheduledExecutorService);
        }
    }

    public static VertexParallelismStore computeVertexParallelismStoreConsideringDynamicGraph(
            Iterable<JobVertex> vertices, boolean isDynamicGraph, int defaultMaxParallelism) {
        if (isDynamicGraph) {
            return AdaptiveBatchScheduler.computeVertexParallelismStoreForDynamicGraph(
                    vertices, defaultMaxParallelism);
        } else {
            return SchedulerBase.computeVertexParallelismStore(vertices);
        }
    }

    private static IntermediateResult createResult(
            ResultPartitionType resultPartitionType, int parallelism) throws Exception {

        JobVertex source = new JobVertex("v1");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(parallelism);

        JobVertex sink1 = new JobVertex("v2");
        sink1.setInvokableClass(NoOpInvokable.class);
        sink1.setParallelism(parallelism);

        JobVertex sink2 = new JobVertex("v3");
        sink2.setInvokableClass(NoOpInvokable.class);
        sink2.setParallelism(parallelism);

        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        sink1.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, resultPartitionType, dataSetId, false);
        sink2.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, resultPartitionType, dataSetId, false);

        ScheduledExecutorService executorService = new DirectScheduledExecutorService();

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(source, sink1, sink2);

        SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                executorService)
                        .build();

        ExecutionJobVertex ejv = scheduler.getExecutionJobVertex(source.getID());

        return ejv.getProducedDataSets()[0];
    }
}
