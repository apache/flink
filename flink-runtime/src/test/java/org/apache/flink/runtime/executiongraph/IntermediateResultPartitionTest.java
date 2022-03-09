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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link IntermediateResultPartition}. */
public class IntermediateResultPartitionTest extends TestLogger {

    @Test
    public void testPipelinedPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.PIPELINED, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Partition 1 consumable after data are produced
        partition1.markDataProduced();
        assertTrue(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
    }

    @Test
    public void testBlockingPartitionConsumable() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());

        // Not consumable if only one partition is FINISHED
        partition1.markFinished();
        assertTrue(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());

        // Consumable after all partitions are FINISHED
        partition2.markFinished();
        assertTrue(partition1.isConsumable());
        assertTrue(partition2.isConsumable());
        assertTrue(consumedPartitionGroup.areAllPartitionsFinished());

        // Not consumable if failover happens
        result.resetForNewExecution();
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());
    }

    @Test
    public void testBlockingPartitionResetting() throws Exception {
        IntermediateResult result = createResult(ResultPartitionType.BLOCKING, 2);
        IntermediateResultPartition partition1 = result.getPartitions()[0];
        IntermediateResultPartition partition2 = result.getPartitions()[1];

        ConsumedPartitionGroup consumedPartitionGroup =
                partition1.getConsumedPartitionGroups().get(0);

        // Not consumable on init
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());

        // Not consumable if partition1 is FINISHED
        partition1.markFinished();
        assertEquals(1, consumedPartitionGroup.getNumberOfUnfinishedPartitions());
        assertTrue(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());

        // Reset the result and mark partition2 FINISHED, the result should still not be consumable
        result.resetForNewExecution();
        assertEquals(2, consumedPartitionGroup.getNumberOfUnfinishedPartitions());
        partition2.markFinished();
        assertEquals(1, consumedPartitionGroup.getNumberOfUnfinishedPartitions());
        assertFalse(partition1.isConsumable());
        assertTrue(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());

        // Consumable after all partitions are FINISHED
        partition1.markFinished();
        assertEquals(0, consumedPartitionGroup.getNumberOfUnfinishedPartitions());
        assertTrue(partition1.isConsumable());
        assertTrue(partition2.isConsumable());
        assertTrue(consumedPartitionGroup.areAllPartitionsFinished());

        // Not consumable again if failover happens
        result.resetForNewExecution();
        assertEquals(2, consumedPartitionGroup.getNumberOfUnfinishedPartitions());
        assertFalse(partition1.isConsumable());
        assertFalse(partition2.isConsumable());
        assertFalse(consumedPartitionGroup.areAllPartitionsFinished());
    }

    @Test
    public void testGetNumberOfSubpartitionsForNonDynamicAllToAllGraph() throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.ALL_TO_ALL, false, Arrays.asList(7, 7));
    }

    @Test
    public void testGetNumberOfSubpartitionsForNonDynamicPointwiseGraph() throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.POINTWISE, false, Arrays.asList(4, 3));
    }

    @Test
    public void testGetNumberOfSubpartitionsFromConsumerParallelismForDynamicAllToAllGraph()
            throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.ALL_TO_ALL, true, Arrays.asList(7, 7));
    }

    @Test
    public void testGetNumberOfSubpartitionsFromConsumerParallelismForDynamicPointwiseGraph()
            throws Exception {
        testGetNumberOfSubpartitions(7, DistributionPattern.POINTWISE, true, Arrays.asList(4, 4));
    }

    @Test
    public void testGetNumberOfSubpartitionsFromConsumerMaxParallelismForDynamicAllToAllGraph()
            throws Exception {
        testGetNumberOfSubpartitions(
                -1, DistributionPattern.ALL_TO_ALL, true, Arrays.asList(13, 13));
    }

    @Test
    public void testGetNumberOfSubpartitionsFromConsumerMaxParallelismForDynamicPointwiseGraph()
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
                        isDynamicGraph);

        final Iterator<ExecutionJobVertex> vertexIterator =
                eg.getVerticesTopologically().iterator();
        final ExecutionJobVertex producer = vertexIterator.next();

        if (isDynamicGraph) {
            ExecutionJobVertexTest.initializeVertex(producer);
        }

        final IntermediateResult result = producer.getProducedDataSets()[0];

        assertThat(expectedNumSubpartitions.size(), is(producerParallelism));
        assertThat(
                Arrays.stream(result.getPartitions())
                        .map(IntermediateResultPartition::getNumberOfSubpartitions)
                        .collect(Collectors.toList()),
                equalTo(expectedNumSubpartitions));
    }

    public static ExecutionGraph createExecutionGraph(
            int producerParallelism,
            int consumerParallelism,
            int consumerMaxParallelism,
            DistributionPattern distributionPattern,
            boolean isDynamicGraph)
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

        v2.connectNewDataSetAsInput(v1, distributionPattern, ResultPartitionType.BLOCKING);

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .addJobVertices(Arrays.asList(v1, v2))
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
            return builder.buildDynamicGraph();
        } else {
            return builder.build();
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

        JobVertex sink = new JobVertex("v2");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(parallelism);

        sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, resultPartitionType);

        ScheduledExecutorService executorService = new DirectScheduledExecutorService();

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(source, sink);

        SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setIoExecutor(executorService)
                        .setFutureExecutor(executorService)
                        .build();

        ExecutionJobVertex ejv = scheduler.getExecutionJobVertex(source.getID());

        return ejv.getProducedDataSets()[0];
    }
}
