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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartitionTest;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests for {@link SsgNetworkMemoryCalculationUtils}. */
public class SsgNetworkMemoryCalculationUtilsTest {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final TestShuffleMaster SHUFFLE_MASTER = new TestShuffleMaster();

    private static final ResourceProfile DEFAULT_RESOURCE = ResourceProfile.fromResources(1.0, 100);

    @Test
    public void testGenerateEnrichedResourceProfile() throws Exception {
        // 1. for the first source vertex, no input channel, 1 or 2 subpartitions for point-wise
        // output edge (the max is 2)
        // 2. for the second map vertex, 1 input channel for point-wise edge, 2 result partitions
        // for 2 all-to-all output edges (no partition reuse for pipelined shuffle), each with 6
        // subpartitions
        // 3. for the third sink vertex, 10 input channels for two result partitions, each with 5
        // channels, no shuffle output
        testGenerateEnrichedResourceProfile(
                ResultPartitionType.PIPELINED,
                new MemorySize(
                        TestShuffleMaster.computeRequiredShuffleMemoryBytes(0, 2)
                                + TestShuffleMaster.computeRequiredShuffleMemoryBytes(1, 12)),
                new MemorySize(TestShuffleMaster.computeRequiredShuffleMemoryBytes(10, 0)));

        // 1. for the first source vertex, no input channel, 1 or 2 subpartitions for point-wise
        // output edge (the max is 2)
        // 2. for the second map vertex, 1 input channel for point-wise edge, 1 result partitions (6
        // subpartitions) for 2 all-to-all output edges (partition reused for blocking shuffle)
        // 3. for the third sink vertex, 10 input channels for 2 all-to-all input edges consuming 1
        // result partition, each with 5 channels, no shuffle output
        testGenerateEnrichedResourceProfile(
                ResultPartitionType.BLOCKING,
                new MemorySize(
                        TestShuffleMaster.computeRequiredShuffleMemoryBytes(0, 2)
                                + TestShuffleMaster.computeRequiredShuffleMemoryBytes(1, 6)),
                new MemorySize(TestShuffleMaster.computeRequiredShuffleMemoryBytes(5, 0)));
    }

    private void testGenerateEnrichedResourceProfile(
            ResultPartitionType resultPartitionType,
            MemorySize group0MemorySize,
            MemorySize group1MemorySize)
            throws Exception {

        SlotSharingGroup slotSharingGroup0 = new SlotSharingGroup();
        slotSharingGroup0.setResourceProfile(DEFAULT_RESOURCE);

        SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        slotSharingGroup1.setResourceProfile(DEFAULT_RESOURCE);

        createExecutionGraphAndEnrichNetworkMemory(
                Arrays.asList(slotSharingGroup0, slotSharingGroup0, slotSharingGroup1),
                resultPartitionType);

        assertEquals(group0MemorySize, slotSharingGroup0.getResourceProfile().getNetworkMemory());
        assertEquals(group1MemorySize, slotSharingGroup1.getResourceProfile().getNetworkMemory());
    }

    @Test
    public void testGenerateUnknownResourceProfile() throws Exception {
        SlotSharingGroup slotSharingGroup0 = new SlotSharingGroup();
        slotSharingGroup0.setResourceProfile(ResourceProfile.UNKNOWN);

        SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        slotSharingGroup1.setResourceProfile(ResourceProfile.UNKNOWN);

        createExecutionGraphAndEnrichNetworkMemory(
                Arrays.asList(slotSharingGroup0, slotSharingGroup0, slotSharingGroup1),
                ResultPartitionType.PIPELINED);

        assertEquals(ResourceProfile.UNKNOWN, slotSharingGroup0.getResourceProfile());
        assertEquals(ResourceProfile.UNKNOWN, slotSharingGroup1.getResourceProfile());
    }

    @Test
    public void testGenerateEnrichedResourceProfileForDynamicGraph() throws Exception {
        List<SlotSharingGroup> slotSharingGroups =
                Arrays.asList(
                        new SlotSharingGroup(), new SlotSharingGroup(), new SlotSharingGroup());

        for (SlotSharingGroup group : slotSharingGroups) {
            group.setResourceProfile(DEFAULT_RESOURCE);
        }

        DefaultExecutionGraph executionGraph = createDynamicExecutionGraph(slotSharingGroups, 20);
        Iterator<ExecutionJobVertex> jobVertices =
                executionGraph.getVerticesTopologically().iterator();
        ExecutionJobVertex source = jobVertices.next();
        ExecutionJobVertex map = jobVertices.next();
        ExecutionJobVertex sink = jobVertices.next();

        executionGraph.initializeJobVertex(
                source, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        triggerComputeNumOfSubpartitions(source.getProducedDataSets()[0]);

        map.setParallelism(5);
        executionGraph.initializeJobVertex(
                map, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        triggerComputeNumOfSubpartitions(map.getProducedDataSets()[0]);

        sink.setParallelism(7);
        executionGraph.initializeJobVertex(
                sink, 0L, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        assertNetworkMemory(
                slotSharingGroups,
                Arrays.asList(
                        new MemorySize(TestShuffleMaster.computeRequiredShuffleMemoryBytes(0, 5)),
                        new MemorySize(TestShuffleMaster.computeRequiredShuffleMemoryBytes(5, 20)),
                        new MemorySize(
                                TestShuffleMaster.computeRequiredShuffleMemoryBytes(15, 0))));
    }

    private void triggerComputeNumOfSubpartitions(IntermediateResult result) {
        // call IntermediateResultPartition#getNumberOfSubpartitions to trigger computation of
        // numOfSubpartitions
        for (IntermediateResultPartition partition : result.getPartitions()) {
            partition.getNumberOfSubpartitions();
        }
    }

    private void assertNetworkMemory(
            List<SlotSharingGroup> slotSharingGroups, List<MemorySize> networkMemory) {

        assertEquals(slotSharingGroups.size(), networkMemory.size());
        for (int i = 0; i < slotSharingGroups.size(); ++i) {
            assertThat(
                    slotSharingGroups.get(i).getResourceProfile().getNetworkMemory(),
                    is(networkMemory.get(i)));
        }
    }

    @Test
    public void testGetMaxInputChannelNumForResultForAllToAll() throws Exception {
        testGetMaxInputChannelNumForResult(DistributionPattern.ALL_TO_ALL, 5, 20, 7, 15);
    }

    @Test
    public void testGetMaxInputChannelNumForResultForPointWise() throws Exception {
        testGetMaxInputChannelNumForResult(DistributionPattern.POINTWISE, 5, 20, 3, 8);
        testGetMaxInputChannelNumForResult(DistributionPattern.POINTWISE, 5, 20, 5, 4);
        testGetMaxInputChannelNumForResult(DistributionPattern.POINTWISE, 5, 20, 7, 4);
    }

    private void testGetMaxInputChannelNumForResult(
            DistributionPattern distributionPattern,
            int producerParallelism,
            int consumerMaxParallelism,
            int decidedConsumerParallelism,
            int expectedNumChannels)
            throws Exception {

        final DefaultExecutionGraph eg =
                (DefaultExecutionGraph)
                        IntermediateResultPartitionTest.createExecutionGraph(
                                producerParallelism,
                                -1,
                                consumerMaxParallelism,
                                distributionPattern,
                                true,
                                EXECUTOR_RESOURCE.getExecutor());

        final Iterator<ExecutionJobVertex> vertexIterator =
                eg.getVerticesTopologically().iterator();
        final ExecutionJobVertex producer = vertexIterator.next();
        final ExecutionJobVertex consumer = vertexIterator.next();

        eg.initializeJobVertex(
                producer,
                0L,
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        final IntermediateResult result = producer.getProducedDataSets()[0];
        triggerComputeNumOfSubpartitions(result);

        consumer.setParallelism(decidedConsumerParallelism);
        eg.initializeJobVertex(
                consumer,
                0L,
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        Map<IntermediateDataSetID, Integer> maxInputChannelNums = new HashMap<>();
        Map<IntermediateDataSetID, ResultPartitionType> inputPartitionTypes = new HashMap<>();
        SsgNetworkMemoryCalculationUtils.getMaxInputChannelInfoForDynamicGraph(
                consumer, maxInputChannelNums, inputPartitionTypes);

        assertThat(maxInputChannelNums.size(), is(1));
        assertThat(maxInputChannelNums.get(result.getId()), is(expectedNumChannels));
        assertThat(inputPartitionTypes.size(), is(1));
        assertThat(inputPartitionTypes.get(result.getId()), is(result.getResultType()));
    }

    private DefaultExecutionGraph createDynamicExecutionGraph(
            final List<SlotSharingGroup> slotSharingGroups, int defaultMaxParallelism)
            throws Exception {

        JobGraph jobGraph =
                createJobGraph(
                        slotSharingGroups, Arrays.asList(4, -1, -1), ResultPartitionType.BLOCKING);

        final VertexParallelismStore vertexParallelismStore =
                AdaptiveBatchScheduler.computeVertexParallelismStoreForDynamicGraph(
                        jobGraph.getVertices(), defaultMaxParallelism);

        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(jobGraph)
                .setVertexParallelismStore(vertexParallelismStore)
                .setShuffleMaster(SHUFFLE_MASTER)
                .buildDynamicGraph(EXECUTOR_RESOURCE.getExecutor());
    }

    private void createExecutionGraphAndEnrichNetworkMemory(
            final List<SlotSharingGroup> slotSharingGroups, ResultPartitionType resultPartitionType)
            throws Exception {
        TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(
                        createJobGraph(
                                slotSharingGroups, Arrays.asList(4, 5, 6), resultPartitionType))
                .setShuffleMaster(SHUFFLE_MASTER)
                .build(EXECUTOR_RESOURCE.getExecutor());
    }

    private static JobGraph createJobGraph(
            final List<SlotSharingGroup> slotSharingGroups,
            List<Integer> parallelisms,
            ResultPartitionType resultPartitionType) {

        assertThat(slotSharingGroups.size(), is(3));
        assertThat(parallelisms.size(), is(3));

        JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        trySetParallelism(source, parallelisms.get(0));
        source.setSlotSharingGroup(slotSharingGroups.get(0));

        JobVertex map = new JobVertex("map");
        map.setInvokableClass(NoOpInvokable.class);
        trySetParallelism(map, parallelisms.get(1));
        map.setSlotSharingGroup(slotSharingGroups.get(1));

        JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        trySetParallelism(sink, parallelisms.get(2));
        sink.setSlotSharingGroup(slotSharingGroups.get(2));

        map.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, resultPartitionType);
        if (resultPartitionType == ResultPartitionType.BLOCKING) {
            IntermediateDataSetID dataSetId = new IntermediateDataSetID();
            sink.connectNewDataSetAsInput(
                    map, DistributionPattern.ALL_TO_ALL, resultPartitionType, dataSetId, false);
            sink.connectNewDataSetAsInput(
                    map, DistributionPattern.ALL_TO_ALL, resultPartitionType, dataSetId, false);
        } else {
            sink.connectNewDataSetAsInput(map, DistributionPattern.ALL_TO_ALL, resultPartitionType);
            sink.connectNewDataSetAsInput(map, DistributionPattern.ALL_TO_ALL, resultPartitionType);
        }

        if (!resultPartitionType.isBlockingOrBlockingPersistentResultPartition()) {
            return JobGraphTestUtils.streamingJobGraph(source, map, sink);

        } else {
            return JobGraphTestUtils.batchJobGraph(source, map, sink);
        }
    }

    private static void trySetParallelism(JobVertex jobVertex, int parallelism) {
        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        }
    }

    private static class TestShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {
        @Override
        public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
                JobID jobID,
                PartitionDescriptor partitionDescriptor,
                ProducerDescriptor producerDescriptor) {
            return null;
        }

        @Override
        public void releasePartitionExternally(final ShuffleDescriptor shuffleDescriptor) {}

        @Override
        public MemorySize computeShuffleMemorySizeForTask(final TaskInputsOutputsDescriptor desc) {
            int numTotalChannels =
                    desc.getInputChannelNums().values().stream().mapToInt(Integer::intValue).sum();
            int numTotalSubpartitions =
                    desc.getSubpartitionNums().values().stream().mapToInt(Integer::intValue).sum();
            return new MemorySize(
                    computeRequiredShuffleMemoryBytes(numTotalChannels, numTotalSubpartitions));
        }

        static int computeRequiredShuffleMemoryBytes(
                final int numTotalChannels, final int numTotalSubpartitions) {
            return numTotalChannels * 10000 + numTotalSubpartitions;
        }
    }
}
