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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SsgNetworkMemoryCalculationUtils}. */
public class SsgNetworkMemoryCalculationUtilsTest {

    private static final TestShuffleMaster SHUFFLE_MASTER = new TestShuffleMaster();

    private static final ResourceProfile DEFAULT_RESOURCE = ResourceProfile.fromResources(1.0, 100);

    private JobGraph jobGraph;

    private ExecutionGraph executionGraph;

    private List<SlotSharingGroup> slotSharingGroups;

    @Test
    public void testGenerateEnrichedResourceProfile() throws Exception {
        setup(DEFAULT_RESOURCE);

        slotSharingGroups.forEach(
                ssg ->
                        SsgNetworkMemoryCalculationUtils.enrichNetworkMemory(
                                ssg, executionGraph.getAllVertices()::get, SHUFFLE_MASTER));

        assertEquals(
                new MemorySize(
                        TestShuffleMaster.computeRequiredShuffleMemoryBytes(0, 2)
                                + TestShuffleMaster.computeRequiredShuffleMemoryBytes(1, 6)),
                slotSharingGroups.get(0).getResourceProfile().getNetworkMemory());

        assertEquals(
                new MemorySize(TestShuffleMaster.computeRequiredShuffleMemoryBytes(5, 0)),
                slotSharingGroups.get(1).getResourceProfile().getNetworkMemory());
    }

    @Test
    public void testGenerateUnknownResourceProfile() throws Exception {
        setup(ResourceProfile.UNKNOWN);

        slotSharingGroups.forEach(
                ssg ->
                        SsgNetworkMemoryCalculationUtils.enrichNetworkMemory(
                                ssg, executionGraph.getAllVertices()::get, SHUFFLE_MASTER));

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            assertEquals(ResourceProfile.UNKNOWN, slotSharingGroup.getResourceProfile());
        }
    }

    private void setup(final ResourceProfile resourceProfile) throws Exception {
        slotSharingGroups = Arrays.asList(new SlotSharingGroup(), new SlotSharingGroup());

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            slotSharingGroup.setResourceProfile(resourceProfile);
        }

        jobGraph = createJobGraph(slotSharingGroups);
        executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
    }

    private static JobGraph createJobGraph(final List<SlotSharingGroup> slotSharingGroups) {

        JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);
        source.setParallelism(4);
        source.setSlotSharingGroup(slotSharingGroups.get(0));

        JobVertex map = new JobVertex("map");
        map.setInvokableClass(NoOpInvokable.class);
        map.setParallelism(5);
        map.setSlotSharingGroup(slotSharingGroups.get(0));

        JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);
        sink.setParallelism(6);
        sink.setSlotSharingGroup(slotSharingGroups.get(1));

        map.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        sink.connectNewDataSetAsInput(
                map, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(source, map, sink);
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
