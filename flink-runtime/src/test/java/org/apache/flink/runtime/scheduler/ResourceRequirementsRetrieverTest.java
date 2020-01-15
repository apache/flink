/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.scheduler.ResourceRequirementsRetrieverTest.TestShuffleMaster.computeRequiredShuffleMemoryBytes;
import static org.junit.Assert.assertEquals;

/**
 * Unit tests for {@link ResourceRequirementsRetriever}.
 */
public class ResourceRequirementsRetrieverTest extends TestLogger {

	private static final List<Integer> PARALLELISMS = Arrays.asList(4, 5, 6);

	private static final List<DistributionPattern> DISTRIBUTION_PATTERNS = Arrays.asList(
		DistributionPattern.POINTWISE,
		DistributionPattern.ALL_TO_ALL);

	private static final List<Integer> EXPECTED_SHUFFLE_MEMORY = computeExpectedShuffleMemoryBytes();

	private static final ResourceSpec DEFAULT_RESOURCES = ResourceSpec.newBuilder(1.0, 100).build();

	private static final TestShuffleMaster SHUFFLE_MASTER = new TestShuffleMaster();

	private JobGraph jobGraph;

	private ExecutionGraph executionGraph;

	private List<SlotSharingGroup> slotSharingGroups;

	private ResourceRequirementsRetriever resourceRequirementsRetriever;

	private static List<Integer> computeExpectedShuffleMemoryBytes() {
		// build expected shuffle memory for each vertex
		// the parallelisms of vertices are 4/5/6 respectively
		// the shuffle edge is POINTWISE from source to map, and ALL_TO_ALL from map to sink
		return Arrays.asList(
			// source has 0 input channels, 2 (= ceiling(5/4)) result subpartitions
			computeRequiredShuffleMemoryBytes(0, 2),

			// sink has 1 (= ceiling(4/5)) input channels, 6 (sink parallelism) result subpartitions
			computeRequiredShuffleMemoryBytes(1, 6),

			// sink has 5 (map parallelism) input channels, 0 result subpartitions
			computeRequiredShuffleMemoryBytes(5, 0));
	}

	@Test
	public void testJobVertexResourceRequirements() throws Exception {
		setUp(DEFAULT_RESOURCES);

		final List<JobVertex> jobVertices = jobGraph.getVerticesSortedTopologicallyFromSources();
		final ExecutionJobVertex source = executionGraph.getJobVertex(jobVertices.get(0).getID());
		final ExecutionJobVertex map = executionGraph.getJobVertex(jobVertices.get(1).getID());
		final ExecutionJobVertex sink = executionGraph.getJobVertex(jobVertices.get(2).getID());

		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES, new MemorySize(EXPECTED_SHUFFLE_MEMORY.get(0))),
			resourceRequirementsRetriever.getJobVertexResourceRequirement(source.getJobVertexId()));

		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES, new MemorySize(EXPECTED_SHUFFLE_MEMORY.get(1))),
			resourceRequirementsRetriever.getJobVertexResourceRequirement(map.getJobVertexId()));

		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES, new MemorySize(EXPECTED_SHUFFLE_MEMORY.get(2))),
			resourceRequirementsRetriever.getJobVertexResourceRequirement(sink.getJobVertexId()));
	}

	@Test
	public void testGroupResourceRequirements() throws Exception {
		setUp(DEFAULT_RESOURCES);

		final SlotSharingGroupId groupId1 = slotSharingGroups.get(0).getSlotSharingGroupId();
		final SlotSharingGroupId groupId2 = slotSharingGroups.get(1).getSlotSharingGroupId();

		assertEquals(
			ResourceProfile.fromResourceSpec(
				DEFAULT_RESOURCES.merge(DEFAULT_RESOURCES),
				new MemorySize(EXPECTED_SHUFFLE_MEMORY.get(0) + EXPECTED_SHUFFLE_MEMORY.get(1))),
			resourceRequirementsRetriever.getSlotSharingGroupResourceRequirement(groupId1));

		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES, new MemorySize(EXPECTED_SHUFFLE_MEMORY.get(2))),
			resourceRequirementsRetriever.getSlotSharingGroupResourceRequirement(groupId2));
	}

	@Test
	public void testJobVertexResourceRequirementWithUnknownResources() throws Exception {
		setUp(ResourceSpec.UNKNOWN);

		for (JobVertex jobVertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			assertEquals(
				ResourceProfile.UNKNOWN,
				resourceRequirementsRetriever.getJobVertexResourceRequirement(jobVertex.getID()));
		}
	}

	@Test
	public void testGroupResourceRequirementWithUnknownResources() throws Exception {
		setUp(ResourceSpec.UNKNOWN);

		for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
			assertEquals(
				ResourceProfile.UNKNOWN,
				resourceRequirementsRetriever.getSlotSharingGroupResourceRequirement(slotSharingGroup.getSlotSharingGroupId()));
		}
	}

	private void setUp(final ResourceSpec resources) throws Exception {
		slotSharingGroups = Arrays.asList(new SlotSharingGroup(), new SlotSharingGroup());

		jobGraph = createJobGraph(slotSharingGroups, resources);

		executionGraph = TestingExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();

		resourceRequirementsRetriever = new ResourceRequirementsRetriever(executionGraph.getAllVertices(), SHUFFLE_MASTER);
	}

	/**
	 * Create a job graph for testing.
	 * The parallelism is 4/5/6 for source/map/sink.
	 * source and map are in the same slot sharing group. sink is in an individual group.
	 * map is POINTWISE connected to source. sink is ALL_TO_ALL connected to map.
	 */
	private static JobGraph createJobGraph(final List<SlotSharingGroup> slotSharingGroups, final ResourceSpec resources) {
		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(PARALLELISMS.get(0));
		source.setSlotSharingGroup(slotSharingGroups.get(0));
		source.setResources(resources, resources);

		final JobVertex map = new JobVertex("map");
		map.setInvokableClass(NoOpInvokable.class);
		map.setParallelism(PARALLELISMS.get(1));
		map.setSlotSharingGroup(slotSharingGroups.get(0));
		map.setResources(resources, resources);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(PARALLELISMS.get(2));
		sink.setSlotSharingGroup(slotSharingGroups.get(1));
		sink.setResources(resources, resources);

		map.connectNewDataSetAsInput(source, DISTRIBUTION_PATTERNS.get(0), ResultPartitionType.PIPELINED);
		sink.connectNewDataSetAsInput(map, DISTRIBUTION_PATTERNS.get(1), ResultPartitionType.PIPELINED);

		return new JobGraph(source, map, sink);
	}

	/**
	 * Test implementation of {@link ShuffleMaster}.
	 * The shuffle memory size for a task is computed via
	 * {@link TestShuffleMaster#computeRequiredShuffleMemoryBytes(int numTotalChannels, int numTotalSubpartitions)}.
	 */
	static class TestShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

		@Override
		public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
				final PartitionDescriptor partitionDescriptor,
				final ProducerDescriptor producerDescriptor) {

			return null;
		}

		@Override
		public void releasePartitionExternally(final ShuffleDescriptor shuffleDescriptor) {
		}

		@Override
		public MemorySize getShuffleMemoryForTask(final TaskInputsOutputsDescriptor descriptor) {
			final int numTotalChannels = descriptor.getNumbersOfInputGateChannels().values().stream()
				.mapToInt(Integer::intValue)
				.sum();
			final int numTotalSubpartitions = descriptor.getNumbersOfResultSubpartitions().values().stream()
				.mapToInt(Integer::intValue)
				.sum();

			return new MemorySize(computeRequiredShuffleMemoryBytes(numTotalChannels, numTotalSubpartitions));
		}

		static int computeRequiredShuffleMemoryBytes(final int numTotalChannels, final int numTotalSubpartitions) {
			return numTotalChannels * 10000 + numTotalSubpartitions;
		}
	}
}
