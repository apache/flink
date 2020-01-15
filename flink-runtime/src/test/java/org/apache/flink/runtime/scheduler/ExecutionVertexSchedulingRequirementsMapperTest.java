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
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.runtime.scheduler.ResourceRequirementsRetrieverTest.TestShuffleMaster.computeRequiredShuffleMemoryBytes;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ExecutionVertexSchedulingRequirementsMapper}.
 */
public class ExecutionVertexSchedulingRequirementsMapperTest extends TestLogger {

	private static final ResourceSpec DEFAULT_RESOURCES = ResourceSpec.newBuilder(1.0, 100).build();

	@Test
	public void testResourcesInCreatedRequirements() throws Exception {
		final int parallelism = 5;
		final JobGraph jobGraph = createJobGraph(parallelism);
		final ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder()
			.setJobGraph(jobGraph)
			.setShuffleMaster(new ResourceRequirementsRetrieverTest.TestShuffleMaster())
			.build();

		final ExecutionVertex sourceVertex = executionGraph.getAllExecutionVertices().iterator().next();

		final ExecutionVertexSchedulingRequirements requirements = ExecutionVertexSchedulingRequirementsMapper.from(sourceVertex);

		final MemorySize sourceVertexShuffleMemorySize = new MemorySize(computeRequiredShuffleMemoryBytes(0, parallelism));
		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES, sourceVertexShuffleMemorySize),
			requirements.getTaskResourceProfile());

		final MemorySize sinkVertexShuffleMemorySize = new MemorySize(computeRequiredShuffleMemoryBytes(parallelism, 0));
		final MemorySize groupShuffleMemorySize = sourceVertexShuffleMemorySize.add(sinkVertexShuffleMemorySize);
		assertEquals(
			ResourceProfile.fromResourceSpec(DEFAULT_RESOURCES.merge(DEFAULT_RESOURCES), groupShuffleMemorySize),
			requirements.getPhysicalSlotResourceProfile());
	}

	private static JobGraph createJobGraph(final int parallelism) {
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(slotSharingGroup);
		source.setResources(DEFAULT_RESOURCES, DEFAULT_RESOURCES);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(slotSharingGroup);
		sink.setResources(DEFAULT_RESOURCES, DEFAULT_RESOURCES);

		sink.connectNewDataSetAsInput(source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		return new JobGraph(source, sink);
	}
}
