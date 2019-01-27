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

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TestingSlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.schedule.SchedulingTestUtils;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionGraphWithSharingSlots;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test scheduling with slot sharing.
 */
@RunWith(MockitoJUnitRunner.class)
public class ExecutionGraphSchedulingWithSlotSharingTest {
	private static final ResourceSpec[] TEST_RESOURCE_SPECS = {
		ResourceSpec
			.newBuilder()
			.setCpuCores(1)
			.setHeapMemoryInMB(100)
			.setDirectMemoryInMB(110)
			.setNativeMemoryInMB(120)
			.setStateSizeInMB(130)
			.setGPUResource(2)
			.addExtendedResource(new CommonExtendedResource("extend", 10))
			.build(),
		ResourceSpec
			.newBuilder()
			.setCpuCores(2)
			.setHeapMemoryInMB(200)
			.setDirectMemoryInMB(210)
			.setNativeMemoryInMB(220)
			.setStateSizeInMB(230)
			.setGPUResource(4)
			.addExtendedResource(new CommonExtendedResource("extend", 20))
			.build(),
		ResourceSpec
			.newBuilder()
			.setCpuCores(3)
			.setHeapMemoryInMB(300)
			.setDirectMemoryInMB(310)
			.setNativeMemoryInMB(320)
			.setStateSizeInMB(330)
			.setGPUResource(6)
			.addExtendedResource(new CommonExtendedResource("extend", 30))
			.build()
	};

	private static final Resource ZERO_MANAGED_MEMORY_RESOURCE = new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 0);

	@Captor
	private ArgumentCaptor<List<SlotProfile>> profilesCapture;

	@Test
	public void testSchedulingWithSlotSharingAndResourceMatching() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 8);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, 2);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 2);

		SlotProvider slotProvider = createSlotProvider();
		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			slotProvider,
			new int[]{5, 5, 2},
			new ResourceSpec[]{TEST_RESOURCE_SPECS[0], TEST_RESOURCE_SPECS[1], TEST_RESOURCE_SPECS[2]},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		executionGraph.scheduleForExecution();

		List<ExecutionVertex> vertices = new ArrayList<>();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			vertices.add(vertex);
		}
		Collections.shuffle(vertices);

		ResourceProfile expected = new ResourceProfile(
			TEST_RESOURCE_SPECS[0].getCpuCores() + TEST_RESOURCE_SPECS[1].getCpuCores() + TEST_RESOURCE_SPECS[2].getCpuCores(),
			TEST_RESOURCE_SPECS[0].getHeapMemory() + TEST_RESOURCE_SPECS[1].getHeapMemory() + TEST_RESOURCE_SPECS[2].getHeapMemory(),
			TEST_RESOURCE_SPECS[0].getDirectMemory() + TEST_RESOURCE_SPECS[1].getDirectMemory() + TEST_RESOURCE_SPECS[2].getDirectMemory(),
			TEST_RESOURCE_SPECS[0].getNativeMemory() + TEST_RESOURCE_SPECS[1].getNativeMemory() + TEST_RESOURCE_SPECS[2].getNativeMemory(),
			3,
			new HashMap<String, Resource>() {{
				put("extend", new CommonExtendedResource("extend", 60));
				put(ResourceSpec.GPU_NAME, new CommonExtendedResource(ResourceSpec.GPU_NAME, 12));
				put(ResourceSpec.MANAGED_MEMORY_NAME, ZERO_MANAGED_MEMORY_RESOURCE);
			}});

		// Ensure all the ResourceProfile has been computed for all the shared groups correctly.
		for (ExecutionVertex vertex : vertices) {
			SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
			assertNotNull(sharingGroup);
			assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + sharingGroup.getResourceProfile(),
				0, expected.compareTo(sharingGroup.getResourceProfile()));
		}

		List<List<SlotProfile>> requestedProfiles = scheduleAndGetRequestedSlotProfiles(vertices, executionGraph, slotProvider);
		requestedProfiles.forEach(
			list -> list.forEach(slotProfile -> {
				assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + slotProfile.getResourceProfile(),
					0, expected.compareTo(slotProfile.getResourceProfile()));
			}));
	}

	@Test
	public void testSchedulingWithSlotSharingAndWithoutResourceMatching() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		SlotProvider slotProvider = createSlotProvider();

		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			slotProvider,
			new int[]{5, 5, 2},
			new ResourceSpec[]{ResourceSpec.DEFAULT, ResourceSpec.DEFAULT, ResourceSpec.DEFAULT},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		executionGraph.scheduleForExecution();

		List<ExecutionVertex> vertices = new ArrayList<>();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			vertices.add(vertex);
		}
		Collections.shuffle(vertices);

		ResourceProfile expected = ResourceProfile.UNKNOWN;

		// Ensure all the ResourceProfile has been computed for all the shared groups correctly.
		for (ExecutionVertex vertex : vertices) {
			SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
			assertNotNull(sharingGroup);
			assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + sharingGroup.getResourceProfile(),
				0, expected.compareTo(sharingGroup.getResourceProfile()));
		}

		List<List<SlotProfile>> requestedProfiles = scheduleAndGetRequestedSlotProfiles(vertices, executionGraph, slotProvider);
		requestedProfiles.forEach(
			list -> list.forEach(slotProfile -> {
				assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + slotProfile.getResourceProfile(),
					0, expected.compareTo(slotProfile.getResourceProfile()));
			}));
	}

	@Test
	public void testSchedulingWithoutSlotSharingAndWithResourceMatching() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setBoolean(JobManagerOptions.SLOT_ENABLE_SHARED_SLOT, false);

		SlotProvider slotProvider = createSlotProvider();

		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			slotProvider,
			new int[]{5, 5, 2},
			new ResourceSpec[]{TEST_RESOURCE_SPECS[0], TEST_RESOURCE_SPECS[1], TEST_RESOURCE_SPECS[2]},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		executionGraph.scheduleForExecution();

		List<JobVertexID> jobVertexIDS = new ArrayList<>();
		for (ExecutionJobVertex executionJobVertex : executionGraph.getVerticesTopologically()) {
			jobVertexIDS.add(executionJobVertex.getJobVertexId());
		}

		List<ExecutionVertex> vertices = new ArrayList<>();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			vertices.add(vertex);
		}
		Collections.shuffle(vertices);

		// Ensure all the ResourceProfile has been computed for all the shared groups correctly.
		for (ExecutionVertex vertex : vertices) {
			SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
			assertNotNull(sharingGroup);
			assertNull(sharingGroup.getResourceProfile());
		}

		List<List<SlotProfile>> requestedProfiles = scheduleAndGetRequestedSlotProfiles(vertices, executionGraph, slotProvider);

		int nextVertexIndex = 0;
		for (int i = 0; i < requestedProfiles.size(); ++i) {
			for (int j = 0; j < requestedProfiles.get(i).size(); ++j) {
				ExecutionVertex nextVertex = vertices.get(nextVertexIndex++);
				int index = jobVertexIDS.indexOf(nextVertex.getJobvertexId());

				ResourceProfile expected = ResourceProfile.fromResourceSpec(TEST_RESOURCE_SPECS[index]
					.merge(ResourceSpec.newBuilder().addExtendedResource(ZERO_MANAGED_MEMORY_RESOURCE).build()), 1);
				assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + requestedProfiles.get(i).get(j).getResourceProfile(),
					0, expected.compareTo(requestedProfiles.get(i).get(j).getResourceProfile()));
			}
		}
	}

	@Test
	public void testSchedulingWithoutSlotSharingAndWithoutResourceMatching() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setBoolean(JobManagerOptions.SLOT_ENABLE_SHARED_SLOT, false);

		SlotProvider slotProvider = createSlotProvider();

		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			slotProvider,
			new int[]{5, 5, 2},
			new ResourceSpec[]{ResourceSpec.DEFAULT, ResourceSpec.DEFAULT, ResourceSpec.DEFAULT},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		executionGraph.scheduleForExecution();

		List<ExecutionVertex> vertices = new ArrayList<>();
		for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
			vertices.add(vertex);
		}
		Collections.shuffle(vertices);

		// Ensure all the ResourceProfile has been computed for all the shared groups correctly.
		for (ExecutionVertex vertex : vertices) {
			SlotSharingGroup sharingGroup = vertex.getJobVertex().getSlotSharingGroup();
			assertNotNull(sharingGroup);
			assertNull(sharingGroup.getResourceProfile());
		}

		List<List<SlotProfile>> requestedProfiles = scheduleAndGetRequestedSlotProfiles(vertices, executionGraph, slotProvider);

		for (int i = 0; i < requestedProfiles.size(); ++i) {
			for (int j = 0; j < requestedProfiles.get(i).size(); ++j) {
				ResourceProfile expected = ResourceProfile.UNKNOWN;
				assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + requestedProfiles.get(i).get(j).getResourceProfile(),
					0, expected.compareTo(requestedProfiles.get(i).get(j).getResourceProfile()));
			}
		}
	}

	private List<List<SlotProfile>> scheduleAndGetRequestedSlotProfiles(
			List<ExecutionVertex> vertices,
			ExecutionGraph executionGraph,
			SlotProvider slotProvider) {
		// Scheduling 4 vertices each time.
		final int size = 4;

		for (int start = 0; start < vertices.size(); start += size) {
			int end = Math.min(start + size, vertices.size());
			List<ExecutionVertex> verticesToSchedule = vertices.subList(start, end);
			executionGraph.scheduleVertices(verticesToSchedule
				.stream()
				.map(ExecutionVertex::getExecutionVertexID)
				.collect(Collectors.toList()));
		}

		verify(slotProvider, times((int) Math.ceil(vertices.size() / (double) size))).allocateSlots(
			anyListOf(SlotRequestId.class),
			anyListOf(ScheduledUnit.class),
			anyBoolean(),
			profilesCapture.capture(),
			any(Time.class));

		List<List<SlotProfile>> requestedSlotProfiles = profilesCapture.getAllValues();
		for (int start = 0; start < vertices.size(); start += size) {
			assertEquals(Math.min(size, vertices.size() - start), requestedSlotProfiles.get(start / size).size());
		}

		return requestedSlotProfiles;
	}

	private static SlotProvider createSlotProvider() {
		SlotProvider slotProvider = mock(SlotProvider.class);
		when(slotProvider.allocateSlots(
				anyListOf(SlotRequestId.class),
			anyListOf(ScheduledUnit.class),
			anyBoolean(),
			anyListOf(SlotProfile.class),
			any(Time.class))).then(new Answer<List<CompletableFuture<LogicalSlot>>>() {
			@Override
			public List<CompletableFuture<LogicalSlot>> answer(InvocationOnMock invocationOnMock) throws Throwable {
				List<SlotRequestId> requestIds = ((List<SlotRequestId>) invocationOnMock.getArguments()[0]);
				List<ScheduledUnit> units = ((List<ScheduledUnit>) invocationOnMock.getArguments()[1]);

				List<CompletableFuture<LogicalSlot>> logicalSlots = new ArrayList<>();

				for (int i = 0; i < units.size(); ++i) {
					LogicalSlot slot = new SingleLogicalSlot(
						requestIds.get(i),
						new SimpleSlotContext(
							new AllocationID(),
							new TaskManagerLocation(
								new ResourceID("tm_0"),
								InetAddress.getLocalHost(),
								12356),
							0,
							new SimpleAckingTaskManagerGateway()),
						units.get(i).getSlotSharingGroupId(),
						null,
						Locality.UNCONSTRAINED,
						new TestingSlotOwner());

					logicalSlots.add(CompletableFuture.completedFuture(slot));
				}

				return logicalSlots;
			}
		});

		return slotProvider;
	}
}
