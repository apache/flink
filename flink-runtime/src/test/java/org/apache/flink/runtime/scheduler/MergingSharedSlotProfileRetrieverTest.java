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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link org.apache.flink.runtime.scheduler.MergingSharedSlotProfileRetrieverFactory}.
 */
public class MergingSharedSlotProfileRetrieverTest {

	private static final PreferredLocationsRetriever EMPTY_PREFERRED_LOCATIONS_RETRIEVER =
		(executionVertexId, producersToIgnore) -> CompletableFuture.completedFuture(Collections.emptyList());

	@Test
	public void testGetEmptySlotProfile() throws ExecutionException, InterruptedException {
		SharedSlotProfileRetriever sharedSlotProfileRetriever = new MergingSharedSlotProfileRetrieverFactory(
			EMPTY_PREFERRED_LOCATIONS_RETRIEVER,
			executionVertexID -> new AllocationID()
		).createFromBulk(Collections.emptySet());

		SlotProfile slotProfile = sharedSlotProfileRetriever
			.getSlotProfileFuture(new ExecutionSlotSharingGroup(), ResourceProfile.ZERO)
			.get();

		assertThat(slotProfile.getTaskResourceProfile(), is(ResourceProfile.ZERO));
		assertThat(slotProfile.getPhysicalSlotResourceProfile(), is(ResourceProfile.ZERO));
		assertThat(slotProfile.getPreferredLocations(), hasSize(0));
		assertThat(slotProfile.getPreferredAllocations(), hasSize(0));
		assertThat(slotProfile.getPreviousExecutionGraphAllocations(), hasSize(0));
	}

	@Test
	public void testResourceProfileOfSlotProfile() throws ExecutionException, InterruptedException {
		ResourceProfile resourceProfile = ResourceProfile
			.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemory(MemorySize.ofMebiBytes(1))
			.build();
		SlotProfile slotProfile = getSlotProfile(
			resourceProfile,
			Collections.nCopies(3, new AllocationID()),
			2);

		assertThat(slotProfile.getTaskResourceProfile(), is(resourceProfile));
		assertThat(slotProfile.getPhysicalSlotResourceProfile(), is(resourceProfile));
	}

	@Test
	public void testPreferredLocationsOfSlotProfile() throws ExecutionException, InterruptedException {
		// preferred locations
		List<ExecutionVertexID> executions = IntStream
			.range(0, 3)
			.mapToObj(i -> new ExecutionVertexID(new JobVertexID(), 0))
			.collect(Collectors.toList());

		List<TaskManagerLocation> allLocations = executions.stream().map(e -> createTaskManagerLocation()).collect(Collectors.toList());
		Map<ExecutionVertexID, Collection<TaskManagerLocation>> locations = new HashMap<>();
		locations.put(executions.get(0), Arrays.asList(allLocations.get(0), allLocations.get(1)));
		locations.put(executions.get(1), Arrays.asList(allLocations.get(1), allLocations.get(2)));

		SlotProfile slotProfile = getSlotProfile(
			(executionVertexId, producersToIgnore) -> {
				assertThat(producersToIgnore, containsInAnyOrder(executions.toArray()));
				return CompletableFuture.completedFuture(locations.get(executionVertexId));
			},
			executions,
			ResourceProfile.ZERO,
			Collections.nCopies(3, new AllocationID()),
			2);

		assertThat(slotProfile.getPreferredLocations().stream().filter(allLocations.get(0)::equals).count(), is(1L));
		assertThat(slotProfile.getPreferredLocations().stream().filter(allLocations.get(1)::equals).count(), is(2L));
		assertThat(slotProfile.getPreferredLocations().stream().filter(allLocations.get(2)::equals).count(), is(1L));
	}

	@Test
	public void testAllocationIdsOfSlotProfile() throws ExecutionException, InterruptedException {
		AllocationID prevAllocationID1 = new AllocationID();
		AllocationID prevAllocationID2 = new AllocationID();
		List<AllocationID> prevAllocationIDs = Arrays.asList(prevAllocationID1, prevAllocationID2, new AllocationID());

		SlotProfile slotProfile = getSlotProfile(ResourceProfile.ZERO, prevAllocationIDs, 2);

		assertThat(slotProfile.getPreferredAllocations(), containsInAnyOrder(prevAllocationID1, prevAllocationID2));
		assertThat(slotProfile.getPreviousExecutionGraphAllocations(), containsInAnyOrder(prevAllocationIDs.toArray()));
	}

	@Test
	public void testIgnoringExecutionsOutOfScheduledBulk() throws ExecutionException, InterruptedException {
		ExecutionVertexID executionVertexId1 = createRandomExecutionVertexId();
		AllocationID allocationId1 = new AllocationID();
		TaskManagerLocation location1 = createTaskManagerLocation();

		ExecutionVertexID executionVertexId2 = createRandomExecutionVertexId();
		AllocationID allocationId2 = new AllocationID();
		TaskManagerLocation location2 = createTaskManagerLocation();

		Map<ExecutionVertexID, Collection<TaskManagerLocation>> preferredlocations = new HashMap<>();
		preferredlocations.put(executionVertexId1, Collections.singleton(location1));
		preferredlocations.put(executionVertexId2, Collections.singleton(location2));

		Map<ExecutionVertexID, AllocationID> priorAllocationIds = new HashMap<>();
		priorAllocationIds.put(executionVertexId1, allocationId1);
		priorAllocationIds.put(executionVertexId2, allocationId2);

		ExecutionSlotSharingGroup group = new ExecutionSlotSharingGroup();
		group.addVertex(executionVertexId1);
		group.addVertex(executionVertexId2);

		SharedSlotProfileRetriever sharedSlotProfileRetriever = new MergingSharedSlotProfileRetrieverFactory(
			(id, ignored) -> CompletableFuture.completedFuture(preferredlocations.get(id)),
			priorAllocationIds::get
		).createFromBulk(Collections.singleton(executionVertexId1));

		SlotProfile slotProfile = sharedSlotProfileRetriever
			.getSlotProfileFuture(group, ResourceProfile.ZERO)
			.get();

		assertThat(slotProfile.getPreferredLocations(), contains(location1));
		assertThat(slotProfile.getPreferredAllocations(), contains(allocationId1));
	}

	private static SlotProfile getSlotProfile(
			ResourceProfile resourceProfile,
			List<AllocationID> prevAllocationIDs,
			int executionSlotSharingGroupSize) throws ExecutionException, InterruptedException {
		List<ExecutionVertexID> executions = prevAllocationIDs.stream()
			.map(stub -> new ExecutionVertexID(new JobVertexID(), 0))
			.collect(Collectors.toList());
		return getSlotProfile(
			EMPTY_PREFERRED_LOCATIONS_RETRIEVER,
			executions,
			resourceProfile,
			prevAllocationIDs,
			executionSlotSharingGroupSize);
	}

	private static SlotProfile getSlotProfile(
			PreferredLocationsRetriever preferredLocationsRetriever,
			List<ExecutionVertexID> executions,
			ResourceProfile resourceProfile,
			List<AllocationID> prevAllocationIDs,
			int executionSlotSharingGroupSize) throws ExecutionException, InterruptedException {
		SharedSlotProfileRetriever sharedSlotProfileRetriever = new MergingSharedSlotProfileRetrieverFactory(
			preferredLocationsRetriever,
			executionVertexID -> prevAllocationIDs.get(executions.indexOf(executionVertexID))
		).createFromBulk(new HashSet<>(executions));

		ExecutionSlotSharingGroup executionSlotSharingGroup = new ExecutionSlotSharingGroup();
		executions.stream().limit(executionSlotSharingGroupSize).forEach(executionSlotSharingGroup::addVertex);
		return sharedSlotProfileRetriever.getSlotProfileFuture(executionSlotSharingGroup, resourceProfile).get();
	}

	private static TaskManagerLocation createTaskManagerLocation() {
		try {
			return new TaskManagerLocation(ResourceID.generate(), InetAddress.getByAddress(new byte[] {1, 2, 3, 4}), 8888);
		} catch (UnknownHostException e) {
			throw new FlinkRuntimeException("unexpected", e);
		}
	}
}
