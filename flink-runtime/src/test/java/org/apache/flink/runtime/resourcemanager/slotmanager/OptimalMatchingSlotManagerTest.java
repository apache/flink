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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.*;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OptimalMatchingSlotManagerTest {

	private static final double DEFAULT_TESTING_CPU_CORES = 1.0;

	private static final int DEFAULT_TESTING_MEMORY = 512;

	private static final ResourceProfile DEFAULT_TESTING_PROFILE =
		new ResourceProfile(DEFAULT_TESTING_CPU_CORES, DEFAULT_TESTING_MEMORY);

	private static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE =
		new ResourceProfile(DEFAULT_TESTING_CPU_CORES * 2, DEFAULT_TESTING_MEMORY * 2);

	/**
	 * Tests that there are no matching free slots when we request, need to allocate from cluster manager master
	 */
	@Test
	public void testRequestSlotWithoutMatchingFreeSlot() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(
				jobId,
				allocationId,
				DEFAULT_TESTING_BIG_PROFILE,
				targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			// accept an incoming slot request
			final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
			final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

			final SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
			final SlotReport slotReport = new SlotReport(slotStatus);

			slotManager.registerTaskManager(
					taskExecutorConnection,
					slotReport);

			assertTrue("The slot request should be accepted", slotManager.registerSlotRequest(slotRequest));

			verify(resourceManagerActions).allocateResource(eq(DEFAULT_TESTING_BIG_PROFILE));
		}
	}

	/**
	 * Tests that there are some free slots when we request, and the request is fulfilled with the slot closet to it
	 */
	@Test
	public void testRequestSlotFulfilledWithClosestFreeSlot() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId1 = new SlotID(resourceID, 0);
		final SlotID slotId2 = new SlotID(resourceID, 1);
		final SlotID slotId3 = new SlotID(resourceID, 2);
		final String targetAddress = "localhost";
		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(
				jobId,
				allocationId,
				DEFAULT_TESTING_PROFILE,
				targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			// accept an incoming slot request
			final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
			when(taskExecutorGateway.requestSlot(
					eq(slotId2),
					eq(jobId),
					eq(allocationId),
					anyString(),
					eq(leaderId),
					any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

			final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

			final SlotStatus slotStatus1 = new SlotStatus(slotId1, DEFAULT_TESTING_BIG_PROFILE);
			final SlotStatus slotStatus2 = new SlotStatus(slotId2, DEFAULT_TESTING_PROFILE);
			final SlotStatus slotStatus3 = new SlotStatus(slotId3, ResourceProfile.UNIVERSAL);
			final List<SlotStatus> slotStatusList = new ArrayList<>(3);
			slotStatusList.add(slotStatus1);
			slotStatusList.add(slotStatus2);
			slotStatusList.add(slotStatus3);
			final SlotReport slotReport = new SlotReport(slotStatusList);

			slotManager.registerTaskManager(
					taskExecutorConnection,
					slotReport);

			assertTrue("The slot request should be accepted", slotManager.registerSlotRequest(slotRequest));

			verify(taskExecutorGateway).requestSlot(eq(slotId2), eq(jobId), eq(allocationId), eq(targetAddress), eq(leaderId), any(Time.class));

			TaskManagerSlot slot = slotManager.getSlot(slotId2);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());
			assertTrue("The slot should be allocated.", slot.isAllocated());
		}
	}

	/**
	 * Tests that a new slot appeared in SlotReport, and we used it to fulfill a closet pending request
	 */
	@Test
	public void testNewlyAppearedFreeSlotFulfillClosestPendingRequest() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID1 = ResourceID.generate();
		final ResourceID resourceID2 = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId1 = new SlotID(resourceID1, 0);
		final SlotID slotId2 = new SlotID(resourceID2, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();
		final AllocationID allocationId3 = new AllocationID();
		final SlotRequest slotRequest1 = new SlotRequest(
				jobId,
				allocationId1,
				DEFAULT_TESTING_BIG_PROFILE,
				targetAddress);
		final SlotRequest slotRequest2 = new SlotRequest(
				jobId,
				allocationId2,
				DEFAULT_TESTING_PROFILE,
				targetAddress);
		final SlotRequest slotRequest3 = new SlotRequest(
				jobId,
				allocationId3,
				ResourceProfile.UNIVERSAL,
				targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		// accept an incoming slot request
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
				any(SlotID.class),
				eq(jobId),
				any(AllocationID.class),
				anyString(),
				eq(leaderId),
				any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskExecutorConnection1 = new TaskExecutorConnection(taskExecutorGateway);
		final TaskExecutorConnection taskExecutorConnection2 = new TaskExecutorConnection(taskExecutorGateway);

		final SlotStatus slotStatus1 = new SlotStatus(slotId1, DEFAULT_TESTING_BIG_PROFILE);
		final SlotReport slotReport1 = new SlotReport(slotStatus1);

		final SlotStatus slotStatus2 = new SlotStatus(slotId2, DEFAULT_TESTING_BIG_PROFILE);
		final SlotReport slotReport2 = new SlotReport(slotStatus2);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			assertTrue("The slot request1 should be accepted", slotManager.registerSlotRequest(slotRequest1));
			assertTrue("The slot request2 should be accepted", slotManager.registerSlotRequest(slotRequest2));

			verify(resourceManagerActions, times(2)).allocateResource(any(ResourceProfile.class));

			slotManager.registerTaskManager(
					taskExecutorConnection1,
					slotReport1);

			verify(taskExecutorGateway).requestSlot(eq(slotId1), eq(jobId), eq(allocationId1), eq(targetAddress), eq(leaderId), any(Time.class));

			TaskManagerSlot slot1 = slotManager.getSlot(slotId1);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId1, slot1.getAllocationId());

			assertTrue("The slot request3 should be accepted", slotManager.registerSlotRequest(slotRequest3));
			verify(resourceManagerActions, times(3)).allocateResource(any(ResourceProfile.class));

			slotManager.registerTaskManager(
					taskExecutorConnection2,
					slotReport2);

			verify(taskExecutorGateway).requestSlot(eq(slotId2), eq(jobId), eq(allocationId3), eq(targetAddress), eq(leaderId), any(Time.class));

			TaskManagerSlot slot2 = slotManager.getSlot(slotId2);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId3, slot2.getAllocationId());
		}
	}

	/**
	 * Tests that a new slot appeared in SlotReport, but it's not suitable for all the pending requests
	 */
	@Test
	public void testNewlyAppearedFreeSlotNotMatchPendingRequests() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = new AllocationID();
		final SlotRequest slotRequest = new SlotRequest(
				jobId,
				allocationId,
				DEFAULT_TESTING_BIG_PROFILE,
				targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		// accept an incoming slot request
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			slotManager.registerTaskManager(
					taskExecutorConnection,
					slotReport);

			assertTrue("The slot request should be accepted", slotManager.registerSlotRequest(slotRequest));

			verify(resourceManagerActions, times(1)).allocateResource(eq(DEFAULT_TESTING_BIG_PROFILE));

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertTrue("The slot has not been allocated to the expected allocation id.", slot.isFree());
		}
	}

	private OptimalMatchingSlotManager createSlotManager(UUID leaderId, ResourceManagerActions resourceManagerActions) {
		OptimalMatchingSlotManager slotManager = new OptimalMatchingSlotManager(
				TestingUtils.defaultScheduledExecutor(),
				TestingUtils.infiniteTime(),
				TestingUtils.infiniteTime(),
				TestingUtils.infiniteTime());

		slotManager.start(leaderId, Executors.directExecutor(), resourceManagerActions);

		return slotManager;
	}
}
