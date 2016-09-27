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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServices;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.SlotRequestReply;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class SlotManagerTest {

	private static final double DEFAULT_TESTING_CPU_CORES = 1.0;

	private static final long DEFAULT_TESTING_MEMORY = 512;

	private static final ResourceProfile DEFAULT_TESTING_PROFILE =
		new ResourceProfile(DEFAULT_TESTING_CPU_CORES, DEFAULT_TESTING_MEMORY);

	private static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE =
		new ResourceProfile(DEFAULT_TESTING_CPU_CORES * 2, DEFAULT_TESTING_MEMORY * 2);

	private static TaskExecutorGateway taskExecutorGateway;

	@BeforeClass
	public static void setUp() {
		taskExecutorGateway = Mockito.mock(TaskExecutorGateway.class);
		Mockito.when(taskExecutorGateway.requestSlot(any(AllocationID.class), any(UUID.class), any(Time.class)))
			.thenReturn(new FlinkCompletableFuture<SlotRequestReply>());
	}

	/**
	 * Tests that there are no free slots when we request, need to allocate from cluster manager master
	 */
	@Test
	public void testRequestSlotWithoutFreeSlot() {
		TestingSlotManager slotManager = new TestingSlotManager();
		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));

		assertEquals(0, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertEquals(1, slotManager.getAllocatedContainers().size());
		assertEquals(DEFAULT_TESTING_PROFILE, slotManager.getAllocatedContainers().get(0));
	}

	/**
	 * Tests that there are some free slots when we request, and the request is fulfilled immediately
	 */
	@Test
	public void testRequestSlotWithFreeSlot() {
		TestingSlotManager slotManager = new TestingSlotManager();

		directlyProvideFreeSlots(slotManager, DEFAULT_TESTING_PROFILE, 1);
		assertEquals(1, slotManager.getFreeSlotCount());

		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));
		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertEquals(0, slotManager.getAllocatedContainers().size());
	}

	/**
	 * Tests that there are some free slots when we request, but none of them are suitable
	 */
	@Test
	public void testRequestSlotWithoutSuitableSlot() {
		TestingSlotManager slotManager = new TestingSlotManager();

		directlyProvideFreeSlots(slotManager, DEFAULT_TESTING_PROFILE, 2);
		assertEquals(2, slotManager.getFreeSlotCount());

		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_BIG_PROFILE));
		assertEquals(0, slotManager.getAllocatedSlotCount());
		assertEquals(2, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertEquals(1, slotManager.getAllocatedContainers().size());
		assertEquals(DEFAULT_TESTING_BIG_PROFILE, slotManager.getAllocatedContainers().get(0));
	}

	/**
	 * Tests that we send duplicated slot request
	 */
	@Test
	public void testDuplicatedSlotRequest() {
		TestingSlotManager slotManager = new TestingSlotManager();
		directlyProvideFreeSlots(slotManager, DEFAULT_TESTING_PROFILE, 1);

		SlotRequest request1 = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		SlotRequest request2 = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_BIG_PROFILE);

		slotManager.requestSlot(request1);
		slotManager.requestSlot(request2);
		slotManager.requestSlot(request2);
		slotManager.requestSlot(request1);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertEquals(1, slotManager.getAllocatedContainers().size());
		assertEquals(DEFAULT_TESTING_BIG_PROFILE, slotManager.getAllocatedContainers().get(0));
	}

	/**
	 * Tests that we send multiple slot requests
	 */
	@Test
	public void testRequestMultipleSlots() {
		TestingSlotManager slotManager = new TestingSlotManager();
		directlyProvideFreeSlots(slotManager, DEFAULT_TESTING_PROFILE, 5);

		// request 3 normal slots
		for (int i = 0; i < 3; ++i) {
			slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));
		}

		// request 2 big slots
		for (int i = 0; i < 2; ++i) {
			slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_BIG_PROFILE));
		}

		// request 1 normal slot again
		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));

		assertEquals(4, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
		assertEquals(2, slotManager.getPendingRequestCount());
		assertEquals(2, slotManager.getAllocatedContainers().size());
		assertEquals(DEFAULT_TESTING_BIG_PROFILE, slotManager.getAllocatedContainers().get(0));
		assertEquals(DEFAULT_TESTING_BIG_PROFILE, slotManager.getAllocatedContainers().get(1));
	}

	/**
	 * Tests that a new slot appeared in SlotReport, and we used it to fulfill a pending request
	 */
	@Test
	public void testNewlyAppearedFreeSlotFulfillPendingRequest() {
		TestingSlotManager slotManager = new TestingSlotManager();
		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));
		assertEquals(1, slotManager.getPendingRequestCount());

		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slotId));
	}

	/**
	 * Tests that a new slot appeared in SlotReport, but we have no pending request
	 */
	@Test
	public void testNewlyAppearedFreeSlot() {
		TestingSlotManager slotManager = new TestingSlotManager();

		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(0, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
	}

	/**
	 * Tests that a new slot appeared in SlotReport, but it't not suitable for all the pending requests
	 */
	@Test
	public void testNewlyAppearedFreeSlotNotMatchPendingRequests() {
		TestingSlotManager slotManager = new TestingSlotManager();
		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_BIG_PROFILE));
		assertEquals(1, slotManager.getPendingRequestCount());

		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(0, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertFalse(slotManager.isAllocated(slotId));
	}

	/**
	 * Tests that a new slot appeared in SlotReport, and it's been reported using by some job
	 */
	@Test
	public void testNewlyAppearedInUseSlot() {
		TestingSlotManager slotManager = new TestingSlotManager();

		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE, new JobID(), new AllocationID());
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertTrue(slotManager.isAllocated(slotId));
	}

	/**
	 * Tests that we had a slot in-use, and it's confirmed by SlotReport
	 */
	@Test
	public void testExistingInUseSlotUpdateStatus() {
		TestingSlotManager slotManager = new TestingSlotManager();
		SlotRequest request = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request);

		// make this slot in use
		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertTrue(slotManager.isAllocated(slotId));

		// slot status is confirmed
		SlotStatus slotStatus2 = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE,
			request.getJobId(), request.getAllocationId());
		slotManager.updateSlotStatus(slotStatus2);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertTrue(slotManager.isAllocated(slotId));
	}

	/**
	 * Tests that we had a slot in-use, but it's empty according to the SlotReport
	 */
	@Test
	public void testExistingInUseSlotAdjustedToEmpty() {
		TestingSlotManager slotManager = new TestingSlotManager();
		SlotRequest request1 = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request1);

		// make this slot in use
		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		// another request pending
		SlotRequest request2 = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request2);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slotId));
		assertTrue(slotManager.isAllocated(request1.getAllocationId()));


		// but slot is reported empty again, request2 will be fulfilled, request1 will be missing
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slotId));
		assertTrue(slotManager.isAllocated(request2.getAllocationId()));
	}

	/**
	 * Tests that we had a slot in use, and it's also reported in use by TaskManager, but the allocation
	 * information didn't match.
	 */
	@Test
	public void testExistingInUseSlotWithDifferentAllocationInfo() {
		TestingSlotManager slotManager = new TestingSlotManager();
		SlotRequest request = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request);

		// make this slot in use
		SlotID slotId = SlotID.generate();
		slotManager.registerTaskExecutor(slotId.getResourceID(), taskExecutorGateway);
		SlotStatus slotStatus = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slotId));
		assertTrue(slotManager.isAllocated(request.getAllocationId()));

		SlotStatus slotStatus2 = new SlotStatus(slotId, DEFAULT_TESTING_PROFILE, new JobID(), new AllocationID());
		// update slot status with different allocation info
		slotManager.updateSlotStatus(slotStatus2);

		// original request is missing and won't be allocated
		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slotId));
		assertFalse(slotManager.isAllocated(request.getAllocationId()));
		assertTrue(slotManager.isAllocated(slotStatus2.getAllocationID()));
	}

	/**
	 * Tests that we had a free slot, and it's confirmed by SlotReport
	 */
	@Test
	public void testExistingEmptySlotUpdateStatus() {
		TestingSlotManager slotManager = new TestingSlotManager();
		ResourceSlot slot = new ResourceSlot(SlotID.generate(), DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		slotManager.addFreeSlot(slot);

		SlotStatus slotStatus = new SlotStatus(slot.getSlotId(), DEFAULT_TESTING_PROFILE);
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(0, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
	}

	/**
	 * Tests that we had a free slot, and it's reported in-use by TaskManager
	 */
	@Test
	public void testExistingEmptySlotAdjustedToInUse() {
		TestingSlotManager slotManager = new TestingSlotManager();
		final SlotID slotID = SlotID.generate();
		slotManager.registerTaskExecutor(slotID.getResourceID(), taskExecutorGateway);

		ResourceSlot slot = new ResourceSlot(slotID, DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		slotManager.addFreeSlot(slot);

		SlotStatus slotStatus = new SlotStatus(slot.getSlotId(), DEFAULT_TESTING_PROFILE,
			new JobID(), new AllocationID());
		slotManager.updateSlotStatus(slotStatus);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slot.getSlotId()));
	}

	/**
	 * Tests that we did some allocation but failed / rejected by TaskManager, request will retry
	 */
	@Test
	public void testSlotAllocationFailedAtTaskManager() {
		TestingSlotManager slotManager = new TestingSlotManager();
		ResourceSlot slot = new ResourceSlot(SlotID.generate(), DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		slotManager.addFreeSlot(slot);

		SlotRequest request = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertTrue(slotManager.isAllocated(slot.getSlotId()));

		slotManager.handleSlotRequestFailedAtTaskManager(request, slot.getSlotId());

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
	}


	/**
	 * Tests that we did some allocation but failed / rejected by TaskManager, and slot is occupied by another request
	 */
	@Test
	public void testSlotAllocationFailedAtTaskManagerOccupiedByOther() {
		TestingSlotManager slotManager = new TestingSlotManager();
		final SlotID slotID = SlotID.generate();
		slotManager.registerTaskExecutor(slotID.getResourceID(), taskExecutorGateway);

		ResourceSlot slot = new ResourceSlot(slotID, DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		slotManager.addFreeSlot(slot);

		SlotRequest request = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request);

		// slot is set empty by heartbeat
		SlotStatus slotStatus = new SlotStatus(slot.getSlotId(), slot.getResourceProfile());
		slotManager.updateSlotStatus(slotStatus);

		// another request took this slot
		SlotRequest request2 = new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE);
		slotManager.requestSlot(request2);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
		assertFalse(slotManager.isAllocated(request.getAllocationId()));
		assertTrue(slotManager.isAllocated(request2.getAllocationId()));

		// original request should be pended
		slotManager.handleSlotRequestFailedAtTaskManager(request, slot.getSlotId());

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(1, slotManager.getPendingRequestCount());
		assertFalse(slotManager.isAllocated(request.getAllocationId()));
		assertTrue(slotManager.isAllocated(request2.getAllocationId()));
	}

	@Test
	public void testNotifyTaskManagerFailure() {
		TestingSlotManager slotManager = new TestingSlotManager();

		ResourceID resource1 = ResourceID.generate();
		ResourceID resource2 = ResourceID.generate();

		ResourceSlot slot11 = new ResourceSlot(new SlotID(resource1, 1), DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		ResourceSlot slot12 = new ResourceSlot(new SlotID(resource1, 2), DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		ResourceSlot slot21 = new ResourceSlot(new SlotID(resource2, 1), DEFAULT_TESTING_PROFILE, taskExecutorGateway);
		ResourceSlot slot22 = new ResourceSlot(new SlotID(resource2, 2), DEFAULT_TESTING_PROFILE, taskExecutorGateway);

		slotManager.addFreeSlot(slot11);
		slotManager.addFreeSlot(slot21);

		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));
		slotManager.requestSlot(new SlotRequest(new JobID(), new AllocationID(), DEFAULT_TESTING_PROFILE));

		assertEquals(2, slotManager.getAllocatedSlotCount());
		assertEquals(0, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());

		slotManager.addFreeSlot(slot12);
		slotManager.addFreeSlot(slot22);

		assertEquals(2, slotManager.getAllocatedSlotCount());
		assertEquals(2, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());

		slotManager.notifyTaskManagerFailure(resource2);

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());

		// notify an not exist resource failure
		slotManager.notifyTaskManagerFailure(ResourceID.generate());

		assertEquals(1, slotManager.getAllocatedSlotCount());
		assertEquals(1, slotManager.getFreeSlotCount());
		assertEquals(0, slotManager.getPendingRequestCount());
	}

	// ------------------------------------------------------------------------
	//  testing utilities
	// ------------------------------------------------------------------------

	private void directlyProvideFreeSlots(
		final SlotManager slotManager,
		final ResourceProfile resourceProfile,
		final int freeSlotNum)
	{
		for (int i = 0; i < freeSlotNum; ++i) {
			slotManager.addFreeSlot(new ResourceSlot(SlotID.generate(), new ResourceProfile(resourceProfile), taskExecutorGateway));
		}
	}

	// ------------------------------------------------------------------------
	//  testing classes
	// ------------------------------------------------------------------------

	private static class TestingSlotManager extends SlotManager implements ResourceManagerServices {

		private final List<ResourceProfile> allocatedContainers;

		TestingSlotManager() {
			this.allocatedContainers = new LinkedList<>();
			setupResourceManagerServices(this);
		}

		/**
		 * Choose slot randomly if it matches requirement
		 *
		 * @param request   The slot request
		 * @param freeSlots All slots which can be used
		 * @return The chosen slot or null if cannot find a match
		 */
		@Override
		protected ResourceSlot chooseSlotToUse(SlotRequest request, Map<SlotID, ResourceSlot> freeSlots) {
			for (ResourceSlot slot : freeSlots.values()) {
				if (slot.isMatchingRequirement(request.getResourceProfile())) {
					return slot;
				}
			}
			return null;
		}

		/**
		 * Choose request randomly if offered slot can match its requirement
		 *
		 * @param offeredSlot     The free slot
		 * @param pendingRequests All the pending slot requests
		 * @return The chosen request's AllocationID or null if cannot find a match
		 */
		@Override
		protected SlotRequest chooseRequestToFulfill(ResourceSlot offeredSlot,
			Map<AllocationID, SlotRequest> pendingRequests)
		{
			for (Map.Entry<AllocationID, SlotRequest> pendingRequest : pendingRequests.entrySet()) {
				if (offeredSlot.isMatchingRequirement(pendingRequest.getValue().getResourceProfile())) {
					return pendingRequest.getValue();
				}
			}
			return null;
		}

		@Override
		public void allocateResource(ResourceProfile resourceProfile) {
			allocatedContainers.add(resourceProfile);
		}

		@Override
		public Executor getAsyncExecutor() {
			return Mockito.mock(Executor.class);
		}

		@Override
		public Executor getExecutor() {
			return Mockito.mock(Executor.class);
		}

		List<ResourceProfile> getAllocatedContainers() {
			return allocatedContainers;
		}

	}
}
