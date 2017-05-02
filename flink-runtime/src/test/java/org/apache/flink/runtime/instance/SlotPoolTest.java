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

package org.apache.flink.runtime.instance;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.rpc.MainThreadValidatorUtil;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.instance.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlotPoolTest extends TestLogger {

	private RpcService rpcService;

	private JobID jobId;

	private MainThreadValidatorUtil mainThreadValidatorUtil;

	private SlotPool slotPool;

	private ResourceManagerGateway resourceManagerGateway;

	@Before
	public void setUp() throws Exception {

		this.rpcService = new TestingSerialRpcService();
		this.jobId = new JobID();
		this.slotPool = new SlotPool(rpcService, jobId);

		this.mainThreadValidatorUtil = new MainThreadValidatorUtil(slotPool);

		mainThreadValidatorUtil.enterMainThread();

		final String jobManagerAddress = "foobar";

		slotPool.start(UUID.randomUUID(), jobManagerAddress);

		this.resourceManagerGateway = mock(ResourceManagerGateway.class);
		when(resourceManagerGateway
			.requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class)))
			.thenReturn(mock(Future.class, RETURNS_MOCKS));

		slotPool.connectToResourceManager(UUID.randomUUID(), resourceManagerGateway);
	}

	@After
	public void tearDown() throws Exception {
		mainThreadValidatorUtil.exitMainThread();
	}

	@Test
	public void testAllocateSimpleSlot() throws Exception {
		ResourceID resourceID = new ResourceID("resource");
		slotPool.registerTaskManager(resourceID);

		ScheduledUnit task = mock(ScheduledUnit.class);
		Future<SimpleSlot> future = slotPool.allocateSlot(task, DEFAULT_TESTING_PROFILE, null);
		assertFalse(future.isDone());

		ArgumentCaptor<SlotRequest> slotRequestArgumentCaptor = ArgumentCaptor.forClass(SlotRequest.class);
		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), slotRequestArgumentCaptor.capture(), any(Time.class));

		final SlotRequest slotRequest = slotRequestArgumentCaptor.getValue();

		AllocatedSlot allocatedSlot = createAllocatedSlot(resourceID, slotRequest.getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);
		assertTrue(slotPool.offerSlot(allocatedSlot));

		SimpleSlot slot = future.get(1, TimeUnit.SECONDS);
		assertTrue(future.isDone());
		assertTrue(slot.isAlive());
		assertEquals(resourceID, slot.getTaskManagerID());
		assertEquals(jobId, slot.getJobID());
		assertEquals(slotPool.getSlotOwner(), slot.getOwner());
		assertEquals(slotPool.getAllocatedSlots().get(slot.getAllocatedSlot().getSlotAllocationId()), slot);
	}

	@Test
	public void testAllocationFulfilledByReturnedSlot() throws Exception {
		ResourceID resourceID = new ResourceID("resource");
		slotPool.registerTaskManager(resourceID);

		Future<SimpleSlot> future1 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);
		Future<SimpleSlot> future2 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);

		assertFalse(future1.isDone());
		assertFalse(future2.isDone());

		ArgumentCaptor<SlotRequest> slotRequestArgumentCaptor = ArgumentCaptor.forClass(SlotRequest.class);
		verify(resourceManagerGateway, times(2))
			.requestSlot(any(UUID.class), any(UUID.class), slotRequestArgumentCaptor.capture(), any(Time.class));

		final List<SlotRequest> slotRequests = slotRequestArgumentCaptor.getAllValues();

		AllocatedSlot allocatedSlot = createAllocatedSlot(resourceID, slotRequests.get(0).getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);
		assertTrue(slotPool.offerSlot(allocatedSlot));

		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
		assertTrue(future1.isDone());
		assertFalse(future2.isDone());

		// return this slot to pool
		slot1.releaseSlot();

		// second allocation fulfilled by previous slot returning
		SimpleSlot slot2 = future2.get(1, TimeUnit.SECONDS);
		assertTrue(future2.isDone());

		assertNotEquals(slot1, slot2);
		assertTrue(slot1.isReleased());
		assertTrue(slot2.isAlive());
		assertEquals(slot1.getTaskManagerID(), slot2.getTaskManagerID());
		assertEquals(slot1.getSlotNumber(), slot2.getSlotNumber());
		assertEquals(slotPool.getAllocatedSlots().get(slot1.getAllocatedSlot().getSlotAllocationId()), slot2);
	}

	@Test
	public void testAllocateWithFreeSlot() throws Exception {
		ResourceID resourceID = new ResourceID("resource");
		slotPool.registerTaskManager(resourceID);

		Future<SimpleSlot> future1 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);
		assertFalse(future1.isDone());

		ArgumentCaptor<SlotRequest> slotRequestArgumentCaptor = ArgumentCaptor.forClass(SlotRequest.class);
		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), slotRequestArgumentCaptor.capture(), any(Time.class));

		final SlotRequest slotRequest = slotRequestArgumentCaptor.getValue();

		AllocatedSlot allocatedSlot = createAllocatedSlot(resourceID, slotRequest.getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);
		assertTrue(slotPool.offerSlot(allocatedSlot));

		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
		assertTrue(future1.isDone());

		// return this slot to pool
		slot1.releaseSlot();

		Future<SimpleSlot> future2 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);

		// second allocation fulfilled by previous slot returning
		SimpleSlot slot2 = future2.get(1, TimeUnit.SECONDS);
		assertTrue(future2.isDone());

		assertNotEquals(slot1, slot2);
		assertTrue(slot1.isReleased());
		assertTrue(slot2.isAlive());
		assertEquals(slot1.getTaskManagerID(), slot2.getTaskManagerID());
		assertEquals(slot1.getSlotNumber(), slot2.getSlotNumber());
	}

	@Test
	public void testOfferSlot() throws Exception {
		ResourceID resourceID = new ResourceID("resource");
		slotPool.registerTaskManager(resourceID);

		Future<SimpleSlot> future = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);
		assertFalse(future.isDone());

		ArgumentCaptor<SlotRequest> slotRequestArgumentCaptor = ArgumentCaptor.forClass(SlotRequest.class);
		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), slotRequestArgumentCaptor.capture(), any(Time.class));

		final SlotRequest slotRequest = slotRequestArgumentCaptor.getValue();

		// slot from unregistered resource
		AllocatedSlot invalid = createAllocatedSlot(new ResourceID("unregistered"), slotRequest.getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);
		assertFalse(slotPool.offerSlot(invalid));

		AllocatedSlot notRequested = createAllocatedSlot(resourceID, new AllocationID(), jobId, DEFAULT_TESTING_PROFILE);

		// we'll also accept non requested slots
		assertTrue(slotPool.offerSlot(notRequested));

		AllocatedSlot allocatedSlot = createAllocatedSlot(resourceID, slotRequest.getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);

		// accepted slot
		assertTrue(slotPool.offerSlot(allocatedSlot));
		SimpleSlot slot = future.get(1, TimeUnit.SECONDS);
		assertTrue(future.isDone());
		assertTrue(slot.isAlive());

		// duplicated offer with using slot
		assertTrue(slotPool.offerSlot(allocatedSlot));
		assertTrue(future.isDone());
		assertTrue(slot.isAlive());

		// duplicated offer with free slot
		slot.releaseSlot();
		assertTrue(slot.isReleased());
		assertTrue(slotPool.offerSlot(allocatedSlot));
	}

	@Test
	public void testReleaseResource() throws Exception {
		ResourceID resourceID = new ResourceID("resource");
		slotPool.registerTaskManager(resourceID);

		Future<SimpleSlot> future1 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);

		ArgumentCaptor<SlotRequest> slotRequestArgumentCaptor = ArgumentCaptor.forClass(SlotRequest.class);
		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), slotRequestArgumentCaptor.capture(), any(Time.class));

		final SlotRequest slotRequest = slotRequestArgumentCaptor.getValue();

		Future<SimpleSlot> future2 = slotPool.allocateSlot(mock(ScheduledUnit.class),DEFAULT_TESTING_PROFILE, null);

		AllocatedSlot allocatedSlot = createAllocatedSlot(resourceID, slotRequest.getAllocationId(), jobId, DEFAULT_TESTING_PROFILE);
		assertTrue(slotPool.offerSlot(allocatedSlot));

		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
		assertTrue(future1.isDone());
		assertFalse(future2.isDone());

		slotPool.releaseTaskManager(resourceID);
		assertTrue(slot1.isReleased());

		// slot released and not usable, second allocation still not fulfilled
		Thread.sleep(10);
		assertFalse(future2.isDone());
	}

	static AllocatedSlot createAllocatedSlot(
			final ResourceID resourceId,
			final AllocationID allocationId,
			final JobID jobId,
			final ResourceProfile resourceProfile) {
		TaskManagerLocation mockTaskManagerLocation = mock(TaskManagerLocation.class);
		when(mockTaskManagerLocation.getResourceID()).thenReturn(resourceId);

		TaskManagerGateway mockTaskManagerGateway = mock(TaskManagerGateway.class);

		return new AllocatedSlot(
			allocationId,
			jobId,
			mockTaskManagerLocation,
			0,
			resourceProfile,
			mockTaskManagerGateway);
	}

}
