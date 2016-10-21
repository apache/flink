///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.runtime.instance;
//
//import org.apache.flink.api.common.JobID;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.runtime.clusterframework.types.AllocationID;
//import org.apache.flink.runtime.clusterframework.types.ResourceID;
//import org.apache.flink.runtime.concurrent.BiFunction;
//import org.apache.flink.runtime.concurrent.Future;
//import org.apache.flink.runtime.jobgraph.JobVertexID;
//import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
//import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
//import org.apache.flink.runtime.resourcemanager.SlotRequest;
//import org.apache.flink.util.TestLogger;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.UUID;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.Executor;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;
//
//import static org.apache.flink.runtime.instance.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
//import static org.apache.flink.runtime.instance.AvailableSlotsTest.createSlotDescriptor;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNotEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.mockito.Matchers.any;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//
//public class SlotPoolTest extends TestLogger {
//
//	private ExecutorService executor;
//
//	private SlotPool slotPool;
//
//	private ResourceManagerGateway resourceManagerGateway;
//
//	@Before
//	public void setUp() throws Exception {
//		this.executor = Executors.newFixedThreadPool(1);
//		this.slotPool = new SlotPool(executor);
//		this.resourceManagerGateway = mock(ResourceManagerGateway.class);
//		when(resourceManagerGateway
//			.requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class)))
//			.thenReturn(mock(Future.class));
//
//		slotPool.setResourceManager(UUID.randomUUID(), resourceManagerGateway);
//		slotPool.setJobManagerLeaderId(UUID.randomUUID());
//	}
//
//	@After
//	public void tearDown() throws Exception {
//	}
//
//	@Test
//	public void testAllocateSimpleSlot() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobID jobID = new JobID();
//		AllocationID allocationID = new AllocationID();
//		Future<SimpleSlot> future = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID);
//		assertFalse(future.isDone());
//		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class));
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertTrue(slotPool.offerSlot(allocationID, slotDescriptor));
//
//		SimpleSlot slot = future.get(1, TimeUnit.SECONDS);
//		assertTrue(future.isDone());
//		assertTrue(slot.isAlive());
//		assertEquals(resourceID, slot.getTaskManagerID());
//		assertEquals(jobID, slot.getJobID());
//		assertEquals(slotPool, slot.getOwner());
//	}
//
//	@Test
//	public void testAllocateSharedSlot() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobVertexID vid = new JobVertexID();
//		SlotSharingGroup sharingGroup = new SlotSharingGroup(vid);
//		SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();
//
//		JobID jobID = new JobID();
//		AllocationID allocationID = new AllocationID();
//		Future<SharedSlot> future = slotPool.allocateSharedSlot(jobID, DEFAULT_TESTING_PROFILE, assignment, allocationID);
//
//		assertFalse(future.isDone());
//		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class));
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertTrue(slotPool.offerSlot(allocationID, slotDescriptor));
//
//		SharedSlot slot = future.get(1, TimeUnit.SECONDS);
//		assertTrue(future.isDone());
//		assertTrue(slot.isAlive());
//		assertEquals(resourceID, slot.getTaskManagerID());
//		assertEquals(jobID, slot.getJobID());
//		assertEquals(slotPool, slot.getOwner());
//
//		SimpleSlot simpleSlot = slot.allocateSubSlot(vid);
//		assertNotNull(simpleSlot);
//		assertTrue(simpleSlot.isAlive());
//	}
//
//	@Test
//	public void testAllocateSlotWithoutResourceManager() throws Exception {
//		slotPool.disconnectResourceManager();
//		Future<SimpleSlot> future = slotPool.allocateSimpleSlot(new JobID(), DEFAULT_TESTING_PROFILE);
//		future.handleAsync(
//			new BiFunction<SimpleSlot, Throwable, Void>() {
//				@Override
//				public Void apply(SimpleSlot simpleSlot, Throwable throwable) {
//					assertNull(simpleSlot);
//					assertNotNull(throwable);
//					return null;
//				}
//			},
//			executor);
//		try {
//			future.get(1, TimeUnit.SECONDS);
//			fail("We expected a ExecutionException.");
//		} catch (ExecutionException ex) {
//			// we expect the exception
//		}
//	}
//
//	@Test
//	public void testAllocationFulfilledByReturnedSlot() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobID jobID = new JobID();
//
//		AllocationID allocationID1 = new AllocationID();
//		Future<SimpleSlot> future1 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID1);
//
//		AllocationID allocationID2 = new AllocationID();
//		Future<SimpleSlot> future2 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID2);
//
//		assertFalse(future1.isDone());
//		assertFalse(future2.isDone());
//		verify(resourceManagerGateway, times(2))
//			.requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class));
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertTrue(slotPool.offerSlot(allocationID1, slotDescriptor));
//
//		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
//		assertTrue(future1.isDone());
//		assertFalse(future2.isDone());
//
//		// return this slot to pool
//		slot1.releaseSlot();
//
//		// second allocation fulfilled by previous slot returning
//		SimpleSlot slot2 = future2.get(1, TimeUnit.SECONDS);
//		assertTrue(future2.isDone());
//
//		assertNotEquals(slot1, slot2);
//		assertTrue(slot1.isReleased());
//		assertTrue(slot2.isAlive());
//		assertEquals(slot1.getTaskManagerID(), slot2.getTaskManagerID());
//		assertEquals(slot1.getSlotNumber(), slot2.getSlotNumber());
//	}
//
//	@Test
//	public void testAllocateWithFreeSlot() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobID jobID = new JobID();
//		AllocationID allocationID1 = new AllocationID();
//		Future<SimpleSlot> future1 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID1);
//		assertFalse(future1.isDone());
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertTrue(slotPool.offerSlot(allocationID1, slotDescriptor));
//
//		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
//		assertTrue(future1.isDone());
//
//		// return this slot to pool
//		slot1.releaseSlot();
//
//		AllocationID allocationID2 = new AllocationID();
//		Future<SimpleSlot> future2 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID2);
//
//		// second allocation fulfilled by previous slot returning
//		SimpleSlot slot2 = future2.get(1, TimeUnit.SECONDS);
//		assertTrue(future2.isDone());
//
//		assertNotEquals(slot1, slot2);
//		assertTrue(slot1.isReleased());
//		assertTrue(slot2.isAlive());
//		assertEquals(slot1.getTaskManagerID(), slot2.getTaskManagerID());
//		assertEquals(slot1.getSlotNumber(), slot2.getSlotNumber());
//	}
//
//	@Test
//	public void testOfferSlot() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobID jobID = new JobID();
//		AllocationID allocationID = new AllocationID();
//		Future<SimpleSlot> future = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID);
//		assertFalse(future.isDone());
//		verify(resourceManagerGateway).requestSlot(any(UUID.class), any(UUID.class), any(SlotRequest.class), any(Time.class));
//
//		// slot from unregistered resource
//		SlotDescriptor invalid = createSlotDescriptor(new ResourceID("unregistered"), jobID, DEFAULT_TESTING_PROFILE);
//		assertFalse(slotPool.offerSlot(allocationID, invalid));
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//
//		// reject offering with mismatch allocation id
//		assertFalse(slotPool.offerSlot(new AllocationID(), slotDescriptor));
//
//		// accepted slot
//		assertTrue(slotPool.offerSlot(allocationID, slotDescriptor));
//		SimpleSlot slot = future.get(1, TimeUnit.SECONDS);
//		assertTrue(future.isDone());
//		assertTrue(slot.isAlive());
//
//		// conflict offer with using slot
//		SlotDescriptor conflict = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertFalse(slotPool.offerSlot(allocationID, conflict));
//
//		// duplicated offer with using slot
//		assertTrue(slotPool.offerSlot(allocationID, slotDescriptor));
//		assertTrue(future.isDone());
//		assertTrue(slot.isAlive());
//
//		// duplicated offer with free slot
//		slot.releaseSlot();
//		assertTrue(slot.isReleased());
//		assertTrue(slotPool.offerSlot(allocationID, slotDescriptor));
//	}
//
//	@Test
//	public void testReleaseResource() throws Exception {
//		ResourceID resourceID = new ResourceID("resource");
//		slotPool.registerResource(resourceID);
//
//		JobID jobID = new JobID();
//
//		AllocationID allocationID1 = new AllocationID();
//		Future<SimpleSlot> future1 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID1);
//
//		AllocationID allocationID2 = new AllocationID();
//		Future<SimpleSlot> future2 = slotPool.allocateSimpleSlot(jobID, DEFAULT_TESTING_PROFILE, allocationID2);
//
//		SlotDescriptor slotDescriptor = createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
//		assertTrue(slotPool.offerSlot(allocationID1, slotDescriptor));
//
//		SimpleSlot slot1 = future1.get(1, TimeUnit.SECONDS);
//		assertTrue(future1.isDone());
//		assertFalse(future2.isDone());
//
//		slotPool.releaseResource(resourceID);
//		assertTrue(slot1.isReleased());
//
//		// slot released and not usable, second allocation still not fulfilled
//		Thread.sleep(10);
//		assertFalse(future2.isDone());
//	}
//
//}
