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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for the {@link SlotSharingManager}.
 */
@Category(New.class)
public class SlotSharingManagerTest extends TestLogger {

	private static final SlotSharingGroupId SLOT_SHARING_GROUP_ID = new SlotSharingGroupId();

	private static final DummySlotOwner SLOT_OWNER = new DummySlotOwner();

	@Test
	public void testRootSlotCreation() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotRequestId slotRequestId = new SlotRequestId();
		SlotRequestId allocatedSlotRequestId = new SlotRequestId();

		final SlotSharingManager.MultiTaskSlot multiTaskSlot = slotSharingManager.createRootSlot(
			slotRequestId,
			new CompletableFuture<>(),
			allocatedSlotRequestId);

		assertEquals(slotRequestId, multiTaskSlot.getSlotRequestId());
		assertNotNull(slotSharingManager.getTaskSlot(slotRequestId));
	}

	@Test
	public void testRootSlotRelease() throws ExecutionException, InterruptedException {
		final CompletableFuture<SlotRequestId> slotReleasedFuture = new CompletableFuture<>();
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		allocatedSlotActions.setReleaseSlotConsumer(
			tuple3 -> slotReleasedFuture.complete(tuple3.f0));

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotRequestId slotRequestId = new SlotRequestId();
		SlotRequestId allocatedSlotRequestId = new SlotRequestId();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			slotRequestId,
			slotContextFuture,
			allocatedSlotRequestId);

		assertTrue(slotSharingManager.contains(slotRequestId));

		assertTrue(rootSlot.release(new FlinkException("Test exception")));

		// check that we return the allocated slot
		assertEquals(allocatedSlotRequestId, slotReleasedFuture.get());

		assertFalse(slotSharingManager.contains(slotRequestId));
	}

	/**
	 * Tests that we can create nested slots.
	 */
	@Test
	public void testNestedSlotCreation() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			new CompletableFuture<>(),
			new SlotRequestId());

		AbstractID singleTaskSlotGroupId = new AbstractID();
		SlotRequestId singleTaskSlotRequestId = new SlotRequestId();
		SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			singleTaskSlotRequestId,
			singleTaskSlotGroupId,
			Locality.LOCAL);

		AbstractID multiTaskSlotGroupId = new AbstractID();
		SlotRequestId multiTaskSlotRequestId = new SlotRequestId();
		SlotSharingManager.MultiTaskSlot multiTaskSlot = rootSlot.allocateMultiTaskSlot(
			multiTaskSlotRequestId,
			multiTaskSlotGroupId);

		assertTrue(Objects.equals(singleTaskSlotRequestId, singleTaskSlot.getSlotRequestId()));
		assertTrue(Objects.equals(multiTaskSlotRequestId, multiTaskSlot.getSlotRequestId()));

		assertTrue(rootSlot.contains(singleTaskSlotGroupId));
		assertTrue(rootSlot.contains(multiTaskSlotGroupId));

		assertTrue(slotSharingManager.contains(singleTaskSlotRequestId));
		assertTrue(slotSharingManager.contains(multiTaskSlotRequestId));
	}

	/**
	 * Tests that we can release nested slots from the leaves onwards
	 */
	@Test
	public void testNestedSlotRelease() throws Exception {
		TestingAllocatedSlotActions testingAllocatedSlotActions = new TestingAllocatedSlotActions();

		final CompletableFuture<SlotRequestId> releasedSlotFuture = new CompletableFuture<>();
		testingAllocatedSlotActions.setReleaseSlotConsumer(
			tuple3 -> releasedSlotFuture.complete(tuple3.f0));

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			testingAllocatedSlotActions,
			SLOT_OWNER);

		SlotRequestId rootSlotRequestId = new SlotRequestId();
		SlotRequestId allocatedSlotRequestId = new SlotRequestId();
		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			rootSlotRequestId,
			new CompletableFuture<>(),
			allocatedSlotRequestId);

		SlotRequestId singleTaskSlotRequestId = new SlotRequestId();
		SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			singleTaskSlotRequestId,
			new AbstractID(),
			Locality.LOCAL);

		SlotRequestId multiTaskSlotRequestId = new SlotRequestId();
		SlotSharingManager.MultiTaskSlot multiTaskSlot = rootSlot.allocateMultiTaskSlot(
			multiTaskSlotRequestId,
			new AbstractID());

		CompletableFuture<LogicalSlot> singleTaskSlotFuture = singleTaskSlot.getLogicalSlotFuture();

		assertTrue(slotSharingManager.contains(rootSlotRequestId));
		assertTrue(slotSharingManager.contains(singleTaskSlotRequestId));
		assertFalse(singleTaskSlotFuture.isDone());

		FlinkException testException = new FlinkException("Test exception");
		assertTrue(singleTaskSlot.release(testException));

		// check that we fail the single task slot future
		assertTrue(singleTaskSlotFuture.isCompletedExceptionally());
		assertFalse(slotSharingManager.contains(singleTaskSlotRequestId));

		// the root slot has still one child
		assertTrue(slotSharingManager.contains(rootSlotRequestId));

		assertTrue(multiTaskSlot.release(testException));

		assertEquals(allocatedSlotRequestId, releasedSlotFuture.get());
		assertFalse(slotSharingManager.contains(rootSlotRequestId));
		assertFalse(slotSharingManager.contains(multiTaskSlotRequestId));

		assertTrue(slotSharingManager.isEmpty());
	}

	/**
	 * Tests that we can release inner slots and that this triggers the slot release for all
	 * its children.
	 */
	@Test
	public void testInnerSlotRelease() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			new CompletableFuture<>(),
			new SlotRequestId());

		SlotSharingManager.MultiTaskSlot multiTaskSlot = rootSlot.allocateMultiTaskSlot(
			new SlotRequestId(),
			new AbstractID());

		SlotSharingManager.SingleTaskSlot singleTaskSlot1 = multiTaskSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			new AbstractID(),
			Locality.LOCAL);

		SlotSharingManager.MultiTaskSlot multiTaskSlot1 = multiTaskSlot.allocateMultiTaskSlot(
			new SlotRequestId(),
			new AbstractID());

		assertTrue(slotSharingManager.contains(multiTaskSlot1.getSlotRequestId()));
		assertTrue(slotSharingManager.contains(singleTaskSlot1.getSlotRequestId()));
		assertTrue(slotSharingManager.contains(multiTaskSlot.getSlotRequestId()));

		multiTaskSlot.release(new FlinkException("Test exception"));

		assertFalse(slotSharingManager.contains(multiTaskSlot1.getSlotRequestId()));
		assertFalse(slotSharingManager.contains(singleTaskSlot1.getSlotRequestId()));
		assertFalse(slotSharingManager.contains(multiTaskSlot.getSlotRequestId()));
		assertTrue(singleTaskSlot1.getLogicalSlotFuture().isCompletedExceptionally());
	}

	/**
	 * Tests that the logical task slot futures are completed once the slot context
	 * future is completed.
	 */
	@Test
	public void testSlotContextFutureCompletion() throws Exception {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		final SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		final SlotContext slotContext = new SimpleSlotContext(
			new AllocationID(),
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();
		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		Locality locality1 = Locality.LOCAL;
		SlotSharingManager.SingleTaskSlot singleTaskSlot1 = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			new AbstractID(),
			locality1);

		Locality locality2 = Locality.HOST_LOCAL;
		SlotSharingManager.SingleTaskSlot singleTaskSlot2 = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			new AbstractID(),
			locality2);

		CompletableFuture<LogicalSlot> logicalSlotFuture1 = singleTaskSlot1.getLogicalSlotFuture();
		CompletableFuture<LogicalSlot> logicalSlotFuture2 = singleTaskSlot2.getLogicalSlotFuture();
		assertFalse(logicalSlotFuture1.isDone());
		assertFalse(logicalSlotFuture2.isDone());

		slotContextFuture.complete(slotContext);

		assertTrue(logicalSlotFuture1.isDone());
		assertTrue(logicalSlotFuture2.isDone());

		final LogicalSlot logicalSlot1 = logicalSlotFuture1.get();
		final LogicalSlot logicalSlot2 = logicalSlotFuture2.get();

		assertEquals(logicalSlot1.getAllocationId(), slotContext.getAllocationId());
		assertEquals(logicalSlot2.getAllocationId(), slotContext.getAllocationId());
		assertEquals(locality1, logicalSlot1.getLocality());
		assertEquals(locality2, logicalSlot2.getLocality());

		Locality locality3 = Locality.NON_LOCAL;
		SlotSharingManager.SingleTaskSlot singleTaskSlot3 = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			new AbstractID(),
			locality3);

		CompletableFuture<LogicalSlot> logicalSlotFuture3 = singleTaskSlot3.getLogicalSlotFuture();

		assertTrue(logicalSlotFuture3.isDone());
		LogicalSlot logicalSlot3 = logicalSlotFuture3.get();

		assertEquals(locality3, logicalSlot3.getLocality());
		assertEquals(slotContext.getAllocationId(), logicalSlot3.getAllocationId());
	}

	/**
	 * Tests that slot context future failures will release the root slot
	 */
	@Test
	public void testSlotContextFutureFailure() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();

		assertTrue(slotSharingManager.isEmpty());

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			new AbstractID(),
			Locality.LOCAL);

		slotContextFuture.completeExceptionally(new FlinkException("Test exception"));

		assertTrue(singleTaskSlot.getLogicalSlotFuture().isCompletedExceptionally());
		assertTrue(slotSharingManager.isEmpty());
		assertTrue(slotSharingManager.getResolvedRootSlots().isEmpty());
		assertTrue(slotSharingManager.getUnresolvedRootSlots().isEmpty());
	}

	/**
	 * Tests that the root slot are moved from unresolved to resolved once the
	 * slot context future is successfully completed
	 */
	@Test
	public void testRootSlotTransition() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();
		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		assertTrue(slotSharingManager.getUnresolvedRootSlots().contains(rootSlot));
		assertFalse(slotSharingManager.getResolvedRootSlots().contains(rootSlot));

		// now complete the slotContextFuture
		slotContextFuture.complete(
			new SimpleSlotContext(
				new AllocationID(),
				new LocalTaskManagerLocation(),
				0,
				new SimpleAckingTaskManagerGateway()));

		assertFalse(slotSharingManager.getUnresolvedRootSlots().contains(rootSlot));
		assertTrue(slotSharingManager.getResolvedRootSlots().contains(rootSlot));
	}

	/**
	 * Tests that we can correctly retrieve resolved slots.
	 */
	@Test
	public void testGetResolvedSlot() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			CompletableFuture.completedFuture(
				new SimpleSlotContext(
					new AllocationID(),
					new LocalTaskManagerLocation(),
					0,
					new SimpleAckingTaskManagerGateway())),
			new SlotRequestId());

		AbstractID groupId = new AbstractID();
		SlotSharingManager.MultiTaskSlotLocality resolvedRootSlotLocality =
			slotSharingManager.getResolvedRootSlot(groupId, SlotProfile.noRequirements().matcher());

		assertNotNull(resolvedRootSlotLocality);
		assertEquals(Locality.UNCONSTRAINED, resolvedRootSlotLocality.getLocality());
		assertEquals(rootSlot.getSlotRequestId(), resolvedRootSlotLocality.getMultiTaskSlot().getSlotRequestId());

		SlotSharingManager.MultiTaskSlot resolvedRootSlot = resolvedRootSlotLocality.getMultiTaskSlot();

		// occupy the resolved root slot
		resolvedRootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			groupId,
			resolvedRootSlotLocality.getLocality());

		SlotSharingManager.MultiTaskSlotLocality resolvedRootSlot1 = slotSharingManager.getResolvedRootSlot(
			groupId,
			SlotProfile.noRequirements().matcher());

		assertNull(resolvedRootSlot1);
	}

	/**
	 * Tests that the location preferences are honoured when looking for a resolved slot.
	 */
	@Test
	public void testGetResolvedSlotWithLocationPreferences() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotSharingManager.MultiTaskSlot rootSlot1 = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			CompletableFuture.completedFuture(
				new SimpleSlotContext(
					new AllocationID(),
					new LocalTaskManagerLocation(),
					0,
					new SimpleAckingTaskManagerGateway())),
			new SlotRequestId());

		LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		SlotSharingManager.MultiTaskSlot rootSlot2 = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			CompletableFuture.completedFuture(
				new SimpleSlotContext(
					new AllocationID(),
					taskManagerLocation,
					0,
					new SimpleAckingTaskManagerGateway())),
			new SlotRequestId());

		AbstractID groupId = new AbstractID();
		SlotProfile.LocalityAwareRequirementsToSlotMatcher matcher =
			new SlotProfile.LocalityAwareRequirementsToSlotMatcher(Collections.singleton(taskManagerLocation));
		SlotSharingManager.MultiTaskSlotLocality resolvedRootSlot1 = slotSharingManager.getResolvedRootSlot(groupId, matcher);
		assertNotNull(resolvedRootSlot1);
		assertEquals(Locality.LOCAL, resolvedRootSlot1.getLocality());
		assertEquals(rootSlot2.getSlotRequestId(), resolvedRootSlot1.getMultiTaskSlot().getSlotRequestId());

		// occupy the slot
		resolvedRootSlot1.getMultiTaskSlot().allocateSingleTaskSlot(
			new SlotRequestId(),
			groupId,
			resolvedRootSlot1.getLocality());

		SlotSharingManager.MultiTaskSlotLocality resolvedRootSlot2 = slotSharingManager.getResolvedRootSlot(groupId,matcher);

		assertNotNull(resolvedRootSlot2);
		assertNotSame(Locality.LOCAL, (resolvedRootSlot2.getLocality()));
		assertEquals(rootSlot1.getSlotRequestId(), resolvedRootSlot2.getMultiTaskSlot().getSlotRequestId());
	}

	@Test
	public void testGetUnresolvedSlot() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		SlotSharingManager.MultiTaskSlot rootSlot1 = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			new CompletableFuture<>(),
			new SlotRequestId());

		final AbstractID groupId = new AbstractID();
		SlotSharingManager.MultiTaskSlot unresolvedRootSlot = slotSharingManager.getUnresolvedRootSlot(groupId);

		assertNotNull(unresolvedRootSlot);
		assertEquals(rootSlot1.getSlotRequestId(), unresolvedRootSlot.getSlotRequestId());

		// occupy the unresolved slot
		unresolvedRootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			groupId,
			Locality.UNKNOWN);

		SlotSharingManager.MultiTaskSlot unresolvedRootSlot1 = slotSharingManager.getUnresolvedRootSlot(groupId);

		// we should no longer have a free unresolved root slot
		assertNull(unresolvedRootSlot1);
	}
}
