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

import org.apache.flink.api.common.resources.GPUResource;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Test cases for the {@link SlotSharingManager}.
 */
public class SlotSharingManagerTest extends TestLogger {

	private static final SlotSharingGroupId SLOT_SHARING_GROUP_ID = new SlotSharingGroupId();

	private static final DummySlotOwner SLOT_OWNER = new DummySlotOwner();

	@Test
	public void testRootSlotCreation() {
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

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

		rootSlot.release(new FlinkException("Test exception"));

		// check that we return the allocated slot
		assertEquals(allocatedSlotRequestId, slotReleasedFuture.get());

		assertFalse(slotSharingManager.contains(slotRequestId));
	}

	/**
	 * Tests that we can create nested slots.
	 */
	@Test
	public void testNestedSlotCreation() {
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			new CompletableFuture<>(),
			new SlotRequestId());

		AbstractID singleTaskSlotGroupId = new AbstractID();
		SlotRequestId singleTaskSlotRequestId = new SlotRequestId();
		SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			singleTaskSlotRequestId,
			ResourceProfile.UNKNOWN,
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
	 * Tests that we can release nested slots from the leaves onwards.
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
			ResourceProfile.UNKNOWN,
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
		singleTaskSlot.release(testException);

		// check that we fail the single task slot future
		assertTrue(singleTaskSlotFuture.isCompletedExceptionally());
		assertFalse(slotSharingManager.contains(singleTaskSlotRequestId));

		// the root slot has still one child
		assertTrue(slotSharingManager.contains(rootSlotRequestId));

		multiTaskSlot.release(testException);

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
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			new CompletableFuture<>(),
			new SlotRequestId());

		SlotSharingManager.MultiTaskSlot multiTaskSlot = rootSlot.allocateMultiTaskSlot(
			new SlotRequestId(),
			new AbstractID());

		SlotSharingManager.SingleTaskSlot singleTaskSlot1 = multiTaskSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
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
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		final SlotContext slotContext = createSimpleSlotContext();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();
		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		Locality locality1 = Locality.LOCAL;
		SlotSharingManager.SingleTaskSlot singleTaskSlot1 = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
			new AbstractID(),
			locality1);

		Locality locality2 = Locality.HOST_LOCAL;
		SlotSharingManager.SingleTaskSlot singleTaskSlot2 = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
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
			ResourceProfile.UNKNOWN,
			new AbstractID(),
			locality3);

		CompletableFuture<LogicalSlot> logicalSlotFuture3 = singleTaskSlot3.getLogicalSlotFuture();

		assertTrue(logicalSlotFuture3.isDone());
		LogicalSlot logicalSlot3 = logicalSlotFuture3.get();

		assertEquals(locality3, logicalSlot3.getLocality());
		assertEquals(slotContext.getAllocationId(), logicalSlot3.getAllocationId());
	}

	private SimpleSlotContext createSimpleSlotContext() {
		return new SimpleSlotContext(
			new AllocationID(),
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());
	}

	/**
	 * Tests that slot context future failures will release the root slot.
	 */
	@Test
	public void testSlotContextFutureFailure() {
		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();

		assertTrue(slotSharingManager.isEmpty());

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
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
	 * slot context future is successfully completed.
	 */
	@Test
	public void testRootSlotTransition() {
		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();
		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			slotContextFuture,
			new SlotRequestId());

		assertTrue(slotSharingManager.getUnresolvedRootSlots().contains(rootSlot));
		assertFalse(slotSharingManager.getResolvedRootSlots().contains(rootSlot));

		// now complete the slotContextFuture
		slotContextFuture.complete(createSimpleSlotContext());

		assertFalse(slotSharingManager.getUnresolvedRootSlots().contains(rootSlot));
		assertTrue(slotSharingManager.getResolvedRootSlots().contains(rootSlot));
	}

	/**
	 * Tests that we can correctly retrieve resolved slots.
	 */
	@Test
	public void testGetResolvedSlot() {
		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot rootSlot = createRootSlot(new LocalTaskManagerLocation(), slotSharingManager);

		AbstractID groupId = new AbstractID();

		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfos = slotSharingManager.listResolvedRootSlotInfo(groupId);
		Assert.assertEquals(1, slotInfos.size());

		SlotSelectionStrategy.SlotInfoAndResources slotInfoAndRemainingResource = slotInfos.iterator().next();
		SlotSharingManager.MultiTaskSlot resolvedMultiTaskSlot =
			slotSharingManager.getResolvedRootSlot(slotInfoAndRemainingResource.getSlotInfo());

		final LocationPreferenceSlotSelectionStrategy locationPreferenceSlotSelectionStrategy = LocationPreferenceSlotSelectionStrategy.createDefault();
		SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality = locationPreferenceSlotSelectionStrategy
			.selectBestSlotForProfile(slotInfos, SlotProfile.noRequirements()).get();

		assertNotNull(resolvedMultiTaskSlot);
		assertEquals(Locality.UNCONSTRAINED, slotInfoAndLocality.getLocality());
		assertEquals(rootSlot.getSlotRequestId(), resolvedMultiTaskSlot.getSlotRequestId());

		// occupy the resolved root slot
		resolvedMultiTaskSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
			groupId,
			Locality.UNCONSTRAINED);

		slotInfos = slotSharingManager.listResolvedRootSlotInfo(groupId);
		assertTrue(slotInfos.isEmpty());
	}

	/**
	 * Tests that the location preferences are honoured when looking for a resolved slot.
	 */
	@Test
	public void testGetResolvedSlotWithLocationPreferences() {
		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot rootSlot1 = createRootSlot(new LocalTaskManagerLocation(), slotSharingManager);

		LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		SlotSharingManager.MultiTaskSlot rootSlot2 = createRootSlot(taskManagerLocation, slotSharingManager);

		AbstractID groupId = new AbstractID();

		SlotProfile slotProfile = SlotProfile.preferredLocality(ResourceProfile.UNKNOWN, Collections.singleton(taskManagerLocation));

		Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfos = slotSharingManager.listResolvedRootSlotInfo(groupId);
		final LocationPreferenceSlotSelectionStrategy locationPreferenceSlotSelectionStrategy = LocationPreferenceSlotSelectionStrategy.createDefault();
		SlotSelectionStrategy.SlotInfoAndLocality slotInfoAndLocality =
			locationPreferenceSlotSelectionStrategy.selectBestSlotForProfile(slotInfos, slotProfile).get();
		SlotSharingManager.MultiTaskSlot resolvedRootSlot = slotSharingManager.getResolvedRootSlot(slotInfoAndLocality.getSlotInfo());

		assertNotNull(resolvedRootSlot);
		assertEquals(Locality.LOCAL, slotInfoAndLocality.getLocality());
		assertEquals(rootSlot2.getSlotRequestId(), resolvedRootSlot.getSlotRequestId());

		// occupy the slot
		resolvedRootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
			groupId,
			slotInfoAndLocality.getLocality());

		slotInfos = slotSharingManager.listResolvedRootSlotInfo(groupId);
		slotInfoAndLocality = locationPreferenceSlotSelectionStrategy.selectBestSlotForProfile(slotInfos, slotProfile).get();
		resolvedRootSlot = slotSharingManager.getResolvedRootSlot(slotInfoAndLocality.getSlotInfo());
		assertNotNull(resolvedRootSlot);
		assertNotSame(Locality.LOCAL, (slotInfoAndLocality.getLocality()));
		assertEquals(rootSlot1.getSlotRequestId(), resolvedRootSlot.getSlotRequestId());
	}

	/**
	 * Tests that we cannot retrieve a slot when it's releasing children.
	 */
	@Test
	public void testResolvedSlotInReleasingIsNotAvailable() throws Exception {
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		final SlotSharingManager.MultiTaskSlot rootSlot = createRootSlot(new LocalTaskManagerLocation(), slotSharingManager);

		final AbstractID groupId1 = new AbstractID();
		final SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
			groupId1,
			Locality.UNCONSTRAINED);

		final AtomicBoolean verified = new AtomicBoolean(false);

		final AbstractID groupId2 = new AbstractID();
		// register a verification in MultiTaskSlot's children releasing loop
		singleTaskSlot.getLogicalSlotFuture().get().tryAssignPayload(new LogicalSlot.Payload() {
			@Override
			public void fail(Throwable cause) {
				assertEquals(0, slotSharingManager.listResolvedRootSlotInfo(groupId2).size());

				verified.set(true);
			}

			@Override
			public CompletableFuture<?> getTerminalStateFuture() {
				return null;
			}
		});

		assertEquals(1, slotSharingManager.listResolvedRootSlotInfo(groupId2).size());

		rootSlot.release(new Exception("test exception"));

		// ensure the verification in Payload#fail is passed
		assertTrue(verified.get());
	}

	@Test
	public void testGetUnresolvedSlot() {
		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

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
			ResourceProfile.UNKNOWN,
			groupId,
			Locality.UNKNOWN);

		SlotSharingManager.MultiTaskSlot unresolvedRootSlot1 = slotSharingManager.getUnresolvedRootSlot(groupId);

		// we should no longer have a free unresolved root slot
		assertNull(unresolvedRootSlot1);
	}

	@Test
	public void testResourceCalculationOnSlotAllocatingAndReleasing() {
		final ResourceProfile rp1 = ResourceProfile.newBuilder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setManagedMemoryMB(100)
			.setShuffleMemoryMB(100)
			.build();
		final ResourceProfile rp2 = ResourceProfile.newBuilder()
			.setCpuCores(2.0)
			.setTaskHeapMemoryMB(200)
			.setTaskOffHeapMemoryMB(200)
			.setManagedMemoryMB(200)
			.setShuffleMemoryMB(200)
			.addExtendedResource("gpu", new GPUResource(2.0))
			.build();
		final ResourceProfile rp3 = ResourceProfile.newBuilder()
			.setCpuCores(3.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setManagedMemoryMB(300)
			.setShuffleMemoryMB(300)
			.addExtendedResource("gpu", new GPUResource(3.0))
			.build();

		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot unresolvedRootSlot = slotSharingManager.createRootSlot(
				new SlotRequestId(),
				new CompletableFuture<>(),
				new SlotRequestId());

		// Allocates the left subtree.
		SlotSharingManager.MultiTaskSlot leftMultiTaskSlot =
				unresolvedRootSlot.allocateMultiTaskSlot(new SlotRequestId(), new SlotSharingGroupId());

		SlotSharingManager.SingleTaskSlot firstChild = leftMultiTaskSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp1,
				new SlotSharingGroupId(),
				Locality.LOCAL);
		SlotSharingManager.SingleTaskSlot secondChild = leftMultiTaskSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp2,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		assertEquals(rp1, firstChild.getReservedResources());
		assertEquals(rp2, secondChild.getReservedResources());
		assertEquals(rp1.merge(rp2), leftMultiTaskSlot.getReservedResources());
		assertEquals(rp1.merge(rp2), unresolvedRootSlot.getReservedResources());

		// Allocates the right subtree.
		SlotSharingManager.SingleTaskSlot thirdChild = unresolvedRootSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp3,
				new SlotSharingGroupId(),
				Locality.LOCAL);
		assertEquals(rp3, thirdChild.getReservedResources());
		assertEquals(rp1.merge(rp2).merge(rp3), unresolvedRootSlot.getReservedResources());

		// Releases the second child in the left-side tree.
		secondChild.release(new Throwable("Release for testing"));
		assertEquals(rp1, leftMultiTaskSlot.getReservedResources());
		assertEquals(rp1.merge(rp3), unresolvedRootSlot.getReservedResources());

		// Releases the third child in the right-side tree.
		thirdChild.release(new Throwable("Release for testing"));
		assertEquals(rp1, unresolvedRootSlot.getReservedResources());

		// Releases the first child in the left-side tree.
		firstChild.release(new Throwable("Release for testing"));
		assertEquals(ResourceProfile.ZERO, unresolvedRootSlot.getReservedResources());
	}

	@Test
	public void testGetResolvedSlotWithResourceConfigured() {
		ResourceProfile rp1 = ResourceProfile.fromResources(1.0, 100);
		ResourceProfile rp2 = ResourceProfile.fromResources(2.0, 200);
		ResourceProfile allocatedSlotRp = ResourceProfile.fromResources(5.0, 500);

		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
				new SlotRequestId(),
				CompletableFuture.completedFuture(
						new SimpleSlotContext(
								new AllocationID(),
								new LocalTaskManagerLocation(),
								0,
								new SimpleAckingTaskManagerGateway(),
								allocatedSlotRp)),
				new SlotRequestId());

		rootSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp1,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		Collection<SlotSelectionStrategy.SlotInfoAndResources> resolvedRoots =
			slotSharingManager.listResolvedRootSlotInfo(new AbstractID());
		assertEquals(1, resolvedRoots.size());
		assertEquals(allocatedSlotRp.subtract(rp1), resolvedRoots.iterator().next().getRemainingResources());

		rootSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp2,
				new SlotSharingGroupId(),
				Locality.LOCAL);
		resolvedRoots = slotSharingManager.listResolvedRootSlotInfo(new AbstractID());
		assertEquals(1, resolvedRoots.size());
		assertEquals(allocatedSlotRp.subtract(rp1).subtract(rp2), resolvedRoots.iterator().next().getRemainingResources());
	}

	@Test
	public void testHashEnoughResourceOfMultiTaskSlot() {
		ResourceProfile rp1 = ResourceProfile.fromResources(1.0, 100);
		ResourceProfile rp2 = ResourceProfile.fromResources(2.0, 200);
		ResourceProfile allocatedSlotRp = ResourceProfile.fromResources(2.0, 200);

		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();

		SlotSharingManager.MultiTaskSlot unresolvedRootSlot = slotSharingManager.createRootSlot(
				new SlotRequestId(),
				slotContextFuture,
				new SlotRequestId());

		SlotSharingManager.MultiTaskSlot multiTaskSlot =
				unresolvedRootSlot.allocateMultiTaskSlot(new SlotRequestId(), new SlotSharingGroupId());

		SlotSharingManager.SingleTaskSlot firstChild = multiTaskSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				rp1,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(rp1), is(true));
		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(rp2), is(true));
		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(ResourceProfile.UNKNOWN), is(true));

		slotContextFuture.complete(new AllocatedSlot(
				new AllocationID(),
				new TaskManagerLocation(new ResourceID("tm-X"), InetAddress.getLoopbackAddress(), 46),
				0,
				allocatedSlotRp,
				mock(TaskManagerGateway.class)));

		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(rp1), is(true));
		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(rp2), is(false));
		assertThat(multiTaskSlot.mayHaveEnoughResourcesToFulfill(ResourceProfile.UNKNOWN), is(true));
	}

	@Test
	public void testSlotAllocatedWithEnoughResource() {
		SlotSharingResourceTestContext context = createResourceTestContext(ResourceProfile.fromResources(16.0, 1600));

		// With enough resources, all the requests should be fulfilled.
		for (SlotSharingManager.SingleTaskSlot singleTaskSlot : context.singleTaskSlotsInOrder) {
			assertThat(singleTaskSlot.getLogicalSlotFuture().isDone(), is(true));
			assertThat(singleTaskSlot.getLogicalSlotFuture().isCompletedExceptionally(), is(false));
		}

		// The multi-task slot for coLocation should be kept.
		assertThat(context.slotSharingManager.getTaskSlot(context.coLocationTaskSlot.getSlotRequestId()), notNullValue());
	}

	@Test
	public void testSlotOverAllocatedAndTaskSlotsReleased() {
		SlotSharingResourceTestContext context = createResourceTestContext(ResourceProfile.fromResources(7.0, 700));

		for (int i = 0; i < context.singleTaskSlotsInOrder.size(); ++i) {
			SlotSharingManager.SingleTaskSlot singleTaskSlot = context.singleTaskSlotsInOrder.get(i);
			assertThat(singleTaskSlot.getLogicalSlotFuture().isDone(), is(true));

			assertThat(singleTaskSlot.getLogicalSlotFuture().isCompletedExceptionally(), is(true));
			singleTaskSlot.getLogicalSlotFuture().whenComplete((LogicalSlot ignored, Throwable throwable) -> {
				assertThat(throwable, instanceOf(IllegalStateException.class));
			});
		}

		// All the task slots should be removed.
		assertThat(context.slotSharingManager.isEmpty(), is(true));
	}

	private SlotSharingResourceTestContext createResourceTestContext(ResourceProfile allocatedResourceProfile) {
		ResourceProfile coLocationTaskRp = ResourceProfile.fromResources(2.0, 200);
		ResourceProfile thirdChildRp = ResourceProfile.fromResources(3.0, 300);
		ResourceProfile forthChildRp = ResourceProfile.fromResources(9.0, 900);

		SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		CompletableFuture<SlotContext> slotContextFuture = new CompletableFuture<>();

		SlotSharingManager.MultiTaskSlot unresolvedRootSlot = slotSharingManager.createRootSlot(
				new SlotRequestId(),
				slotContextFuture,
				new SlotRequestId());

		SlotSharingManager.MultiTaskSlot coLocationTaskSlot = unresolvedRootSlot.allocateMultiTaskSlot(
				new SlotRequestId(), new SlotSharingGroupId());

		SlotSharingManager.SingleTaskSlot firstCoLocatedChild = coLocationTaskSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				coLocationTaskRp,
				new SlotSharingGroupId(),
				Locality.LOCAL);
		SlotSharingManager.SingleTaskSlot secondCoLocatedChild = coLocationTaskSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				coLocationTaskRp,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		SlotSharingManager.SingleTaskSlot thirdChild = unresolvedRootSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				thirdChildRp,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		SlotSharingManager.SingleTaskSlot forthChild = unresolvedRootSlot.allocateSingleTaskSlot(
				new SlotRequestId(),
				forthChildRp,
				new SlotSharingGroupId(),
				Locality.LOCAL);

		slotContextFuture.complete(new AllocatedSlot(
				new AllocationID(),
				new TaskManagerLocation(new ResourceID("tm-X"), InetAddress.getLoopbackAddress(), 46),
				0,
				allocatedResourceProfile,
				mock(TaskManagerGateway.class)));

		return new SlotSharingResourceTestContext(
				slotSharingManager,
				coLocationTaskSlot,
				Arrays.asList(firstCoLocatedChild, secondCoLocatedChild, thirdChild, forthChild));
	}

	private SlotSharingManager createTestingSlotSharingManager() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();

		return new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);
	}

	/**
	 * An utility class maintains the testing sharing slot hierarchy.
	 */
	private class SlotSharingResourceTestContext {
		final SlotSharingManager slotSharingManager;
		final SlotSharingManager.MultiTaskSlot coLocationTaskSlot;
		final List<SlotSharingManager.SingleTaskSlot> singleTaskSlotsInOrder;

		SlotSharingResourceTestContext(
				@Nonnull SlotSharingManager slotSharingManager,
				@Nonnull SlotSharingManager.MultiTaskSlot coLocationTaskSlot,
				@Nonnull List<SlotSharingManager.SingleTaskSlot> singleTaskSlotsInOrder) {

			this.slotSharingManager = slotSharingManager;
			this.coLocationTaskSlot = coLocationTaskSlot;
			this.singleTaskSlotsInOrder = singleTaskSlotsInOrder;
		}
	}

	@Test
	public void testTaskExecutorUtilizationCalculation() {
		final TestingAllocatedSlotActions allocatedSlotActions = new TestingAllocatedSlotActions();
		final TaskManagerLocation firstTaskExecutorLocation = new LocalTaskManagerLocation();
		final TaskManagerLocation secondTaskExecutorLocation = new LocalTaskManagerLocation();

		SlotSharingManager slotSharingManager = new SlotSharingManager(
			SLOT_SHARING_GROUP_ID,
			allocatedSlotActions,
			SLOT_OWNER);

		final SlotSharingManager.MultiTaskSlot firstRootSlot = createRootSlot(firstTaskExecutorLocation, slotSharingManager);
		createRootSlot(firstTaskExecutorLocation, slotSharingManager);
		createRootSlot(secondTaskExecutorLocation, slotSharingManager);

		final AbstractID groupId = new AbstractID();

		firstRootSlot.allocateSingleTaskSlot(new SlotRequestId(), ResourceProfile.UNKNOWN, groupId, Locality.UNCONSTRAINED);

		final Collection<SlotSelectionStrategy.SlotInfoAndResources> slotInfoAndResources = slotSharingManager.listResolvedRootSlotInfo(groupId);

		assertThat(slotInfoAndResources, hasSize(2));

		final Map<TaskManagerLocation, Double> utilizationPerTaskExecutor = slotInfoAndResources.stream()
			.collect(
				Collectors.toMap(
					slot -> slot.getSlotInfo().getTaskManagerLocation(),
					SlotSelectionStrategy.SlotInfoAndResources::getTaskExecutorUtilization));

		assertThat(utilizationPerTaskExecutor.get(firstTaskExecutorLocation), is(closeTo(1.0 / 2, 0.1)));
		assertThat(utilizationPerTaskExecutor.get(secondTaskExecutorLocation), is(closeTo(0, 0.1)));
	}

	@Test
	public void shouldResolveRootSlotBeforeCompletingChildSlots() {
		final SlotSharingManager slotSharingManager = createTestingSlotSharingManager();

		final CompletableFuture<SlotContext> slotFuture = new CompletableFuture<>();
		// important to add additional completion stage in order to reverse the execution order of callbacks
		final CompletableFuture<SlotContext> identityFuture = slotFuture.thenApply(Function.identity());

		final SlotSharingManager.MultiTaskSlot rootSlot = slotSharingManager.createRootSlot(
			new SlotRequestId(),
			identityFuture,
			new SlotRequestId());

		final SlotSharingManager.SingleTaskSlot singleTaskSlot = rootSlot.allocateSingleTaskSlot(
			new SlotRequestId(),
			ResourceProfile.UNKNOWN,
			new AbstractID(),
			Locality.UNCONSTRAINED);

		final CompletableFuture<Void> assertionFuture = singleTaskSlot.getLogicalSlotFuture().thenRun(() -> assertThat(
			slotSharingManager.getResolvedRootSlots(),
			contains(rootSlot)));

		slotFuture.complete(createSimpleSlotContext());

		assertionFuture.join();
	}

	private SlotSharingManager.MultiTaskSlot createRootSlot(TaskManagerLocation firstTaskExecutorLocation, SlotSharingManager slotSharingManager) {
		return slotSharingManager.createRootSlot(
			new SlotRequestId(),
			CompletableFuture.completedFuture(
				new SimpleSlotContext(
					new AllocationID(),
					firstTaskExecutorLocation,
					0,
					new SimpleAckingTaskManagerGateway())),
			new SlotRequestId());
	}
}
