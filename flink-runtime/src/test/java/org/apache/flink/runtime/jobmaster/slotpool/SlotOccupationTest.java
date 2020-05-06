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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests whether the slot occupation works correctly.
 */
public class SlotOccupationTest extends TestLogger {

	private static final Time TIMEOUT = Time.milliseconds(1L);

	private static ScheduledExecutorService singleThreadScheduledExecutorService;

	private static ComponentMainThreadExecutor mainThreadExecutor;

	private SlotPoolImpl slotPool;

	private Scheduler scheduler;

	@BeforeClass
	public static void setupClass() {
		singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadScheduledExecutorService);
	}

	@AfterClass
	public static void teardownClass() {
		if (singleThreadScheduledExecutorService != null) {
			singleThreadScheduledExecutorService.shutdownNow();
		}
	}

	@Before
	public void setup() throws Exception {
		slotPool = new SlotPoolBuilder(mainThreadExecutor).build();
		SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(ResourceProfile.ANY));

		scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);
	}

	@After
	public void teardown() {
		CompletableFuture.runAsync(() -> slotPool.close(), mainThreadExecutor).join();
	}

	@Test
	public void testSingleStreamingTaskSlotRequestWillOccupySlotIndefinitely() throws Exception {
		final CompletableFuture<LogicalSlot> slotFuture = allocateSlot(
			getSingleStreamingTaskSlotAllocationSupplier(scheduler),
			mainThreadExecutor);
		slotFuture.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());
		assertTrue(slot.willBeOccupiedIndefinitely());
	}

	@Test
	public void testSingleBatchTaskSlotRequestWillNotOccupySlotIndefinitely() throws Exception {
		final CompletableFuture<LogicalSlot> slotFuture = allocateSlot(
			getSingleBatchTaskSlotAllocationSupplier(scheduler),
			mainThreadExecutor);
		slotFuture.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());
		assertFalse(slot.willBeOccupiedIndefinitely());
	}

	@Test
	public void testSharedStreamingTaskSlotRequestWillOccupySlotIndefinitely() throws Exception {
		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();
		final CompletableFuture<LogicalSlot> slotFuture = allocateSlot(
			getSharedStreamingTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		slotFuture.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());
		assertTrue(slot.willBeOccupiedIndefinitely());
	}

	@Test
	public void testSharedBatchTaskSlotRequestWillNotOccupySlotIndefinitely() throws Exception {
		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();
		final CompletableFuture<LogicalSlot> slotFuture = allocateSlot(
			getSharedBatchTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		slotFuture.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());
		assertFalse(slot.willBeOccupiedIndefinitely());
	}

	@Test
	public void testSharedMixedTaskSlotRequestWillOccupySlotIndefinitely() throws Exception {
		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

		final CompletableFuture<LogicalSlot> slotFuture1 = allocateSlot(
			getSharedStreamingTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		slotFuture1.get();

		final CompletableFuture<LogicalSlot> slotFuture2 = allocateSlot(
			getSharedBatchTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		slotFuture2.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());
		assertTrue(slot.willBeOccupiedIndefinitely());
	}

	@Test
	public void testSharedSlotNotOccupiedOnAllHostedStreamingTaskSlotReleased() throws Exception {
		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

		final CompletableFuture<LogicalSlot> slotFuture1 = allocateSlot(
			getSharedStreamingTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		final LogicalSlot streamingTaskSlot1 = slotFuture1.get();

		final CompletableFuture<LogicalSlot> slotFuture2 = allocateSlot(
			getSharedStreamingTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		final LogicalSlot streamingTaskSlot2 = slotFuture2.get();

		final CompletableFuture<LogicalSlot> slotFuture3 = allocateSlot(
			getSharedBatchTaskSlotAllocationSupplier(scheduler, slotSharingGroupId),
			mainThreadExecutor);
		slotFuture3.get();

		final SlotInfo slot = Iterables.getOnlyElement(slotPool.getAllocatedSlots().listSlotInfo());

		CompletableFuture.runAsync(
			() -> streamingTaskSlot1.releaseSlot(new Exception("Force releasing slot")),
			mainThreadExecutor).join();
		assertTrue(slot.willBeOccupiedIndefinitely());

		CompletableFuture.runAsync(
			() -> streamingTaskSlot2.releaseSlot(new Exception("Force releasing slot")),
			mainThreadExecutor).join();
		assertFalse(slot.willBeOccupiedIndefinitely());
	}

	private static Supplier<CompletableFuture<LogicalSlot>> getSingleStreamingTaskSlotAllocationSupplier(
			final Scheduler scheduler) {
		return () -> scheduler.allocateSlot(
			new SlotRequestId(),
			new DummyScheduledUnit(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			TIMEOUT);
	}

	private static Supplier<CompletableFuture<LogicalSlot>> getSingleBatchTaskSlotAllocationSupplier(
			final Scheduler scheduler) {
		return () -> scheduler.allocateBatchSlot(
			new SlotRequestId(),
			new DummyScheduledUnit(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN));
	}

	private static Supplier<CompletableFuture<LogicalSlot>> getSharedStreamingTaskSlotAllocationSupplier(
			final Scheduler scheduler,
			final SlotSharingGroupId slotSharingGroupId) {
		return () -> scheduler.allocateSlot(
			new SlotRequestId(),
			new ScheduledUnit(new JobVertexID(), slotSharingGroupId, null),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			TIMEOUT);
	}

	private static Supplier<CompletableFuture<LogicalSlot>> getSharedBatchTaskSlotAllocationSupplier(
			final Scheduler scheduler,
			final SlotSharingGroupId slotSharingGroupId) {
		return () -> scheduler.allocateBatchSlot(
			new SlotRequestId(),
			new ScheduledUnit(new JobVertexID(), slotSharingGroupId, null),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN));
	}

	private static CompletableFuture<LogicalSlot> allocateSlot(
			final Supplier<CompletableFuture<LogicalSlot>> slotAllocationSupplier,
			final ComponentMainThreadExecutor mainThreadExecutor) {
		return CompletableFuture
			.supplyAsync(slotAllocationSupplier, mainThreadExecutor)
			.thenCompose(Function.identity());
	}
}
