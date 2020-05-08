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
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests whether bulk slot allocation works correctly.
 */
public class BulkSlotAllocationTest extends TestLogger {

	private static final Time TIMEOUT = Time.milliseconds(1L);

	private static ScheduledExecutorService singleThreadScheduledExecutorService;

	private static ComponentMainThreadExecutor mainThreadExecutor;

	private TestingSlotPoolImpl slotPool;

	private Scheduler scheduler;

	private ManualClock clock;

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
		clock = new ManualClock();

		slotPool = new SlotPoolBuilder(mainThreadExecutor).setClock(clock).build();

		scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);
	}

	@After
	public void teardown() {
		CompletableFuture.runAsync(() -> slotPool.close(), mainThreadExecutor).join();
	}

	@Test
	public void testFulfilledBulkSlotAllocation() {
		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		assertThat(slotFutures.isDone(), is(false));

		addSlotToSlotPool();

		assertThat(slotFutures.isDone(), is(true));
		assertThat(slotFutures.isCompletedExceptionally(), is(false));
	}

	@Test
	public void testFulfillableBulkSlotAllocation() {
		final LogicalSlot occupyingSlot = addTemporarilyOccupiedSlotToSlotPool();

		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		triggerSlotRequestTimeoutCheck();

		assertThat(slotFutures.isDone(), is(false));

		releaseSlot(occupyingSlot);

		// requests become fulfilled once the occupied slot has its payload released
		assertThat(slotFutures.isDone(), is(true));
	}

	@Test
	public void testUnfulfillableBulkSlotAllocationDueToInsufficientSlots() {
		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		triggerSlotRequestTimeoutCheck();

		assertThat(slotFutures.isCompletedExceptionally(), is(true));

		try {
			slotFutures.get();
			fail("Expected that the slot futures time out.");
		} catch (Exception e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			assertThat(cause, instanceOf(FlinkException.class));
			assertThat(cause.getMessage(), containsString("Bulk slot allocation failed."));
		}
	}

	@Test
	public void testUnfulfillableBulkSlotAllocationDueToOccupiedSlots() {
		addIndefinitelyOccupiedSlotToSlotPool();

		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		triggerSlotRequestTimeoutCheck();

		assertThat(slotFutures.isCompletedExceptionally(), is(true));

		try {
			slotFutures.get();
			fail("Expected that the slot futures time out.");
		} catch (Exception e) {
			final Throwable cause = ExceptionUtils.stripExecutionException(e);
			assertThat(cause, instanceOf(FlinkException.class));
			assertThat(cause.getMessage(), containsString("Bulk slot allocation failed."));
		}
	}

	@Test
	public void testFailedBulkSlotAllocationReleasesAllocatedSlot() {
		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		allocateSlots(requests);

		addSlotToSlotPool();

		assertThat(slotPool.getAllocatedSlots().listSlotInfo(), hasSize(1));

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		triggerSlotRequestTimeoutCheck();

		assertThat(slotPool.getAllocatedSlots().listSlotInfo(), hasSize(0));
	}

	@Test
	public void testFailedBulkSlotAllocationClearsPendingRequests() {
		final List<LogicalSlotRequest> requests = Arrays.asList(
			createLogicalSlotRequest(),
			createLogicalSlotRequest());
		allocateSlots(requests);

		addSlotToSlotPool();

		assertThat(slotPool.getPendingRequests().values(), hasSize(1));

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
		triggerSlotRequestTimeoutCheck();

		assertThat(slotPool.getPendingRequests().values(), hasSize(0));
	}

	private LogicalSlot addTemporarilyOccupiedSlotToSlotPool() {
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(Arrays.asList(createLogicalSlotRequest(false)));
		addSlotToSlotPool();

		return slotFutures.getNow(null).get(0);
	}

	private LogicalSlot addIndefinitelyOccupiedSlotToSlotPool() {
		final CompletableFuture<List<LogicalSlot>> slotFutures = allocateSlots(Arrays.asList(createLogicalSlotRequest()));
		addSlotToSlotPool();

		return slotFutures.getNow(null).get(0);
	}

	private void addSlotToSlotPool() {
		SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(ResourceProfile.ANY));
	}

	private CompletableFuture<List<LogicalSlot>> allocateSlots(final List<LogicalSlotRequest> requests) {
		return CompletableFuture
			.supplyAsync(
				() -> scheduler.allocateSlots(
					requests,
					TIMEOUT),
				mainThreadExecutor)
			.thenCompose(Function.identity());
	}

	private static void releaseSlot(final LogicalSlot slot) {
		CompletableFuture.runAsync(
			() -> slot.releaseSlot(new Exception("Force releasing slot")),
			mainThreadExecutor).join();
	}

	private static LogicalSlotRequest createLogicalSlotRequest() {
		return createLogicalSlotRequest(true);
	}

	private static LogicalSlotRequest createLogicalSlotRequest(final boolean slotWillBeOccupiedIndefinitely) {
		return new LogicalSlotRequest(
			new SlotRequestId(),
			new DummyScheduledUnit(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			slotWillBeOccupiedIndefinitely);
	}

	private void triggerSlotRequestTimeoutCheck() {
		slotPool.triggerAndWaitForPendingRequestsTimeoutCheck(TIMEOUT, mainThreadExecutor);
	}
}
