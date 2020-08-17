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
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for {@link BulkSlotProviderImpl}.
 */
public class BulkSlotProviderImplTest extends TestLogger {

	private static final Time TIMEOUT = Time.milliseconds(1000L);

	private static ScheduledExecutorService singleThreadScheduledExecutorService;

	private static ComponentMainThreadExecutor mainThreadExecutor;

	private TestingSlotPoolImpl slotPool;

	private BulkSlotProviderImpl bulkSlotProvider;

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

		slotPool = new SlotPoolBuilder(mainThreadExecutor).build();

		bulkSlotProvider = new BulkSlotProviderImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		bulkSlotProvider.start(mainThreadExecutor);
	}

	@After
	public void teardown() {
		CompletableFuture.runAsync(() -> slotPool.close(), mainThreadExecutor).join();
	}

	@Test
	public void testBulkSlotAllocationFulfilledWithAvailableSlots() throws Exception {
		final PhysicalSlotRequest request1 = createPhysicalSlotRequest();
		final PhysicalSlotRequest request2 = createPhysicalSlotRequest();
		final List<PhysicalSlotRequest> requests = Arrays.asList(request1, request2);

		addSlotToSlotPool();
		addSlotToSlotPool();

		final CompletableFuture<Collection<PhysicalSlotRequest.Result>> slotFutures = allocateSlots(requests);

		final Collection<PhysicalSlotRequest.Result> results = slotFutures.get();
		final Collection<SlotRequestId> resultRequestIds = results.stream()
			.map(PhysicalSlotRequest.Result::getSlotRequestId)
			.collect(Collectors.toList());

		assertThat(resultRequestIds, containsInAnyOrder(request1.getSlotRequestId(), request2.getSlotRequestId()));
	}

	@Test
	public void testBulkSlotAllocationFulfilledWithNewSlots() throws ExecutionException, InterruptedException {
		final List<PhysicalSlotRequest> requests = Arrays.asList(
			createPhysicalSlotRequest(),
			createPhysicalSlotRequest());
		final CompletableFuture<Collection<PhysicalSlotRequest.Result>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		assertThat(slotFutures.isDone(), is(false));

		addSlotToSlotPool();

		slotFutures.get();
	}

	@Test
	public void testBulkSlotAllocationTimeoutsIfUnfulfillable() {
		final Exception exception = allocateSlotsAndWaitForTimeout();

		final Optional<Throwable> cause = ExceptionUtils.findThrowableWithMessage(
			exception,
			"Slot request bulk is not fulfillable!");
		assertThat(cause.isPresent(), is(true));
		assertThat(cause.get(), instanceOf(TimeoutException.class));
	}

	@Test
	public void testFailedBulkSlotAllocationReleasesAllocatedSlot() {
		allocateSlotsAndWaitForTimeout();

		assertThat(slotPool.getAllocatedSlots().listSlotInfo(), hasSize(0));
	}

	@Test
	public void testFailedBulkSlotAllocationClearsPendingRequests() {
		allocateSlotsAndWaitForTimeout();

		assertThat(slotPool.getPendingRequests().values(), hasSize(0));
	}

	private Exception allocateSlotsAndWaitForTimeout() {
		final List<PhysicalSlotRequest> requests = Arrays.asList(
			createPhysicalSlotRequest(),
			createPhysicalSlotRequest());
		final CompletableFuture<Collection<PhysicalSlotRequest.Result>> slotFutures = allocateSlots(requests);

		addSlotToSlotPool();

		assertThat(slotPool.getAllocatedSlots().listSlotInfo(), hasSize(1));

		clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);

		try {
			// wait util the requests timed out
			slotFutures.get();

			fail("Expected that the slot futures time out.");
			return new Exception("Unexpected");
		} catch (Exception e) {
			// expected
			return e;
		}
	}

	@Test
	public void testIndividualBatchSlotRequestTimeoutCheckIsDisabledOnAllocatingNewSlots() {
		assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(true));

		allocateSlots(Collections.singletonList(createPhysicalSlotRequest()));

		// drain the main thread tasks to ensure BulkSlotProviderImpl#requestNewSlot() to have been invoked
		drainMainThreadExecutorTasks();

		assertThat(slotPool.isBatchSlotRequestTimeoutCheckEnabled(), is(false));
	}

	private static void drainMainThreadExecutorTasks() {
		CompletableFuture.runAsync(() -> {}, mainThreadExecutor).join();
	}

	private void addSlotToSlotPool() {
		SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Collections.singletonList(ResourceProfile.ANY));
	}

	private CompletableFuture<Collection<PhysicalSlotRequest.Result>> allocateSlots(
			final Collection<PhysicalSlotRequest> requests) {

		return CompletableFuture
			.supplyAsync(
				() -> bulkSlotProvider.allocatePhysicalSlots(
					requests,
					TIMEOUT),
				mainThreadExecutor)
			.thenCompose(Function.identity());
	}

	private static PhysicalSlotRequest createPhysicalSlotRequest() {
		return new PhysicalSlotRequest(
			new SlotRequestId(),
			SlotProfile.noLocality(ResourceProfile.UNKNOWN),
			false);
	}
}
