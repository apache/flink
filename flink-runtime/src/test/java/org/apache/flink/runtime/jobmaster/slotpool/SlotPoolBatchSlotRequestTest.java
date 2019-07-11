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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

/**
 * Tests for batch slot requests.
 */
public class SlotPoolBatchSlotRequestTest extends TestLogger {

	private static final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1024);
	private static final ResourceProfile smallerResourceProfile = new ResourceProfile(0.5, 512);
	public static final CompletableFuture[] COMPLETABLE_FUTURES_EMPTY_ARRAY = new CompletableFuture[0];
	private static ScheduledExecutorService singleThreadScheduledExecutorService;
	private static ComponentMainThreadExecutor mainThreadExecutor;

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

	/**
	 * Tests that a batch slot request fails if there is no slot which can fulfill the
	 * slot request.
	 */
	@Test
	public void testPendingBatchSlotRequestTimeout() throws Exception {
		try (final SlotPoolImpl slotPool = new SlotPoolBuilder()
				.build()) {
			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedBatchSlot(
				slotPool,
				mainThreadExecutor,
				ResourceProfile.UNKNOWN);

			try {
				slotFuture.get();
				fail("Expected that slot future times out.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TimeoutException.class));
			}
		}
	}

	/**
	 * Tests that a batch slot request won't time out if there exists a slot in the
	 * SlotPool which fulfills the requested {@link ResourceProfile}.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotTimeoutIfFulfillingSlotExists() throws Exception {
		final Time batchSlotTimeout = Time.milliseconds(2L);
		final ComponentMainThreadExecutor directMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
		final ManualClock clock = new ManualClock();

		try (final SlotPoolImpl slotPool = new SlotPoolBuilder()
				.setComponentMainThreadExecutor(directMainThreadExecutor)
				.setClock(clock)
				.setBatchSlotTimeout(batchSlotTimeout)
				.build()) {

			offerSlots(slotPool, directMainThreadExecutor, Collections.singletonList(resourceProfile));

			final CompletableFuture<PhysicalSlot> firstSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, resourceProfile);
			final CompletableFuture<PhysicalSlot> secondSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, ResourceProfile.UNKNOWN);
			final CompletableFuture<PhysicalSlot> thirdSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, smallerResourceProfile);

			final List<CompletableFuture<PhysicalSlot>> slotFutures = Arrays.asList(firstSlotFuture, secondSlotFuture, thirdSlotFuture);
			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
				assertThat(slotFuture.isDone(), is(false));
			}
		}

	}

	/**
	 * Tests that a batch slot request does not react to {@link SlotPool#failAllocation(AllocationID, Exception)}
	 * signals.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotFailIfAllocationFails() throws Exception {
		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
		testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		final ComponentMainThreadExecutor directMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

		final Time batchSlotTimeout = Time.milliseconds(1000L);
		try (final SlotPoolImpl slotPool = new SlotPoolBuilder()
			.setComponentMainThreadExecutor(directMainThreadExecutor)
			.setBatchSlotTimeout(batchSlotTimeout)
			.setResourceManagerGateway(testingResourceManagerGateway)
			.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, resourceProfile);

			failAllocation(slotPool, directMainThreadExecutor, allocationIdFuture.get());

			assertThat(slotFuture.isDone(), is(false));
		}
	}

	/**
	 * Tests that a batch slot request won't fail if its resource manager request fails.
	 */
	@Test
	public void testPendingBatchSlotRequestDoesNotFailIfRMRequestFails() throws Exception {
		final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();
		testingResourceManagerGateway.setRequestSlotFuture(FutureUtils.completedExceptionally(new FlinkException("Failed request")));

		final ComponentMainThreadExecutor directMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();

		final Time batchSlotTimeout = Time.milliseconds(1000L);
		try (final SlotPoolImpl slotPool = new SlotPoolBuilder()
			.setComponentMainThreadExecutor(directMainThreadExecutor)
			.setBatchSlotTimeout(batchSlotTimeout)
			.setResourceManagerGateway(testingResourceManagerGateway)
			.build()) {

			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, resourceProfile);

			assertThat(slotFuture.isDone(), is(false));
		}
	}

	/**
	 * Tests that a pending batch slot request times out after the last fulfilling slot gets
	 * released.
	 */
	@Test
	public void testPendingBatchSlotRequestTimeoutAfterSlotRelease() throws Exception {
		final ComponentMainThreadExecutor directMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
		final ManualClock clock = new ManualClock();
		final Time batchSlotTimeout = Time.milliseconds(1000L);

		try (final SlotPoolImpl slotPool = new SlotPoolBuilder()
				.setComponentMainThreadExecutor(directMainThreadExecutor)
				.setClock(clock)
				.setBatchSlotTimeout(batchSlotTimeout)
				.build()) {
			final ResourceID taskManagerResourceId = offerSlots(slotPool, directMainThreadExecutor, Collections.singletonList(resourceProfile));
			final CompletableFuture<PhysicalSlot> firstSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, resourceProfile);
			final CompletableFuture<PhysicalSlot> secondSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, ResourceProfile.UNKNOWN);
			final CompletableFuture<PhysicalSlot> thirdSlotFuture = requestNewAllocatedBatchSlot(slotPool, directMainThreadExecutor, smallerResourceProfile);

			final List<CompletableFuture<PhysicalSlot>> slotFutures = Arrays.asList(firstSlotFuture, secondSlotFuture, thirdSlotFuture);

			// initial batch slot timeout check
			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			assertThat(CompletableFuture.anyOf(slotFutures.toArray(COMPLETABLE_FUTURES_EMPTY_ARRAY)).isDone(), is(false));

			releaseTaskManager(slotPool, directMainThreadExecutor, taskManagerResourceId);

			advanceTimeAndTriggerCheckBatchSlotTimeout(slotPool, clock, batchSlotTimeout);

			for (CompletableFuture<PhysicalSlot> slotFuture : slotFutures) {
				assertThat(slotFuture.isCompletedExceptionally(), is(true));

				try {
					slotFuture.get();
					fail("Expected that the slot future times out.");
				} catch (ExecutionException ee) {
					assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TimeoutException.class));
				}
			}
		}
	}

	private void advanceTimeAndTriggerCheckBatchSlotTimeout(SlotPoolImpl slotPool, ManualClock clock, Time batchSlotTimeout) {
		// trigger batch slot timeout check which marks unfulfillable slots
		slotPool.triggerCheckBatchSlotTimeout();

		// advance clock behind timeout
		clock.advanceTime(batchSlotTimeout.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);

		// timeout all as unfulfillable marked slots
		slotPool.triggerCheckBatchSlotTimeout();
	}

	private CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
			SlotPool slotPool,
			ComponentMainThreadExecutor mainThreadExecutor,
			ResourceProfile resourceProfile) {
		return CompletableFuture
			.supplyAsync(() -> slotPool.requestNewAllocatedBatchSlot(new SlotRequestId(), resourceProfile), mainThreadExecutor)
			.thenCompose(Function.identity());
	}

	private ResourceID offerSlots(SlotPoolImpl slotPool, ComponentMainThreadExecutor mainThreadExecutor, List<ResourceProfile> resourceProfiles) {
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		CompletableFuture.runAsync(
			() -> {
				slotPool.registerTaskManager(taskManagerLocation.getResourceID());

				final Collection<SlotOffer> slotOffers = IntStream
					.range(0, resourceProfiles.size())
					.mapToObj(i -> new SlotOffer(new AllocationID(), i, resourceProfiles.get(i)))
					.collect(Collectors.toList());

				final Collection<SlotOffer> acceptedOffers = slotPool.offerSlots(
					taskManagerLocation,
					new SimpleAckingTaskManagerGateway(),
					slotOffers);

				assertThat(acceptedOffers, is(slotOffers));
			},
			mainThreadExecutor
		).join();

		return taskManagerLocation.getResourceID();
	}

	private void failAllocation(SlotPoolImpl slotPool, ComponentMainThreadExecutor mainThreadExecutor, AllocationID allocationId) {
		CompletableFuture.runAsync(
			() -> slotPool.failAllocation(allocationId, new FlinkException("Test exception")),
			mainThreadExecutor).join();
	}

	private void releaseTaskManager(SlotPoolImpl slotPool, ComponentMainThreadExecutor mainThreadExecutor, ResourceID taskManagerResourceId) {
		CompletableFuture.runAsync(
			() -> slotPool.releaseTaskManager(taskManagerResourceId, new FlinkException("Let's get rid of the offered slot.")),
			mainThreadExecutor
		).join();
	}

	private static class SlotPoolBuilder {

		private ComponentMainThreadExecutor componentMainThreadExecutor = mainThreadExecutor;
		private ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		private Time batchSlotTimeout = Time.milliseconds(2L);
		private Clock clock = SystemClock.getInstance();

		private SlotPoolBuilder setComponentMainThreadExecutor(ComponentMainThreadExecutor componentMainThreadExecutor) {
			this.componentMainThreadExecutor = componentMainThreadExecutor;
			return this;
		}

		private SlotPoolBuilder setResourceManagerGateway(ResourceManagerGateway resourceManagerGateway) {
			this.resourceManagerGateway = resourceManagerGateway;
			return this;
		}

		private SlotPoolBuilder setBatchSlotTimeout(Time batchSlotTimeout) {
			this.batchSlotTimeout = batchSlotTimeout;
			return this;
		}

		private SlotPoolBuilder setClock(Clock clock) {
			this.clock = clock;
			return this;
		}

		private SlotPoolImpl build() throws Exception {
			final SlotPoolImpl slotPool = new SlotPoolImpl(
				new JobID(),
				clock,
				TestingUtils.infiniteTime(),
				TestingUtils.infiniteTime(),
				batchSlotTimeout);

			slotPool.start(JobMasterId.generate(), "foobar", componentMainThreadExecutor);

			CompletableFuture.runAsync(() -> slotPool.connectToResourceManager(resourceManagerGateway), componentMainThreadExecutor).join();

			return slotPool;
		}
	}
}
