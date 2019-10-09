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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SlotPoolImpl}.
 */
public class SlotPoolImplTest extends TestLogger {

	private final Time timeout = Time.seconds(10L);

	private JobID jobId;

	private TaskManagerLocation taskManagerLocation;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	private ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	@Before
	public void setUp() throws Exception {
		this.jobId = new JobID();

		taskManagerLocation = new LocalTaskManagerLocation();
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	@Test
	public void testAllocateSimpleSlot() throws Exception {
		CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				requestId,
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));

			LogicalSlot slot = future.get(1, TimeUnit.SECONDS);
			assertTrue(future.isDone());
			assertTrue(slot.isAlive());
			assertEquals(taskManagerLocation, slot.getTaskManagerLocation());
		}
	}

	@Nonnull
	private SlotPoolImpl createSlotPoolImpl() {
		return new TestingSlotPoolImpl(jobId);
	}

	@Test
	public void testAllocationFulfilledByReturnedSlot() throws Exception {
		final ArrayBlockingQueue<SlotRequest> slotRequestQueue = new ArrayBlockingQueue<>(2);

		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> {
			while (!slotRequestQueue.offer(slotRequest)) {
				// noop
			}
		});

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);
			CompletableFuture<LogicalSlot> future2 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);

			assertFalse(future1.isDone());
			assertFalse(future2.isDone());

			final List<SlotRequest> slotRequests = new ArrayList<>(2);

			for (int i = 0; i < 2; i++) {
				slotRequests.add(slotRequestQueue.poll(timeout.toMilliseconds(), TimeUnit.MILLISECONDS));
			}

			final SlotOffer slotOffer = new SlotOffer(
				slotRequests.get(0).getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));

			LogicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());
			assertFalse(future2.isDone());

			// return this slot to pool
			slot1.releaseSlot();

			// second allocation fulfilled by previous slot returning
			LogicalSlot slot2 = future2.get(1, TimeUnit.SECONDS);
			assertTrue(future2.isDone());

			assertNotEquals(slot1, slot2);
			assertFalse(slot1.isAlive());
			assertTrue(slot2.isAlive());
			assertEquals(slot1.getTaskManagerLocation(), slot2.getTaskManagerLocation());
			assertEquals(slot1.getPhysicalSlotNumber(), slot2.getPhysicalSlotNumber());
			assertEquals(slot1.getAllocationId(), slot2.getAllocationId());
		}
	}

	@Test
	public void testAllocateWithFreeSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);
			assertFalse(future1.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));

			LogicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());

			// return this slot to pool
			slot1.releaseSlot();

			CompletableFuture<LogicalSlot> future2 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);

			// second allocation fulfilled by previous slot returning
			LogicalSlot slot2 = future2.get(1, TimeUnit.SECONDS);
			assertTrue(future2.isDone());

			assertNotEquals(slot1, slot2);
			assertFalse(slot1.isAlive());
			assertTrue(slot2.isAlive());
			assertEquals(slot1.getTaskManagerLocation(), slot2.getTaskManagerLocation());
			assertEquals(slot1.getPhysicalSlotNumber(), slot2.getPhysicalSlotNumber());
		}
	}

	@Test
	public void testOfferSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			final TaskManagerLocation invalidTaskManagerLocation = new LocalTaskManagerLocation();

			// slot from unregistered resource
			assertFalse(slotPool.offerSlot(invalidTaskManagerLocation, taskManagerGateway, slotOffer));

			final SlotOffer nonRequestedSlotOffer = new SlotOffer(
				new AllocationID(),
				0,
				DEFAULT_TESTING_PROFILE);

			// we'll also accept non requested slots
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, nonRequestedSlotOffer));

			// accepted slot
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			LogicalSlot slot = future.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertTrue(slot.isAlive());

			// duplicated offer with using slot
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			assertTrue(slot.isAlive());

			final SlotOffer anotherSlotOfferWithSameAllocationId = new SlotOffer(
				slotRequest.getAllocationId(),
				1,
				DEFAULT_TESTING_PROFILE);
			assertFalse(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, anotherSlotOfferWithSameAllocationId));

			TaskManagerLocation anotherTaskManagerLocation = new LocalTaskManagerLocation();
			assertFalse(slotPool.offerSlot(anotherTaskManagerLocation, taskManagerGateway, slotOffer));

			// duplicated offer with free slot
			slot.releaseSlot();
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			assertFalse(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, anotherSlotOfferWithSameAllocationId));
			assertFalse(slotPool.offerSlot(anotherTaskManagerLocation, taskManagerGateway, slotOffer));
		}
	}

	@Test
	public void testReleaseResource() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			CompletableFuture<LogicalSlot> future2 = scheduler.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				timeout);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));

			LogicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());
			assertFalse(future2.isDone());

			final CompletableFuture<?> releaseFuture = new CompletableFuture<>();
			final DummyPayload dummyPayload = new DummyPayload(releaseFuture);

			slot1.tryAssignPayload(dummyPayload);

			slotPool.releaseTaskManager(taskManagerLocation.getResourceID(), null);

			releaseFuture.get();
			assertFalse(slot1.isAlive());

			// slot released and not usable, second allocation still not fulfilled
			Thread.sleep(10);
			assertFalse(future2.isDone());
		}
	}

	/**
	 * Tests that unused offered slots are directly used to fulfill pending slot
	 * requests.
	 *
	 * <p>Moreover it tests that the old slot request is canceled
	 *
	 * <p>See FLINK-8089, FLINK-8934
	 */
	@Test
	public void testFulfillingSlotRequestsWithUnusedOfferedSlots() throws Exception {

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(2);
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIds.offer(slotRequest.getAllocationId()));
			final ArrayBlockingQueue<AllocationID> canceledAllocations = new ArrayBlockingQueue<>(2);
			resourceManagerGateway.setCancelSlotConsumer(canceledAllocations::offer);
			final SlotRequestId slotRequestId1 = new SlotRequestId();
			final SlotRequestId slotRequestId2 = new SlotRequestId();
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			final Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);

			final ScheduledUnit scheduledUnit = new ScheduledUnit(
				new JobVertexID(),
				null,
				null);

			CompletableFuture<LogicalSlot> slotFuture1 = scheduler.allocateSlot(
				slotRequestId1,
				scheduledUnit,
				SlotProfile.noRequirements(),
				timeout);

			// wait for the first slot request
			final AllocationID allocationId1 = allocationIds.take();

			CompletableFuture<LogicalSlot> slotFuture2 = scheduler.allocateSlot(
				slotRequestId2,
				scheduledUnit,
				SlotProfile.noRequirements(),
				timeout);

			// wait for the second slot request
			final AllocationID allocationId2 = allocationIds.take();

			slotPool.releaseSlot(slotRequestId1, null);

			try {
				// this should fail with a CancellationException
				slotFuture1.get();
				fail("The first slot future should have failed because it was cancelled.");
			} catch (ExecutionException ee) {
				// expected
				assertTrue(ExceptionUtils.stripExecutionException(ee) instanceof FlinkException);
			}

			assertEquals(allocationId1, canceledAllocations.take());

			final SlotOffer slotOffer = new SlotOffer(allocationId1, 0, ResourceProfile.ANY);

			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));

			// the slot offer should fulfill the second slot request
			assertEquals(allocationId1, slotFuture2.get().getAllocationId());

			// check that the second slot allocation has been canceled
			assertEquals(allocationId2, canceledAllocations.take());
		}
	}

	/**
	 * Tests that a SlotPoolImpl shutdown releases all registered slots.
	 */
	@Test
	public void testShutdownReleasesAllSlots() throws Exception {

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);

			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			final int numSlotOffers = 2;

			final Collection<SlotOffer> slotOffers = new ArrayList<>(numSlotOffers);

			for (int i = 0; i < numSlotOffers; i++) {
				slotOffers.add(
					new SlotOffer(
						new AllocationID(),
						i,
						ResourceProfile.ANY));
			}

			final ArrayBlockingQueue<AllocationID> freedSlotQueue = new ArrayBlockingQueue<>(numSlotOffers);

			taskManagerGateway.setFreeSlotFunction(
				(AllocationID allocationID, Throwable cause) -> {
					try {
						freedSlotQueue.put(allocationID);
						return CompletableFuture.completedFuture(Acknowledge.get());
					} catch (InterruptedException e) {
						return FutureUtils.completedExceptionally(e);
					}
				});

			final Collection<SlotOffer> acceptedSlotOffers = slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

			assertThat(acceptedSlotOffers, Matchers.equalTo(slotOffers));

			// shut down the slot pool
			slotPool.close();

			// the shut down operation should have freed all registered slots
			ArrayList<AllocationID> freedSlots = new ArrayList<>(numSlotOffers);

			while (freedSlots.size() < numSlotOffers) {
				freedSlotQueue.drainTo(freedSlots);
			}

			assertThat(freedSlots, Matchers.containsInAnyOrder(slotOffers.stream().map(SlotOffer::getAllocationId).toArray()));
		}
	}

	@Test
	public void testCheckIdleSlot() throws Exception {
		final ManualClock clock = new ManualClock();

		try (TestingSlotPoolImpl slotPool = createSlotPoolImpl(clock)) {
			final BlockingQueue<AllocationID> freedSlots = new ArrayBlockingQueue<>(1);
			taskManagerGateway.setFreeSlotFunction(
				(AllocationID allocationId, Throwable cause) -> {
					try {
						freedSlots.put(allocationId);
						return CompletableFuture.completedFuture(Acknowledge.get());
					} catch (InterruptedException e) {
						return FutureUtils.completedExceptionally(e);
					}
				});

			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);

			final AllocationID expiredSlotID = new AllocationID();
			final AllocationID freshSlotID = new AllocationID();
			final SlotOffer slotToExpire = new SlotOffer(expiredSlotID, 0, ResourceProfile.ANY);
			final SlotOffer slotToNotExpire = new SlotOffer(freshSlotID, 1, ResourceProfile.ANY);

			assertThat(slotPool.registerTaskManager(taskManagerLocation.getResourceID()),
				Matchers.is(true));

			assertThat(
				slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToExpire),
				Matchers.is(true));

			clock.advanceTime(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToNotExpire),
				Matchers.is(true));

			clock.advanceTime(1L, TimeUnit.MILLISECONDS);

			slotPool.triggerCheckIdleSlot();

			final AllocationID freedSlot = freedSlots.poll(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(freedSlot, Matchers.is(expiredSlotID));
			assertThat(freedSlots.isEmpty(), Matchers.is(true));
		}
	}

	@Nonnull
	private TestingSlotPoolImpl createSlotPoolImpl(ManualClock clock) {
		return new TestingSlotPoolImpl(
			jobId,
			clock,
			TestingUtils.infiniteTime(),
			timeout,
			TestingUtils.infiniteTime());
	}

	/**
	 * Tests that idle slots which cannot be released will be discarded. See FLINK-11059.
	 */
	@Test
	public void testDiscardIdleSlotIfReleasingFailed() throws Exception {
		final ManualClock clock = new ManualClock();

		try (TestingSlotPoolImpl slotPool = createSlotPoolImpl(clock)) {

			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);

			final AllocationID expiredAllocationId = new AllocationID();
			final SlotOffer slotToExpire = new SlotOffer(expiredAllocationId, 0, ResourceProfile.ANY);

			OneShotLatch freeSlotLatch = new OneShotLatch();
			taskManagerGateway.setFreeSlotFunction((AllocationID allocationId, Throwable cause) -> {
				freeSlotLatch.trigger();
				return FutureUtils.completedExceptionally(new TimeoutException("Test failure"));
			});

			assertThat(slotPool.registerTaskManager(taskManagerLocation.getResourceID()), Matchers.is(true));

			assertThat(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToExpire), Matchers.is(true));

			clock.advanceTime(timeout.toMilliseconds() + 1, TimeUnit.MILLISECONDS);

			slotPool.triggerCheckIdleSlot();

			freeSlotLatch.await();

			CompletableFuture<LogicalSlot> allocatedSlotFuture = allocateSlot(scheduler, new SlotRequestId());

			try {
				// since the slot must have been discarded, we cannot fulfill the slot request
				allocatedSlotFuture.get(10L, TimeUnit.MILLISECONDS);
				fail("Expected to fail with a timeout.");
			} catch (TimeoutException ignored) {
				// expected
				assertEquals(0, slotPool.getAvailableSlots().size());
			}
		}
	}

	/**
	 * Tests that failed slots are freed on the {@link TaskExecutor}.
	 */
	@Test
	public void testFreeFailedSlots() throws Exception {

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {
			final int parallelism = 5;
			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(parallelism);
			resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));

			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);

			final Map<SlotRequestId, CompletableFuture<LogicalSlot>> slotRequestFutures = new HashMap<>(parallelism);

			for (int i = 0; i < parallelism; i++) {
				final SlotRequestId slotRequestId = new SlotRequestId();
				slotRequestFutures.put(slotRequestId, allocateSlot(scheduler, slotRequestId));
			}

			final List<SlotOffer> slotOffers = new ArrayList<>(parallelism);

			for (int i = 0; i < parallelism; i++) {
				slotOffers.add(new SlotOffer(allocationIds.take(), i, ResourceProfile.ANY));
			}

			slotPool.registerTaskManager(taskManagerLocation.getResourceID());
			slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

			// wait for the completion of both slot futures
			FutureUtils.waitForAll(slotRequestFutures.values()).get();

			final ArrayBlockingQueue<AllocationID> freedSlots = new ArrayBlockingQueue<>(1);
			taskManagerGateway.setFreeSlotFunction(
				(allocationID, throwable) -> {
					freedSlots.offer(allocationID);
					return CompletableFuture.completedFuture(Acknowledge.get());
				});

			final FlinkException failException = new FlinkException("Test fail exception");
			// fail allocations one by one
			for (int i = 0; i < parallelism - 1; i++) {
				final SlotOffer slotOffer = slotOffers.get(i);
				Optional<ResourceID> emptyTaskExecutorFuture =
					slotPool.failAllocation(slotOffer.getAllocationId(), failException);

				assertThat(emptyTaskExecutorFuture.isPresent(), is(false));
				assertThat(freedSlots.take(), is(equalTo(slotOffer.getAllocationId())));
			}

			final SlotOffer slotOffer = slotOffers.get(parallelism - 1);
			final Optional<ResourceID> emptyTaskExecutorFuture = slotPool.failAllocation(
				slotOffer.getAllocationId(),
				failException);
			assertThat(emptyTaskExecutorFuture.get(), is(equalTo(taskManagerLocation.getResourceID())));
			assertThat(freedSlots.take(), is(equalTo(slotOffer.getAllocationId())));
		}
	}

	/**
	 * Tests that create report of allocated slots on a {@link TaskExecutor}.
	 */
	@Test
	public void testCreateAllocatedSlotReport() throws Exception {

		try (SlotPoolImpl slotPool = createSlotPoolImpl()) {

			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(1);
			resourceManagerGateway.setRequestSlotConsumer(
					slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));

			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);
			Scheduler scheduler = setupScheduler(slotPool, mainThreadExecutor);

			final SlotRequestId slotRequestId = new SlotRequestId();
			final CompletableFuture<LogicalSlot> slotRequestFuture = allocateSlot(scheduler, slotRequestId);

			final List<AllocatedSlotInfo> allocatedSlotInfos = new ArrayList<>(2);
			final List<SlotOffer> slotOffers = new ArrayList<>(2);

			final AllocationID allocatedId = allocationIds.take();
			slotOffers.add(new SlotOffer(allocatedId, 0, ResourceProfile.ANY));
			allocatedSlotInfos.add(new AllocatedSlotInfo(0, allocatedId));

			final AllocationID availableId = new AllocationID();
			slotOffers.add(new SlotOffer(availableId, 1, ResourceProfile.ANY));
			allocatedSlotInfos.add(new AllocatedSlotInfo(1, availableId));

			slotPool.registerTaskManager(taskManagerLocation.getResourceID());
			slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

			// wait for the completion of slot future
			slotRequestFuture.get();

			final AllocatedSlotReport slotReport = slotPool.createAllocatedSlotReport(taskManagerLocation.getResourceID());
			assertThat(jobId, is(slotReport.getJobId()));
			assertThat(slotReport.getAllocatedSlotInfos(), containsInAnyOrder(isEachEqual(allocatedSlotInfos)));
		}
	}

	@Test
	public void testCalculationOfTaskExecutorUtilization() throws Exception {
		try (final SlotPoolImpl slotPool = createSlotPoolImpl()) {
			setupSlotPool(slotPool, resourceManagerGateway, mainThreadExecutor);

			final TaskManagerLocation firstTaskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerLocation secondTaskManagerLocation = new LocalTaskManagerLocation();

			final List<AllocationID> firstTaskManagersSlots = registerAndOfferSlots(firstTaskManagerLocation, slotPool, 4);
			final List<AllocationID> secondTaskManagersSlots = registerAndOfferSlots(secondTaskManagerLocation, slotPool, 4);

			slotPool.allocateAvailableSlot(new SlotRequestId(), firstTaskManagersSlots.get(0));
			slotPool.allocateAvailableSlot(new SlotRequestId(), firstTaskManagersSlots.get(1));
			slotPool.allocateAvailableSlot(new SlotRequestId(), secondTaskManagersSlots.get(3));

			final Collection<SlotInfoWithUtilization> availableSlotsInformation = slotPool.getAvailableSlotsInformation();

			final Map<TaskManagerLocation, Double> utilizationPerTaskExecutor = ImmutableMap.of(
				firstTaskManagerLocation, 2.0 / 4,
				secondTaskManagerLocation, 1.0 / 4);

			for (SlotInfoWithUtilization slotInfoWithUtilization : availableSlotsInformation) {
				final double expectedTaskExecutorUtilization = utilizationPerTaskExecutor.get(slotInfoWithUtilization.getTaskManagerLocation());
				assertThat(slotInfoWithUtilization.getTaskExecutorUtilization(), is(closeTo(expectedTaskExecutorUtilization, 0.1)));
			}
		}
	}

	private List<AllocationID> registerAndOfferSlots(TaskManagerLocation taskManagerLocation, SlotPoolImpl slotPool, int numberOfSlotsToRegister) {
		slotPool.registerTaskManager(taskManagerLocation.getResourceID());
		final List<AllocationID> allocationIds = IntStream.range(0, numberOfSlotsToRegister)
			.mapToObj(ignored -> new AllocationID())
			.collect(Collectors.toList());

		Collection<SlotOffer> slotOffers = IntStream.range(0, numberOfSlotsToRegister)
			.mapToObj(index -> new SlotOffer(allocationIds.get(index), index, ResourceProfile.ANY))
			.collect(Collectors.toList());

		slotPool.offerSlots(
			taskManagerLocation,
			new SimpleAckingTaskManagerGateway(),
			slotOffers);

		return allocationIds;
	}

	private static Collection<Matcher<? super AllocatedSlotInfo>> isEachEqual(Collection<AllocatedSlotInfo> allocatedSlotInfos) {
		return allocatedSlotInfos
			.stream()
			.map(SlotPoolImplTest::isEqualAllocatedSlotInfo)
			.collect(Collectors.toList());
	}

	private static Matcher<AllocatedSlotInfo> isEqualAllocatedSlotInfo(AllocatedSlotInfo expectedAllocatedSlotInfo) {
		return new TypeSafeDiagnosingMatcher<AllocatedSlotInfo>() {
			@Override
			public void describeTo(Description description) {
				description.appendText(describeAllocatedSlotInformation(expectedAllocatedSlotInfo));
			}

			private String describeAllocatedSlotInformation(AllocatedSlotInfo expectedAllocatedSlotInformation) {
				return expectedAllocatedSlotInformation.toString();
			}

			@Override
			protected boolean matchesSafely(AllocatedSlotInfo item, Description mismatchDescription) {
				final boolean matches = item.getAllocationId().equals(expectedAllocatedSlotInfo.getAllocationId()) &&
					item.getSlotIndex() == expectedAllocatedSlotInfo.getSlotIndex();

				if (!matches) {
					mismatchDescription
						.appendText("Actual value ")
						.appendText(describeAllocatedSlotInformation(item))
						.appendText(" differs from expected value ")
						.appendText(describeAllocatedSlotInformation(expectedAllocatedSlotInfo));
				}

				return matches;
			}
		};
	}

	private CompletableFuture<LogicalSlot> allocateSlot(Scheduler scheduler, SlotRequestId slotRequestId) {
		return scheduler.allocateSlot(
			slotRequestId,
			new DummyScheduledUnit(),
			SlotProfile.noRequirements(),
			timeout);
	}

	private static void setupSlotPool(
		SlotPoolImpl slotPool,
		ResourceManagerGateway resourceManagerGateway,
		ComponentMainThreadExecutor mainThreadExecutable) throws Exception {
		final String jobManagerAddress = "foobar";

		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutable);

		slotPool.connectToResourceManager(resourceManagerGateway);
	}

	private static Scheduler setupScheduler(
		SlotPool slotPool,
		ComponentMainThreadExecutor mainThreadExecutable) {
		Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutable);
		return scheduler;
	}
}
