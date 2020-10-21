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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmaster.AllocatedSlotInfo;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Before;
import org.junit.Test;

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

import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils.requestNewAllocatedSlot;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils.requestNewAllocatedSlots;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SlotPoolImpl}.
 */
public class SlotPoolImplTest extends TestLogger {

	private static final Time TIMEOUT = SlotPoolUtils.TIMEOUT;

	private TaskManagerLocation taskManagerLocation;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	private static final ComponentMainThreadExecutor mainThreadExecutor =
		ComponentMainThreadExecutorServiceAdapter.forMainThread();

	@Before
	public void setUp() throws Exception {
		taskManagerLocation = new LocalTaskManagerLocation();
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	@Test
	public void testAllocateSimpleSlot() throws Exception {
		CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {

			final SlotRequestId requestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future = requestNewAllocatedSlot(slotPool, requestId);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertTrue(registerAndOfferSlot(taskManagerLocation, slotPool, slotRequest.getAllocationId()));

			final PhysicalSlot physicalSlot = future.get(1, TimeUnit.SECONDS);
			assertTrue(future.isDone());
			assertEquals(taskManagerLocation, physicalSlot.getTaskManagerLocation());
			assertEquals(slotRequest.getAllocationId(), physicalSlot.getAllocationId());
		}
	}

	@Test
	public void testAllocationFulfilledByReturnedSlot() throws Exception {
		final ArrayBlockingQueue<SlotRequest> slotRequestQueue = new ArrayBlockingQueue<>(2);

		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> {
			while (!slotRequestQueue.offer(slotRequest)) {
				// noop
			}
		});

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {

			final SlotRequestId requestId1 = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future1 = requestNewAllocatedSlot(
				slotPool,
				requestId1);
			final SlotRequestId requestId2 = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future2 = requestNewAllocatedSlot(
				slotPool,
				requestId2);

			assertFalse(future1.isDone());
			assertFalse(future2.isDone());

			final List<SlotRequest> slotRequests = new ArrayList<>(2);

			for (int i = 0; i < 2; i++) {
				slotRequests.add(slotRequestQueue.poll(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS));
			}

			assertTrue(registerAndOfferSlot(taskManagerLocation, slotPool, slotRequests.get(0).getAllocationId()));

			final PhysicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());
			assertFalse(future2.isDone());

			// return this slot to pool
			slotPool.releaseSlot(requestId1, null);

			// second allocation fulfilled by previous slot returning
			final PhysicalSlot slot2 = future2.get(1, TimeUnit.SECONDS);
			assertTrue(future2.isDone());

			assertEquals(slot1, slot2);
		}
	}

	@Test
	public void testAllocateWithFreeSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {

			final AllocationID allocationId = new AllocationID();
			assertTrue(registerAndOfferSlot(taskManagerLocation, slotPool, allocationId));

			assertEquals(1, slotPool.getAvailableSlots().size());
			assertEquals(0, slotPool.getAllocatedSlots().size());

			Optional<PhysicalSlot> physicalSlot = slotPool.allocateAvailableSlot(
				new SlotRequestId(),
				allocationId,
				ResourceProfile.ANY);

			assertTrue(physicalSlot.isPresent());
			assertEquals(0, slotPool.getAvailableSlots().size());
			assertEquals(1, slotPool.getAllocatedSlots().size());
		}
	}

	@Test
	public void testOfferSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			final SlotRequestId requestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future = requestNewAllocatedSlot(
				slotPool,
				requestId);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				ResourceProfile.ANY);

			final TaskManagerLocation invalidTaskManagerLocation = new LocalTaskManagerLocation();

			// slot from unregistered resource
			assertFalse(slotPool.offerSlot(invalidTaskManagerLocation, taskManagerGateway, slotOffer));

			final SlotOffer nonRequestedSlotOffer = new SlotOffer(
				new AllocationID(),
				0,
				ResourceProfile.ANY);

			// we'll also accept non requested slots
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, nonRequestedSlotOffer));
			assertEquals(1, slotPool.getAllocatedSlots().size());
			final PhysicalSlot slot = future.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertEquals(taskManagerLocation, slot.getTaskManagerLocation());
			assertEquals(nonRequestedSlotOffer.getAllocationId(), slot.getAllocationId());

			// accepted slot
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			assertEquals(1, slotPool.getAvailableSlots().size());

			// duplicated offer with using slot
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			assertEquals(1, slotPool.getAvailableSlots().size());
			assertEquals(1, slotPool.getAllocatedSlots().size());

			final SlotOffer anotherSlotOfferWithSameAllocationId = new SlotOffer(
				slotRequest.getAllocationId(),
				1,
				ResourceProfile.ANY);
			assertFalse(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, anotherSlotOfferWithSameAllocationId));

			TaskManagerLocation anotherTaskManagerLocation = new LocalTaskManagerLocation();
			assertFalse(slotPool.offerSlot(anotherTaskManagerLocation, taskManagerGateway, slotOffer));

			// duplicated offer with free slot
			slotPool.releaseSlot(requestId, null);
			assertTrue(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer));
			assertFalse(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, anotherSlotOfferWithSameAllocationId));
			assertFalse(slotPool.offerSlot(anotherTaskManagerLocation, taskManagerGateway, slotOffer));
		}
	}

	@Test
	public void testReleaseResource() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequestFuture::complete);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {

			final SlotRequestId requestId1 = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future1 = requestNewAllocatedSlot(
				slotPool,
				requestId1);

			final SlotRequest slotRequest = slotRequestFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			final CompletableFuture<PhysicalSlot> future2 = requestNewAllocatedSlot(
				slotPool,
				new SlotRequestId());

			assertTrue(registerAndOfferSlot(taskManagerLocation, slotPool, slotRequest.getAllocationId()));

			final PhysicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());
			assertFalse(future2.isDone());

			final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

			final SingleLogicalSlot logicalSlot = SingleLogicalSlot.allocateFromPhysicalSlot(
				requestId1,
				slot1,
				Locality.UNKNOWN,
				new DummySlotOwner(),
				true);

			logicalSlot.tryAssignPayload(new DummyPayload(releaseFuture));

			slotPool.releaseTaskManager(
				taskManagerLocation.getResourceID(),
				new Exception("Releasing TaskManager in SlotPool for tests"));

			releaseFuture.get();
			assertFalse(logicalSlot.isAlive());

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

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(2);
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIds.offer(slotRequest.getAllocationId()));
			final ArrayBlockingQueue<AllocationID> canceledAllocations = new ArrayBlockingQueue<>(2);
			resourceManagerGateway.setCancelSlotConsumer(canceledAllocations::offer);
			final SlotRequestId slotRequestId1 = new SlotRequestId();
			final SlotRequestId slotRequestId2 = new SlotRequestId();

			final CompletableFuture<PhysicalSlot> slotFuture1 = requestNewAllocatedSlot(
				slotPool,
				slotRequestId1);

			// wait for the first slot request
			final AllocationID allocationId1 = allocationIds.take();

			final CompletableFuture<PhysicalSlot> slotFuture2 = requestNewAllocatedSlot(
				slotPool,
				slotRequestId2);

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

			assertTrue(registerAndOfferSlot(taskManagerLocation, slotPool, allocationId1));

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

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
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
	public void testShutdownCancelsAllPendingRequests() throws Exception {
		final ArrayBlockingQueue<AllocationID> canceledAllocations = new ArrayBlockingQueue<>(2);
		resourceManagerGateway.setCancelSlotConsumer(canceledAllocations::offer);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			slotPool.requestNewAllocatedSlot(new SlotRequestId(), ResourceProfile.UNKNOWN, TIMEOUT);
			slotPool.requestNewAllocatedSlot(new SlotRequestId(), ResourceProfile.UNKNOWN, TIMEOUT);

			assertThat(slotPool.getPendingRequests().values(), hasSize(2));

			slotPool.close();

			assertThat(slotPool.getPendingRequests().values(), hasSize(0));
			assertThat(canceledAllocations, hasSize(2));
		}
	}

	@Test
	public void testCheckIdleSlot() throws Exception {
		final ManualClock clock = new ManualClock();

		try (TestingSlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway, clock, TIMEOUT)) {
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

			final AllocationID expiredSlotID = new AllocationID();
			final AllocationID freshSlotID = new AllocationID();
			final SlotOffer slotToExpire = new SlotOffer(expiredSlotID, 0, ResourceProfile.ANY);
			final SlotOffer slotToNotExpire = new SlotOffer(freshSlotID, 1, ResourceProfile.ANY);

			assertThat(slotPool.registerTaskManager(taskManagerLocation.getResourceID()),
				Matchers.is(true));

			assertThat(
				slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToExpire),
				Matchers.is(true));

			clock.advanceTime(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToNotExpire),
				Matchers.is(true));

			clock.advanceTime(1L, TimeUnit.MILLISECONDS);

			slotPool.triggerCheckIdleSlot();

			final AllocationID freedSlot = freedSlots.poll(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(freedSlot, Matchers.is(expiredSlotID));
			assertThat(freedSlots.isEmpty(), Matchers.is(true));
		}
	}

	/**
	 * Tests that idle slots which cannot be released will be discarded. See FLINK-11059.
	 */
	@Test
	public void testDiscardIdleSlotIfReleasingFailed() throws Exception {
		final ManualClock clock = new ManualClock();

		try (TestingSlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway, clock, TIMEOUT)) {

			final AllocationID expiredAllocationId = new AllocationID();
			final SlotOffer slotToExpire = new SlotOffer(expiredAllocationId, 0, ResourceProfile.ANY);

			OneShotLatch freeSlotLatch = new OneShotLatch();
			taskManagerGateway.setFreeSlotFunction((AllocationID allocationId, Throwable cause) -> {
				freeSlotLatch.trigger();
				return FutureUtils.completedExceptionally(new TimeoutException("Test failure"));
			});

			assertThat(slotPool.registerTaskManager(taskManagerLocation.getResourceID()), Matchers.is(true));

			assertThat(slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotToExpire), Matchers.is(true));

			clock.advanceTime(TIMEOUT.toMilliseconds() + 1, TimeUnit.MILLISECONDS);

			slotPool.triggerCheckIdleSlot();

			freeSlotLatch.await();

			final CompletableFuture<PhysicalSlot> allocatedSlotFuture = requestNewAllocatedSlot(
				slotPool,
				new SlotRequestId());

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

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			final int parallelism = 5;
			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(parallelism);
			resourceManagerGateway.setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));

			final Map<SlotRequestId, CompletableFuture<PhysicalSlot>> slotRequestFutures = new HashMap<>(parallelism);

			for (int i = 0; i < parallelism; i++) {
				final SlotRequestId slotRequestId = new SlotRequestId();
				slotRequestFutures.put(slotRequestId, requestNewAllocatedSlot(slotPool, slotRequestId));
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
		final JobID jobId = new JobID();

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway, jobId)) {

			final ArrayBlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(1);
			resourceManagerGateway.setRequestSlotConsumer(
					slotRequest -> allocationIds.offer(slotRequest.getAllocationId()));

			final CompletableFuture<PhysicalSlot> slotRequestFuture = requestNewAllocatedSlot(
				slotPool,
				new SlotRequestId());

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
		try (final SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			final TaskManagerLocation firstTaskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerLocation secondTaskManagerLocation = new LocalTaskManagerLocation();

			final List<AllocationID> firstTaskManagersSlots = registerAndOfferSlots(firstTaskManagerLocation, slotPool, 4);
			final List<AllocationID> secondTaskManagersSlots = registerAndOfferSlots(secondTaskManagerLocation, slotPool, 4);

			slotPool.allocateAvailableSlot(new SlotRequestId(), firstTaskManagersSlots.get(0), ResourceProfile.ANY);
			slotPool.allocateAvailableSlot(new SlotRequestId(), firstTaskManagersSlots.get(1), ResourceProfile.ANY);
			slotPool.allocateAvailableSlot(new SlotRequestId(), secondTaskManagersSlots.get(3), ResourceProfile.ANY);

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

	@Test
	public void testOrphanedAllocationCanBeRemapped() throws Exception {
		final List<AllocationID> allocationIds = new ArrayList<>();
		resourceManagerGateway.setRequestSlotConsumer(
			slotRequest -> allocationIds.add(slotRequest.getAllocationId()));

		final List<AllocationID> canceledAllocations = new ArrayList<>();
		resourceManagerGateway.setCancelSlotConsumer(canceledAllocations::add);

		try (SlotPoolImpl slotPool = createAndSetUpSlotPool(resourceManagerGateway)) {
			final SlotRequestId slotRequestId1 = new SlotRequestId();
			final SlotRequestId slotRequestId2 = new SlotRequestId();
			requestNewAllocatedSlots(slotPool, slotRequestId1, slotRequestId2);

			final AllocationID allocationId1 = allocationIds.get(0);
			final AllocationID allocationId2 = allocationIds.get(1);

			registerAndOfferSlot(taskManagerLocation, slotPool, allocationId2);

			// verify that orphaned allocationId2 is remapped to slotRequestId2
			assertThat(slotPool.getPendingRequests().values(), hasSize(1));
			assertThat(slotPool.getPendingRequests().containsKeyA(slotRequestId2), is(true));
			assertThat(slotPool.getPendingRequests().containsKeyB(allocationId1), is(true));
			assertThat(canceledAllocations, hasSize(0));
		}
	}

	@Test
	public void testOrphanedAllocationIsCanceledIfNotRemapped() throws Exception {
		final List<AllocationID> allocationIds = new ArrayList<>();
		resourceManagerGateway.setRequestSlotConsumer(
			slotRequest -> allocationIds.add(slotRequest.getAllocationId()));

		final List<AllocationID> canceledAllocations = new ArrayList<>();
		resourceManagerGateway.setCancelSlotConsumer(canceledAllocations::add);

		try (SlotPoolImpl slotPool = SlotPoolUtils.createAndSetUpSlotPool(resourceManagerGateway)) {
			final SlotRequestId slotRequestId1 = new SlotRequestId();
			final SlotRequestId slotRequestId2 = new SlotRequestId();
			requestNewAllocatedSlots(slotPool, slotRequestId1, slotRequestId2);

			final AllocationID allocationId1 = allocationIds.get(0);
			final AllocationID allocationId2 = allocationIds.get(1);

			// create a random non-existing allocation id
			AllocationID randomAllocationId;
			do {
				randomAllocationId = new AllocationID();
			} while (randomAllocationId.equals(allocationId1) || randomAllocationId.equals(allocationId2));

			registerAndOfferSlot(taskManagerLocation, slotPool, randomAllocationId);

			assertThat(slotPool.getPendingRequests().values(), hasSize(1));
			assertThat(canceledAllocations, contains(allocationId1));
		}
	}

	/**
	 * In this case a slot is offered to the SlotPoolImpl before the ResourceManager is connected.
	 * It can happen in production if a TaskExecutor is reconnected to a restarted JobMaster.
	 */
	@Test
	public void testSlotsOfferedWithoutResourceManagerConnected() throws Exception {
		try (SlotPoolImpl slotPool = new TestingSlotPoolImpl(new JobID())) {
			slotPool.start(JobMasterId.generate(), "mock-address", mainThreadExecutor);

			final SlotRequestId slotRequestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedSlot(slotPool, slotRequestId);

			assertThat(slotPool.getWaitingForResourceManager().values(), hasSize(1));

			final AllocationID allocationId = new AllocationID();
			registerAndOfferSlot(taskManagerLocation, slotPool, allocationId);

			assertThat(slotPool.getWaitingForResourceManager().values(), hasSize(0));
			assertThat(slotFuture.isDone(), is(true));
			assertThat(slotFuture.isCompletedExceptionally(), is(false));
			assertThat(slotFuture.getNow(null).getAllocationId(), is(allocationId));
		}
	}

	private static TestingSlotPoolImpl createAndSetUpSlotPool(
			final ResourceManagerGateway resourceManagerGateway) throws Exception {

		return new SlotPoolBuilder(mainThreadExecutor).setResourceManagerGateway(resourceManagerGateway).build();
	}

	private static TestingSlotPoolImpl createAndSetUpSlotPool(
			final ResourceManagerGateway resourceManagerGateway,
			final JobID jobId) throws Exception {

		return new SlotPoolBuilder(mainThreadExecutor)
			.setResourceManagerGateway(resourceManagerGateway)
			.setJobId(jobId)
			.build();
	}

	private static TestingSlotPoolImpl createAndSetUpSlotPool(
			final ResourceManagerGateway resourceManagerGateway,
			final Clock clock,
			final Time idleSlotTimeout) throws Exception {

		return new SlotPoolBuilder(mainThreadExecutor)
			.setResourceManagerGateway(resourceManagerGateway)
			.setClock(clock)
			.setIdleSlotTimeout(idleSlotTimeout)
			.build();
	}

	private boolean registerAndOfferSlot(
			final TaskManagerLocation taskManagerLocation,
			final SlotPoolImpl slotPool,
			final AllocationID allocationId) {

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());

		final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.ANY);
		return slotPool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer);
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
}
