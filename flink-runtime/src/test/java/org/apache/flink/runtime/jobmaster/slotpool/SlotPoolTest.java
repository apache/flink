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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.ManualClock;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(Flip6.class)
public class SlotPoolTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SlotPoolTest.class);

	private final Time timeout = Time.seconds(10L);

	private RpcService rpcService;

	private JobID jobId;

	private TaskManagerLocation taskManagerLocation;

	private SimpleAckingTaskManagerGateway taskManagerGateway;

	private TestingResourceManagerGateway resourceManagerGateway;

	@Before
	public void setUp() throws Exception {
		this.rpcService = new TestingRpcService();
		this.jobId = new JobID();

		taskManagerLocation = new LocalTaskManagerLocation();
		taskManagerGateway = new SimpleAckingTaskManagerGateway();
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	@After
	public void tearDown() throws Exception {
		RpcUtils.terminateRpcService(rpcService, timeout);
	}

	@Test
	public void testAllocateSimpleSlot() throws Exception {
		CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> slotRequestFuture.complete(slotRequest));

		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		try {
			SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);
			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID());

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			LogicalSlot slot = future.get(1, TimeUnit.SECONDS);
			assertTrue(future.isDone());
			assertTrue(slot.isAlive());
			assertEquals(taskManagerLocation, slot.getTaskManagerLocation());
		} finally {
			RpcUtils.terminateRpcEndpoint(slotPool, timeout);
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

		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		try {
			SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);
			slotPool.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);
			CompletableFuture<LogicalSlot> future2 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
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

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

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
		} finally {
			RpcUtils.terminateRpcEndpoint(slotPool, timeout);
		}
	}

	@Test
	public void testAllocateWithFreeSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> slotRequestFuture.complete(slotRequest));

		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		try {
			SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);
			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);
			assertFalse(future1.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			LogicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());

			// return this slot to pool
			slot1.releaseSlot();

			CompletableFuture<LogicalSlot> future2 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);

			// second allocation fulfilled by previous slot returning
			LogicalSlot slot2 = future2.get(1, TimeUnit.SECONDS);
			assertTrue(future2.isDone());

			assertNotEquals(slot1, slot2);
			assertFalse(slot1.isAlive());
			assertTrue(slot2.isAlive());
			assertEquals(slot1.getTaskManagerLocation(), slot2.getTaskManagerLocation());
			assertEquals(slot1.getPhysicalSlotNumber(), slot2.getPhysicalSlotNumber());
		} finally {
			slotPool.shutDown();
		}
	}

	@Test
	public void testOfferSlot() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> slotRequestFuture.complete(slotRequest));

		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		try {
			SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);
			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);
			assertFalse(future.isDone());

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			final TaskManagerLocation invalidTaskManagerLocation = new LocalTaskManagerLocation();

			// slot from unregistered resource
			assertFalse(slotPoolGateway.offerSlot(invalidTaskManagerLocation, taskManagerGateway, slotOffer).get());

			final SlotOffer nonRequestedSlotOffer = new SlotOffer(
				new AllocationID(),
				0,
				DEFAULT_TESTING_PROFILE);

			// we'll also accept non requested slots
			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, nonRequestedSlotOffer).get());

			// accepted slot
			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());
			LogicalSlot slot = future.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertTrue(slot.isAlive());

			// duplicated offer with using slot
			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());
			assertTrue(slot.isAlive());

			// duplicated offer with free slot
			slot.releaseSlot();
			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());
		} finally {
			slotPool.shutDown();
		}
	}

	@Test
	public void testReleaseResource() throws Exception {
		final CompletableFuture<SlotRequest> slotRequestFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> slotRequestFuture.complete(slotRequest));

		final CompletableFuture<Boolean> slotReturnFuture = new CompletableFuture<>();

		final SlotPool slotPool = new SlotPool(rpcService, jobId) {
			@Override
			public CompletableFuture<Acknowledge> releaseSlot(
					SlotRequestId slotRequestId,
					@Nullable SlotSharingGroupId slotSharingGroupId,
					@Nullable Throwable cause) {
				super.releaseSlot(
					slotRequestId,
					slotSharingGroupId,
					cause);

				slotReturnFuture.complete(true);

				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		};

		try {
			SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);
			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID());

			CompletableFuture<LogicalSlot> future1 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);

			final SlotRequest slotRequest = slotRequestFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			CompletableFuture<LogicalSlot> future2 = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				new DummyScheduledUnit(),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				timeout);

			final SlotOffer slotOffer = new SlotOffer(
				slotRequest.getAllocationId(),
				0,
				DEFAULT_TESTING_PROFILE);

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			LogicalSlot slot1 = future1.get(1, TimeUnit.SECONDS);
			assertTrue(future1.isDone());
			assertFalse(future2.isDone());

			slotPoolGateway.releaseTaskManager(taskManagerLocation.getResourceID());

			// wait until the slot has been returned
			slotReturnFuture.get();

			assertFalse(slot1.isAlive());

			// slot released and not usable, second allocation still not fulfilled
			Thread.sleep(10);
			assertFalse(future2.isDone());
		} finally {
			slotPool.shutDown();
		}
	}

	/**
	 * Tests that a slot request is cancelled if it failed with an exception (e.g. TimeoutException).
	 *
	 * <p>See FLINK-7870
	 */
	@Test
	public void testSlotRequestCancellationUponFailingRequest() throws Exception {
		final SlotPool slotPool = new SlotPool(rpcService, jobId);
		final CompletableFuture<Acknowledge> requestSlotFuture = new CompletableFuture<>();
		final CompletableFuture<AllocationID> cancelSlotFuture = new CompletableFuture<>();
		final CompletableFuture<AllocationID> requestSlotFutureAllocationId = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotFuture(requestSlotFuture);
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> requestSlotFutureAllocationId.complete(slotRequest.getAllocationId()));
		resourceManagerGateway.setCancelSlotConsumer(allocationID -> cancelSlotFuture.complete(allocationID));

		final ScheduledUnit scheduledUnit = new ScheduledUnit(
			new JobVertexID(),
			null,
			null);

		try {
			final SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);

			CompletableFuture<LogicalSlot> slotFuture = slotPoolGateway.allocateSlot(
				new SlotRequestId(),
				scheduledUnit,
				ResourceProfile.UNKNOWN,
				Collections.emptyList(),
				true,
				timeout);

			requestSlotFuture.completeExceptionally(new FlinkException("Testing exception."));

			try {
				slotFuture.get();
				fail("The slot future should not have been completed properly.");
			} catch (Exception ignored) {
				// expected
			}

			// check that a failure triggered the slot request cancellation
			// with the correct allocation id
			assertEquals(requestSlotFutureAllocationId.get(), cancelSlotFuture.get());
		} finally {
			try {
				RpcUtils.terminateRpcEndpoint(slotPool, timeout);
			} catch (Exception e) {
				LOG.warn("Could not properly terminate the SlotPool.", e);
			}
		}
	}

	/**
	 * Tests that unused offered slots are directly used to fulfill pending slot
	 * requests.
	 *
	 * <p>See FLINK-8089
	 */
	@Test
	public void testFulfillingSlotRequestsWithUnusedOfferedSlots() throws Exception {
		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

		resourceManagerGateway.setRequestSlotConsumer(
			(SlotRequest slotRequest) -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		final SlotRequestId slotRequestId1 = new SlotRequestId();
		final SlotRequestId slotRequestId2 = new SlotRequestId();

		try {
			final SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);

			final ScheduledUnit scheduledUnit = new ScheduledUnit(
				new JobVertexID(),
				null,
				null);

			CompletableFuture<LogicalSlot> slotFuture1 = slotPoolGateway.allocateSlot(
				slotRequestId1,
				scheduledUnit,
				ResourceProfile.UNKNOWN,
				Collections.emptyList(),
				true,
				timeout);

			// wait for the first slot request
			final AllocationID allocationId = allocationIdFuture.get();

			CompletableFuture<LogicalSlot> slotFuture2 = slotPoolGateway.allocateSlot(
				slotRequestId2,
				scheduledUnit,
				ResourceProfile.UNKNOWN,
				Collections.emptyList(),
				true,
				timeout);

			slotPoolGateway.releaseSlot(slotRequestId1, null, null);

			try {
				// this should fail with a CancellationException
				slotFuture1.get();
				fail("The first slot future should have failed because it was cancelled.");
			} catch (ExecutionException ee) {
				// expected
				assertTrue(ExceptionUtils.stripExecutionException(ee) instanceof FlinkException);

			}

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID()).get();

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			// the slot offer should fulfill the second slot request
			assertEquals(allocationId, slotFuture2.get().getAllocationId());
		} finally {
			RpcUtils.terminateRpcEndpoint(slotPool, timeout);
		}
	}

	/**
	 * Tests that a SlotPool shutdown releases all registered slots
	 */
	@Test
	public void testShutdownReleasesAllSlots() throws Exception {
		final SlotPool slotPool = new SlotPool(rpcService, jobId);

		try {
			final SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);

			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID());

			final int numSlotOffers = 2;

			final Collection<SlotOffer> slotOffers = new ArrayList<>(numSlotOffers);

			for (int i = 0; i < numSlotOffers; i++) {
				slotOffers.add(
					new SlotOffer(
						new AllocationID(),
						i,
						ResourceProfile.UNKNOWN));
			}

			final ArrayBlockingQueue<AllocationID> freedSlotQueue = new ArrayBlockingQueue<>(numSlotOffers);

			taskManagerGateway.setFreeSlotConsumer(tuple -> {
				while(!freedSlotQueue.offer(tuple.f0)) {}
			});

			final CompletableFuture<Collection<SlotOffer>> acceptedSlotOffersFuture = slotPoolGateway.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers);

			final Collection<SlotOffer> acceptedSlotOffers = acceptedSlotOffersFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(acceptedSlotOffers, Matchers.equalTo(slotOffers));

			// shut down the slot pool
			slotPool.shutDown();
			slotPool.getTerminationFuture().get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			// the shut down operation should have freed all registered slots
			ArrayList<AllocationID> freedSlots = new ArrayList<>(numSlotOffers);

			while (freedSlots.size() < numSlotOffers) {
				freedSlotQueue.drainTo(freedSlots);
			}

			assertThat(freedSlots, Matchers.containsInAnyOrder(slotOffers.stream().map(SlotOffer::getAllocationId).toArray()));
		} finally {
			RpcUtils.terminateRpcEndpoint(slotPool, timeout);
		}
	}

	@Test
	public void testCheckIdleSlot() throws Exception {
		final ManualClock clock = new ManualClock();
		final SlotPool slotPool = new SlotPool(
			rpcService,
			jobId,
			clock,
			TestingUtils.infiniteTime(),
			timeout);

		try {
			final BlockingQueue<AllocationID> freedSlots = new ArrayBlockingQueue<>(1);
			taskManagerGateway.setFreeSlotConsumer((tuple) -> freedSlots.offer(tuple.f0));

			final SlotPoolGateway slotPoolGateway = setupSlotPool(slotPool, resourceManagerGateway);

			final AllocationID expiredSlotID = new AllocationID();
			final AllocationID freshSlotID = new AllocationID();
			final SlotOffer slotToExpire = new SlotOffer(expiredSlotID, 0, ResourceProfile.UNKNOWN);
			final SlotOffer slotToNotExpire = new SlotOffer(freshSlotID, 1, ResourceProfile.UNKNOWN);

			assertThat(
				slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID()).get(),
				Matchers.is(Acknowledge.get()));

			assertThat(
				slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotToExpire).get(),
				Matchers.is(true));

			clock.advanceTime(timeout.toMilliseconds() - 1L, TimeUnit.MILLISECONDS);

			assertThat(
				slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotToNotExpire).get(),
				Matchers.is(true));

			clock.advanceTime(1L, TimeUnit.MILLISECONDS);

			slotPool.triggerCheckIdleSlot();

			final AllocationID freedSlot = freedSlots.poll(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			assertThat(freedSlot, Matchers.is(expiredSlotID));
			assertThat(freedSlots.isEmpty(), Matchers.is(true));
		} finally {
			RpcUtils.terminateRpcEndpoint(slotPool, timeout);
		}
	}

	private static SlotPoolGateway setupSlotPool(
			SlotPool slotPool,
			ResourceManagerGateway resourceManagerGateway) throws Exception {
		final String jobManagerAddress = "foobar";

		slotPool.start(JobMasterId.generate(), jobManagerAddress);

		slotPool.connectToResourceManager(resourceManagerGateway);

		return slotPool.getSelfGateway(SlotPoolGateway.class);
	}

	private AllocatedSlot createSlot(final AllocationID allocationId) {
		return new AllocatedSlot(
			allocationId,
			taskManagerLocation,
			0,
			ResourceProfile.UNKNOWN,
			taskManagerGateway);
	}
}
