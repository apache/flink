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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.clock.Clock;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the SlotPool using a proper RPC setup.
 */
public class SlotPoolRpcTest extends TestLogger {

	private static RpcService rpcService;

	private static final Time timeout = Time.seconds(10L);

	private static final Time fastTimeout = Time.milliseconds(1L);

	// ------------------------------------------------------------------------
	//  setup
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() {
		ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
		rpcService = new AkkaRpcService(actorSystem, Time.seconds(10));
	}

	@AfterClass
	public static  void shutdown() throws InterruptedException, ExecutionException, TimeoutException {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, timeout);
			rpcService = null;
		}
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testSlotAllocationNoResourceManager() throws Exception {
		final JobID jid = new JobID();
		
		final SlotPool pool = new SlotPool(
			rpcService,
			jid,
			LocationPreferenceSchedulingStrategy.getInstance(),
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime()
		);

		try {
			pool.start(JobMasterId.generate(), "foobar");

			CompletableFuture<LogicalSlot> future = pool.allocateSlot(
				new SlotRequestId(),
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected an ExecutionException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	@Test
	public void testCancelSlotAllocationWithoutResourceManager() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			final CompletableFuture<SlotRequestId> timeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestId -> timeoutFuture.complete(slotRequestId));
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the timeout of the pending slot request
			timeoutFuture.get();

			assertEquals(0L, (long) pool.getNumberOfWaitingForResourceRequests().get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	/**
	 * Tests that a slot allocation times out wrt to the specified time out.
	 */
	@Test
	public void testSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new DummyScheduledUnit(),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, (long) pool.getNumberOfPendingRequests().get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	/**
	 * Tests that extra slots are kept by the {@link SlotPool}.
	 */
	@Test
	public void testExtraSlotsAreKept() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				SlotProfile.noLocality(DEFAULT_TESTING_PROFILE),
				true,
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, (long) pool.getNumberOfPendingRequests().get());

			AllocationID allocationId = allocationIdFuture.get();
			final SlotOffer slotOffer = new SlotOffer(
				allocationId,
				0,
				DEFAULT_TESTING_PROFILE);
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID()).get();

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			assertTrue(pool.containsAvailableSlot(allocationId).get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	/**
	 * This case make sure when allocateSlot in ProviderAndOwner timeout,
	 * it will automatically call cancelSlotAllocation as will inject future.whenComplete in ProviderAndOwner.
	 */
	@Test
	public void testProviderAndOwnerSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		final CompletableFuture<SlotRequestId> releaseSlotFuture = new CompletableFuture<>();

		pool.setReleaseSlotConsumer(
			slotRequestID -> releaseSlotFuture.complete(slotRequestID));

		try {
			pool.start(JobMasterId.generate(), "foobar");
			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			// test the pending request is clear when timed out
			CompletableFuture<LogicalSlot> future = pool.getSlotProvider().allocateSlot(
				new DummyScheduledUnit(),
				true,
				SlotProfile.noRequirements(),
				fastTimeout);

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the cancel call on the SlotPool
			releaseSlotFuture.get();

			assertEquals(0L, (long) pool.getNumberOfPendingRequests().get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	/**
	 * Testing SlotPool which exposes internal state via some testing methods.
	 */
	private static final class TestingSlotPool extends SlotPool {

		private volatile Consumer<SlotRequestId> releaseSlotConsumer;

		private volatile Consumer<SlotRequestId> timeoutPendingSlotRequestConsumer;

		public TestingSlotPool(
				RpcService rpcService,
				JobID jobId,
				Clock clock,
				Time rpcTimeout,
				Time idleSlotTimeout) {
			super(
				rpcService,
				jobId,
				LocationPreferenceSchedulingStrategy.getInstance(),
				clock,
				rpcTimeout,
				idleSlotTimeout);

			releaseSlotConsumer = null;
			timeoutPendingSlotRequestConsumer = null;
		}

		public void setReleaseSlotConsumer(Consumer<SlotRequestId> releaseSlotConsumer) {
			this.releaseSlotConsumer = Preconditions.checkNotNull(releaseSlotConsumer);
		}

		public void setTimeoutPendingSlotRequestConsumer(Consumer<SlotRequestId> timeoutPendingSlotRequestConsumer) {
			this.timeoutPendingSlotRequestConsumer = Preconditions.checkNotNull(timeoutPendingSlotRequestConsumer);
		}

		@Override
		public CompletableFuture<Acknowledge> releaseSlot(
			SlotRequestId slotRequestId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable Throwable cause) {
			final Consumer<SlotRequestId> currentReleaseSlotConsumer = releaseSlotConsumer;

			final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = super.releaseSlot(slotRequestId, slotSharingGroupId, cause);

			if (currentReleaseSlotConsumer != null) {
				currentReleaseSlotConsumer.accept(slotRequestId);
			}

			return acknowledgeCompletableFuture;
		}

		@Override
		protected void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
			final Consumer<SlotRequestId> currentTimeoutPendingSlotRequestConsumer = timeoutPendingSlotRequestConsumer;

			if (currentTimeoutPendingSlotRequestConsumer != null) {
				currentTimeoutPendingSlotRequestConsumer.accept(slotRequestId);
			}

			super.timeoutPendingSlotRequest(slotRequestId);
		}

		CompletableFuture<Boolean> containsAllocatedSlot(AllocationID allocationId) {
			return callAsync(
				() -> getAllocatedSlots().contains(allocationId),
				timeout);
		}

		CompletableFuture<Boolean> containsAvailableSlot(AllocationID allocationId) {
			return callAsync(
				() -> getAvailableSlots().contains(allocationId),
				timeout);
		}

		CompletableFuture<Integer> getNumberOfPendingRequests() {
			return callAsync(
				() -> getPendingRequests().size(),
				timeout);
		}

		CompletableFuture<Integer> getNumberOfWaitingForResourceRequests() {
			return callAsync(
				() -> getWaitingForResourceManager().size(),
				timeout);
		}
	}

}
