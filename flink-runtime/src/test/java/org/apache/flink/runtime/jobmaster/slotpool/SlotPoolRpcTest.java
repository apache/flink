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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
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
import akka.pattern.AskTimeoutException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the SlotPool using a proper RPC setup.
 */
public class SlotPoolRpcTest extends TestLogger {

	private static RpcService rpcService;

	private static final Time timeout = Time.seconds(10L);

	// ------------------------------------------------------------------------
	//  setup
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void setup() {
		ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
		rpcService = new AkkaRpcService(actorSystem, Time.seconds(10));
	}

	@AfterClass
	public static  void shutdown() {
		if (rpcService != null) {
			rpcService.stopService();
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
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			Time.milliseconds(10L) // this is the timeout for the request tested here
		);

		try {
			pool.start(JobMasterId.generate(), "foobar");

			CompletableFuture<LogicalSlot> future = pool.allocateSlot(
				new SlotRequestId(),
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				TestingUtils.infiniteTime());

			try {
				future.get();
				fail("We expected an ExecutionException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof NoResourceAvailableException);
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
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				Time.milliseconds(10L));

			try {
				future.get();
				fail("We expected a AskTimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof AskTimeoutException);
			}

			assertEquals(1L, (long) pool.getNumberOfWaitingForResourceRequests().get());

			slotPoolGateway.releaseSlot(requestId, null, null).get();

			assertEquals(0L, (long) pool.getNumberOfWaitingForResourceRequests().get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	@Test
	public void testCancelSlotAllocationWithResourceManager() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				Time.milliseconds(10L));

			try {
				future.get();
				fail("We expected a AskTimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof AskTimeoutException);
			}

			assertEquals(1L, (long) pool.getNumberOfPendingRequests().get());

			slotPoolGateway.releaseSlot(requestId, null, null).get();
			assertEquals(0L, (long) pool.getNumberOfPendingRequests().get());
		} finally {
			RpcUtils.terminateRpcEndpoint(pool, timeout);
		}
	}

	/**
	 * Tests that allocated slots are not cancelled.
	 */
	@Test
	public void testCancelSlotAllocationWhileSlotFulfilled() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		try {
			pool.start(JobMasterId.generate(), "foobar");
			SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			CompletableFuture<LogicalSlot> future = slotPoolGateway.allocateSlot(
				requestId,
				new ScheduledUnit(SchedulerTestUtils.getDummyTask()),
				DEFAULT_TESTING_PROFILE,
				Collections.emptyList(),
				true,
				Time.milliseconds(10L));

			try {
				future.get();
				fail("We expected a AskTimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof AskTimeoutException);
			}

			AllocationID allocationId = allocationIdFuture.get();
			final SlotOffer slotOffer = new SlotOffer(
				allocationId,
				0,
				DEFAULT_TESTING_PROFILE);
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

			slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID()).get();

			assertTrue(slotPoolGateway.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer).get());

			assertEquals(0L, (long) pool.getNumberOfPendingRequests().get());

			assertTrue(pool.containsAllocatedSlot(allocationId).get());

			pool.releaseSlot(requestId, null, null).get();

			assertFalse(pool.containsAllocatedSlot(allocationId).get());
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
	public void testProviderAndOwner() throws Exception {
		final JobID jid = new JobID();

		final TestingSlotPool pool = new TestingSlotPool(
			rpcService,
			jid,
			SystemClock.getInstance(),
			Time.milliseconds(10L),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		final CompletableFuture<SlotRequestId> releaseSlotFuture = new CompletableFuture<>();

		pool.setReleaseSlotConsumer(
			slotRequestID -> releaseSlotFuture.complete(slotRequestID));

		try {
			pool.start(JobMasterId.generate(), "foobar");
			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			ScheduledUnit mockScheduledUnit = new ScheduledUnit(SchedulerTestUtils.getDummyTask());

			// test the pending request is clear when timed out
			CompletableFuture<LogicalSlot> future = pool.getSlotProvider().allocateSlot(
				mockScheduledUnit,
				true,
				Collections.emptyList());

			try {
				future.get();
				fail("We expected a AskTimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof AskTimeoutException);
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

		public TestingSlotPool(
				RpcService rpcService,
				JobID jobId,
				Clock clock,
				Time slotRequestTimeout,
				Time resourceManagerAllocationTimeout,
				Time resourceManagerRequestTimeout) {
			super(
				rpcService,
				jobId,
				clock,
				slotRequestTimeout,
				resourceManagerAllocationTimeout,
				resourceManagerRequestTimeout);

			releaseSlotConsumer = null;
		}

		public void setReleaseSlotConsumer(Consumer<SlotRequestId> releaseSlotConsumer) {
			this.releaseSlotConsumer = Preconditions.checkNotNull(releaseSlotConsumer);
		}

		@Override
		public CompletableFuture<Acknowledge> releaseSlot(
			SlotRequestId slotRequestId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			@Nullable Throwable cause) {
			final Consumer<SlotRequestId> currentReleaseSlotConsumer = releaseSlotConsumer;

			if (currentReleaseSlotConsumer != null) {
				currentReleaseSlotConsumer.accept(slotRequestId);
			}

			return super.releaseSlot(slotRequestId, slotSharingGroupId, cause);
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
