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

package org.apache.flink.runtime.instance;

import akka.actor.ActorSystem;
import akka.pattern.AskTimeoutException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.util.clock.SystemClock;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.instance.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests for the SlotPool using a proper RPC setup.
 */
public class SlotPoolRpcTest extends TestLogger {

	private static RpcService rpcService;

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
		rpcService.stopService();
	}

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testSlotAllocationNoResourceManager() throws Exception {
		final JobID jid = new JobID();
		
		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.milliseconds(100) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");

		CompletableFuture<SimpleSlot> future = pool.allocateSlot(new AllocationID(), mock(ScheduledUnit.class), DEFAULT_TESTING_PROFILE, null, Time.days(1));

		try {
			future.get(4, TimeUnit.SECONDS);
			fail("We expected a ExecutionException.");
		}
		catch (ExecutionException e) {
			assertEquals(NoResourceAvailableException.class, e.getCause().getClass());
		}
		catch (TimeoutException e) {
			fail("future timed out rather than being failed");
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}
	}

	@Test
	public void testCancelSlotAllocationWithoutResourceManager() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.seconds(1) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

		AllocationID allocationID = new AllocationID();
		CompletableFuture<SimpleSlot> future = slotPoolGateway.allocateSlot(allocationID, mock(ScheduledUnit.class), DEFAULT_TESTING_PROFILE, null, Time.milliseconds(100));

		try {
			future.get(500, TimeUnit.MILLISECONDS);
			fail("We expected a AskTimeoutException.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof AskTimeoutException);
		}

		assertEquals(1, slotPoolGateway.getNumberOfWaitingForResourceRequests().get().intValue());

		pool.cancelSlotAllocation(allocationID);
		assertEquals(0, slotPoolGateway.getNumberOfWaitingForResourceRequests().get().intValue());
	}

	@Test
	public void testCancelSlotAllocationWithResourceManager() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.seconds(1) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

		ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		pool.connectToResourceManager(resourceManagerGateway);

		AllocationID allocationID = new AllocationID();
		CompletableFuture<SimpleSlot> future = slotPoolGateway.allocateSlot(allocationID, mock(ScheduledUnit.class), DEFAULT_TESTING_PROFILE, null, Time.milliseconds(100));

		try {
			future.get(500, TimeUnit.MILLISECONDS);
			fail("We expected a AskTimeoutException.");
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof AskTimeoutException);
		}

		assertEquals(1, slotPoolGateway.getNumberOfPendingRequests().get().intValue());

		pool.cancelSlotAllocation(allocationID);
		assertEquals(0, slotPoolGateway.getNumberOfPendingRequests().get().intValue());
	}

	@Test
	public void testCancelSlotAllocationWhileSlotFulfilled() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.seconds(1) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

		ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		pool.connectToResourceManager(resourceManagerGateway);

		AllocationID allocationID = new AllocationID();
		CompletableFuture<SimpleSlot> future = slotPoolGateway.allocateSlot(allocationID, mock(ScheduledUnit.class), DEFAULT_TESTING_PROFILE, null, Time.milliseconds(100));

		try {
			future.get(500, TimeUnit.MILLISECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof AskTimeoutException);
		}

		ResourceID resourceID = ResourceID.generate();
		AllocatedSlot allocatedSlot = SlotPoolTest.createAllocatedSlot(resourceID, allocationID, jid, DEFAULT_TESTING_PROFILE);
		slotPoolGateway.registerTaskManager(resourceID);
		assertTrue(slotPoolGateway.offerSlot(allocatedSlot).get());

		assertEquals(0, slotPoolGateway.getNumberOfPendingRequests().get().intValue());
		assertTrue(pool.getAllocatedSlots().contains(allocationID));

		pool.cancelSlotAllocation(allocationID);
		assertFalse(pool.getAllocatedSlots().contains(allocationID));
		assertTrue(pool.getAvailableSlots().contains(allocationID));
	}

	/**
	 * This case make sure when allocateSlot in ProviderAndOwner timeout,
	 * it will automatically call cancelSlotAllocation as will inject future.whenComplete in ProviderAndOwner.
	 */
	@Test
	public void testProviderAndOwner() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.milliseconds(100), Time.days(1),
				Time.seconds(1) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		pool.connectToResourceManager(resourceManagerGateway);

		ScheduledUnit mockScheduledUnit = new ScheduledUnit(SchedulerTestUtils.getDummyTask());

		// test the pending request is clear when timed out
		CompletableFuture<SimpleSlot> future = pool.getSlotProvider().allocateSlot(mockScheduledUnit, true, null);

		try {
			future.get(500, TimeUnit.MILLISECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof AskTimeoutException);
		}

		assertEquals(0, pool.getSelfGateway(SlotPoolGateway.class).getNumberOfPendingRequests().get().intValue());
	}
}
