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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
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
import static org.mockito.Mockito.when;

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

		CompletableFuture<SimpleSlot> future = pool.allocateSlot(new AllocationID(), DEFAULT_TESTING_PROFILE, null, Time.days(1));

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
	public void testCancelSlotAllocation() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.days(1), Time.days(1),
				Time.seconds(3) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		SlotPoolGateway slotPoolGateway = pool.getSelfGateway(SlotPoolGateway.class);

		// 1. test the pending request is in waitingResourceManagerRequests
		AllocationID allocationID = new AllocationID();
		CompletableFuture<SimpleSlot> future = slotPoolGateway.allocateSlot(allocationID, DEFAULT_TESTING_PROFILE, null, Time.seconds(1));

		try {
			future.get(2, TimeUnit.SECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertEquals(AskTimeoutException.class, e.getCause().getClass());
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}

		assertEquals(1, pool.getNumOfWaitingForResourceRequests());

		pool.cancelSlotAllocation(allocationID);
		assertEquals(0, pool.getNumOfWaitingForResourceRequests());

		// 2. test the pending request is in pendingRequests
		ResourceManagerGateway resourceManagerGateway = SlotPoolTest.createResourceManagerGatewayMock();
		pool.connectToResourceManager(resourceManagerGateway);

		AllocationID allocationID2 = new AllocationID();
		future = slotPoolGateway.allocateSlot(allocationID2, DEFAULT_TESTING_PROFILE, null, Time.seconds(1));

		try {
			future.get(2, TimeUnit.SECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertEquals(AskTimeoutException.class, e.getCause().getClass());
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}

		assertEquals(1, pool.getNumOfPendingRequests());

		pool.cancelSlotAllocation(allocationID2);
		assertEquals(0, pool.getNumOfPendingRequests());
		//verify(resourceManagerGateway, times(1)).cancelSlotRequest(jid, any(JobMasterId.class), allocationID2);

		// 3. test the allocation is timed out in client side but the request is fulfilled in slot pool
		AllocationID allocationID3 = new AllocationID();
		future = slotPoolGateway.allocateSlot(allocationID3, DEFAULT_TESTING_PROFILE, null, Time.seconds(1));

		try {
			future.get(2, TimeUnit.SECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertEquals(AskTimeoutException.class, e.getCause().getClass());
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}

		ResourceID resourceID = ResourceID.generate();
		AllocatedSlot allocatedSlot = SlotPoolTest.createAllocatedSlot(resourceID, allocationID3, jid, DEFAULT_TESTING_PROFILE);
		slotPoolGateway.registerTaskManager(resourceID);
		assertTrue(slotPoolGateway.offerSlot(allocatedSlot).get());

		assertEquals(0, pool.getNumOfPendingRequests());
		assertTrue(pool.getAllocatedSlots().contains(allocationID3));

		pool.cancelSlotAllocation(allocationID3);
		assertFalse(pool.getAllocatedSlots().contains(allocationID3));
		assertTrue(pool.getAvailableSlots().contains(allocationID3));
	}

	@Test
	public void testProviderAndOwner() throws Exception {
		final JobID jid = new JobID();

		final SlotPool pool = new SlotPool(
				rpcService, jid,
				SystemClock.getInstance(),
				Time.seconds(1), Time.days(1),
				Time.seconds(3) // this is the timeout for the request tested here
		);
		pool.start(JobMasterId.generate(), "foobar");
		ResourceManagerGateway resourceManagerGateway = SlotPoolTest.createResourceManagerGatewayMock();
		pool.connectToResourceManager(resourceManagerGateway);

		ScheduledUnit mockScheduledUnit = mock(ScheduledUnit.class);
		Execution mockExecution = mock(Execution.class);
		ExecutionVertex mockExecutionVertex = mock(ExecutionVertex.class);
		when(mockScheduledUnit.getTaskToExecute()).thenReturn(mockExecution);
		when(mockExecution.getVertex()).thenReturn(mockExecutionVertex);
		when(mockExecutionVertex.getPreferredLocations()).thenReturn(null);
														
		// test the pending request is clear when timed out
		CompletableFuture<SimpleSlot> future = pool.getSlotProvider().allocateSlot(mockScheduledUnit, true);

		try {
			future.get(2, TimeUnit.SECONDS);
			fail("We expected a AskTimeoutException.");
		}
		catch (ExecutionException e) {
			assertEquals(AskTimeoutException.class, e.getCause().getClass());
		}
		catch (Exception e) {
			fail("wrong exception: " + e);
		}

		// wait for the async call to execute
		Thread.sleep(1000);
		assertEquals(0, pool.getNumOfPendingRequests());
	}
}
