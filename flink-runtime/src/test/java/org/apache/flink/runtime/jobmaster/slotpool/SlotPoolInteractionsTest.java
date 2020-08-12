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
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.SystemClock;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.jobmaster.slotpool.AvailableSlotsTest.DEFAULT_TESTING_PROFILE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the SlotPoolImpl interactions.
 */
public class SlotPoolInteractionsTest extends TestLogger {

	private static final Time fastTimeout = Time.milliseconds(1L);

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource(10L);

	private final TestingComponentMainThreadExecutor testMainThreadExecutor =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testSlotAllocationNoResourceManager() throws Exception {
		final JobID jid = new JobID();

		try (SlotPool pool = new SlotPoolImpl(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime()
		)) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor.getMainThreadExecutor());

			final CompletableFuture<PhysicalSlot> future = testMainThreadExecutor.execute(() -> pool.requestNewAllocatedSlot(
				new SlotRequestId(),
				ResourceProfile.UNKNOWN,
				fastTimeout));

			try {
				future.get();
				fail("We expected an ExecutionException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}
		}
	}

	@Test
	public void testCancelSlotAllocationWithoutResourceManager() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPoolImpl pool = createTestingSlotPool(jid)) {

			final CompletableFuture<SlotRequestId> timeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(timeoutFuture::complete);
			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor.getMainThreadExecutor());

			SlotRequestId requestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future = testMainThreadExecutor.execute(() -> pool.requestNewAllocatedSlot(
				requestId,
				ResourceProfile.UNKNOWN,
				fastTimeout));

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the timeout of the pending slot request
			timeoutFuture.get();

			assertEquals(0L, pool.getNumberOfWaitingForResourceRequests());
		}
	}

	@Nonnull
	private TestingSlotPoolImpl createTestingSlotPool(JobID jid) {
		return new TestingSlotPoolImpl(
			jid,
			SystemClock.getInstance(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());
	}

	/**
	 * Tests that a slot allocation times out wrt to the specified time out.
	 */
	@Test
	public void testSlotAllocationTimeout() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPoolImpl pool = createTestingSlotPool(jid)) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor.getMainThreadExecutor());

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future = testMainThreadExecutor.execute(() -> pool.requestNewAllocatedSlot(
				requestId,
				ResourceProfile.UNKNOWN,
				fastTimeout));

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());
		}
	}

	/**
	 * Tests that extra slots are kept by the {@link SlotPoolImpl}.
	 */
	@Test
	public void testExtraSlotsAreKept() throws Exception {
		final JobID jid = new JobID();

		try (TestingSlotPoolImpl pool = createTestingSlotPool(jid)) {

			pool.start(JobMasterId.generate(), "foobar", testMainThreadExecutor.getMainThreadExecutor());

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

			TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
			resourceManagerGateway.setRequestSlotConsumer(
				(SlotRequest slotRequest) -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture = new CompletableFuture<>();
			pool.setTimeoutPendingSlotRequestConsumer(slotRequestTimeoutFuture::complete);

			pool.connectToResourceManager(resourceManagerGateway);

			SlotRequestId requestId = new SlotRequestId();
			final CompletableFuture<PhysicalSlot> future = testMainThreadExecutor.execute(() -> pool.requestNewAllocatedSlot(
				requestId,
				ResourceProfile.UNKNOWN,
				fastTimeout));

			try {
				future.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait until we have timed out the slot request
			slotRequestTimeoutFuture.get();

			assertEquals(0L, pool.getNumberOfPendingRequests());

			AllocationID allocationId = allocationIdFuture.get();
			final SlotOffer slotOffer = new SlotOffer(
				allocationId,
				0,
				DEFAULT_TESTING_PROFILE);
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

			testMainThreadExecutor.execute(() -> pool.registerTaskManager(taskManagerLocation.getResourceID()));

			assertTrue(testMainThreadExecutor.execute(() -> pool.offerSlot(taskManagerLocation, taskManagerGateway, slotOffer)));

			assertTrue(pool.containsAvailableSlot(allocationId));
		}
	}
}
