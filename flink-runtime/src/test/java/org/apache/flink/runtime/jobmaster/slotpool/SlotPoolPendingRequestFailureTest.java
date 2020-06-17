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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the failing of pending slot requests at the {@link SlotPool}.
 */
public class SlotPoolPendingRequestFailureTest extends TestLogger {

	private static final JobID jobId = new JobID();

	private static final ComponentMainThreadExecutor mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
	public static final Time TIMEOUT = Time.seconds(10L);

	private TestingResourceManagerGateway resourceManagerGateway;

	@Before
	public void setup() {
		resourceManagerGateway = new TestingResourceManagerGateway();
	}

	/**
	 * Tests that failing an allocation fails the pending slot request.
	 */
	@Test
	public void testFailingAllocationFailsPendingSlotRequests() throws Exception {
		final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

		try (SlotPoolImpl slotPool = setUpSlotPool()) {

			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedSlot(slotPool, new SlotRequestId());

			final AllocationID allocationId = allocationIdFuture.get();

			assertThat(slotFuture.isDone(), is(false));

			final FlinkException cause = new FlinkException("Fail pending slot request failure.");
			final Optional<ResourceID> responseFuture = slotPool.failAllocation(allocationId, cause);

			assertThat(responseFuture.isPresent(), is(false));

			try {
				slotFuture.get();
				fail("Expected a slot allocation failure.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), equalTo(cause));
			}
		}
	}

	@Test
	public void testFailingAllocationFailsRemappedPendingSlotRequests() throws Exception {
		final List<AllocationID> allocations = new ArrayList<>();
		resourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocations.add(slotRequest.getAllocationId()));

		try (SlotPoolImpl slotPool = setUpSlotPool()) {
			final CompletableFuture<PhysicalSlot> slotFuture1 = requestNewAllocatedSlot(slotPool, new SlotRequestId());
			final CompletableFuture<PhysicalSlot> slotFuture2 = requestNewAllocatedSlot(slotPool, new SlotRequestId());

			final AllocationID allocationId1 = allocations.get(0);
			final AllocationID allocationId2 = allocations.get(1);

			final TaskManagerLocation location = new LocalTaskManagerLocation();
			final SlotOffer slotOffer = new SlotOffer(allocationId2, 0, ResourceProfile.ANY);
			slotPool.registerTaskManager(location.getResourceID());
			slotPool.offerSlot(location, new SimpleAckingTaskManagerGateway(), slotOffer);

			assertThat(slotFuture1.isDone(), is(true));
			assertThat(slotFuture2.isDone(), is(false));

			final FlinkException cause = new FlinkException("Fail pending slot request failure.");
			final Optional<ResourceID> responseFuture = slotPool.failAllocation(allocationId1, cause);

			assertThat(responseFuture.isPresent(), is(false));

			try {
				slotFuture2.getNow(null);
				fail("Expected a slot allocation failure.");
			} catch (Throwable t) {
				assertThat(ExceptionUtils.stripCompletionException(t), equalTo(cause));
			}
		}
	}

	/**
	 * Tests that a failing resource manager request fails a pending slot request and cancels the slot
	 * request at the RM (e.g. due to a TimeoutException).
	 *
	 * <p>See FLINK-7870
	 */
	@Test
	public void testFailingResourceManagerRequestFailsPendingSlotRequestAndCancelsRMRequest() throws Exception {

		try (SlotPoolImpl slotPool = setUpSlotPool()) {
			final CompletableFuture<Acknowledge> requestSlotFuture = new CompletableFuture<>();
			final CompletableFuture<AllocationID> cancelSlotFuture = new CompletableFuture<>();
			final CompletableFuture<AllocationID> requestSlotFutureAllocationId = new CompletableFuture<>();
			resourceManagerGateway.setRequestSlotFuture(requestSlotFuture);
			resourceManagerGateway.setRequestSlotConsumer(slotRequest -> requestSlotFutureAllocationId.complete(slotRequest.getAllocationId()));
			resourceManagerGateway.setCancelSlotConsumer(cancelSlotFuture::complete);

			final CompletableFuture<PhysicalSlot> slotFuture = requestNewAllocatedSlot(slotPool, new SlotRequestId());

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
		}
	}

	/**
	 * Tests that a pending slot request is failed with a timeout.
	 */
	@Test
	public void testPendingSlotRequestTimeout() throws Exception {
		final ScheduledExecutorService singleThreadExecutor = Executors.newSingleThreadScheduledExecutor();
		final ComponentMainThreadExecutor componentMainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadExecutor);

		final SlotPoolImpl slotPool = setUpSlotPool(componentMainThreadExecutor);

		try {
			final Time timeout = Time.milliseconds(5L);

			final CompletableFuture<PhysicalSlot> slotFuture = CompletableFuture
				.supplyAsync(() -> requestNewAllocatedSlot(slotPool, new SlotRequestId(), timeout), componentMainThreadExecutor)
				.thenCompose(Function.identity());

			try {
				slotFuture.get();
				fail("Expected that the future completes with a TimeoutException.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(TimeoutException.class));
			}
		} finally {
			CompletableFuture.runAsync(ThrowingRunnable.unchecked(slotPool::close), componentMainThreadExecutor).get();
			singleThreadExecutor.shutdownNow();
		}
	}

	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(SlotPoolImpl slotPool, SlotRequestId slotRequestId) {
		return requestNewAllocatedSlot(slotPool, slotRequestId, TIMEOUT);
	}

	private CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(SlotPoolImpl slotPool, SlotRequestId slotRequestId, Time timeout) {
		return slotPool.requestNewAllocatedSlot(slotRequestId, ResourceProfile.UNKNOWN, timeout);
	}

	private SlotPoolImpl setUpSlotPool() throws Exception {
		return setUpSlotPool(mainThreadExecutor);
	}

	private SlotPoolImpl setUpSlotPool(ComponentMainThreadExecutor componentMainThreadExecutor) throws Exception {
		final SlotPoolImpl slotPool = new TestingSlotPoolImpl(jobId);
		slotPool.start(JobMasterId.generate(), "foobar", componentMainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);

		return slotPool;
	}

}
