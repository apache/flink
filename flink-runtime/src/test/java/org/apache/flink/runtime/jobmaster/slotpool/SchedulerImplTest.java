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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.DummyScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SchedulerImpl}.
 */
public class SchedulerImplTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(1L);

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource(10L);

	private final TestingComponentMainThreadExecutor testMainThreadExecutor =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

	private TestingResourceManagerGateway resourceManagerGateway;
	private SlotPoolBuilder slotPoolBuilder;

	@Before
	public void setUp() throws Exception {
		resourceManagerGateway = new TestingResourceManagerGateway();
		slotPoolBuilder = new SlotPoolBuilder(testMainThreadExecutor.getMainThreadExecutor())
			.setResourceManagerGateway(resourceManagerGateway);
	}

	/**
	 * This case make sure when allocateSlot in ProviderAndOwner timeout,
	 * it will automatically call cancelSlotAllocation as will inject future.whenComplete in ProviderAndOwner.
	 */
	@Test
	public void testProviderAndOwnerSlotAllocationTimeout() throws Exception {

		try (TestingSlotPoolImpl slotPool = createAndSetUpSlotPool()) {

			final CompletableFuture<SlotRequestId> releaseSlotFuture = new CompletableFuture<>();

			slotPool.setReleaseSlotConsumer(releaseSlotFuture::complete);

			final Scheduler scheduler = createAndSetUpScheduler(slotPool);

			// test the pending request is clear when timed out
			final CompletableFuture<LogicalSlot> allocateSlotFuture = allocateSlot(scheduler);
			try {
				allocateSlotFuture.get();
				fail("We expected a TimeoutException.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
			}

			// wait for the cancel call on the SlotPoolImpl
			releaseSlotFuture.get();

			assertEquals(0L, slotPool.getNumberOfPendingRequests());
		}
	}

	private Scheduler createAndSetUpScheduler(SlotPool slotPool) {
		final Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(testMainThreadExecutor.getMainThreadExecutor());
		return scheduler;
	}

	private TestingSlotPoolImpl createAndSetUpSlotPool() throws Exception {
		return slotPoolBuilder.build();
	}

	private CompletableFuture<LogicalSlot> allocateSlot(Scheduler scheduler) {
		return testMainThreadExecutor.execute(() -> scheduler.allocateSlot(
			new DummyScheduledUnit(),
			SlotProfile.noRequirements(),
			TIMEOUT
		));
	}
}
