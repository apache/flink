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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Test base for scheduler related test cases. The test are
 * executed with the {@link SlotPool}.
 */
public abstract class SchedulerTestBase extends TestLogger {

	protected TestingSlotPoolSlotProvider testingSlotProvider;

	private TestingSlotPoolImpl slotPool;

	private Scheduler scheduler;

	private ComponentMainThreadExecutor componentMainThreadExecutor;

	@Before
	public void setup() throws Exception {
		final JobID jobId = new JobID();
		slotPool = new TestingSlotPoolImpl(jobId);
		scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);

		testingSlotProvider = new TestingSlotPoolSlotProvider();

		final JobMasterId jobMasterId = JobMasterId.generate();
		final String jobManagerAddress = "localhost";
		componentMainThreadExecutor = getComponentMainThreadExecutor();
		slotPool.start(jobMasterId, jobManagerAddress, componentMainThreadExecutor);
		scheduler.start(componentMainThreadExecutor);
	}

	protected abstract ComponentMainThreadExecutor getComponentMainThreadExecutor();

	@After
	public void teardown() throws Exception {
		if (testingSlotProvider != null) {
			testingSlotProvider.shutdown();
			testingSlotProvider = null;
		}
	}

	protected final <T> T supplyInMainThreadExecutor(Supplier<T> supplier) {
		return CompletableFuture.supplyAsync(supplier, componentMainThreadExecutor).join();
	}

	protected final void runInMainThreadExecutor(Runnable runnable) {
		CompletableFuture.runAsync(runnable, componentMainThreadExecutor).join();
	}

	/**
	 * A test implementation of {@link SlotProvider}.
	 */
	protected final class TestingSlotPoolSlotProvider implements SlotProvider {

		private final AtomicInteger numberOfLocalizedAssignments;

		private final AtomicInteger numberOfNonLocalizedAssignments;

		private final AtomicInteger numberOfUnconstrainedAssignments;

		private final AtomicInteger numberOfHostLocalizedAssignments;

		private TestingSlotPoolSlotProvider() {
			this.numberOfLocalizedAssignments = new AtomicInteger();
			this.numberOfNonLocalizedAssignments = new AtomicInteger();
			this.numberOfUnconstrainedAssignments = new AtomicInteger();
			this.numberOfHostLocalizedAssignments = new AtomicInteger();
		}

		public TaskManagerLocation addTaskManager(int numberSlots) {
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final ResourceID resourceId = taskManagerLocation.getResourceID();

			try {
				supplyInMainThreadExecutor(() -> slotPool.registerTaskManager(resourceId));
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception occurred. This indicates a programming bug.", e);
			}

			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
			final Collection<SlotOffer> slotOffers = new ArrayList<>(numberSlots);

			for (int i = 0; i < numberSlots; i++) {
				final SlotOffer slotOffer = new SlotOffer(
					new AllocationID(),
					i,
					ResourceProfile.ANY);

				slotOffers.add(slotOffer);
			}

			final Collection<SlotOffer> acceptedSlotOffers;

			try {
				acceptedSlotOffers = supplyInMainThreadExecutor(() -> slotPool.offerSlots(
					taskManagerLocation,
					taskManagerGateway,
					slotOffers));
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception occurred. This indicates a programming bug.", e);
			}

			Preconditions.checkState(acceptedSlotOffers.size() == numberSlots);

			return taskManagerLocation;
		}

		public void releaseTaskManager(ResourceID resourceId) {
			try {
				supplyInMainThreadExecutor(
					() -> slotPool.releaseTaskManager(
						resourceId,
						new Exception("Releasing TaskManager in SlotPool for tests")));
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
		}

		public int getNumberOfAvailableSlots() {
			return supplyInMainThreadExecutor(() -> slotPool.getAvailableSlotsInformation().size());
		}

		public int getNumberOfLocalizedAssignments() {
			return numberOfLocalizedAssignments.get();
		}

		public int getNumberOfNonLocalizedAssignments() {
			return numberOfNonLocalizedAssignments.get();
		}

		public int getNumberOfUnconstrainedAssignments() {
			return numberOfUnconstrainedAssignments.get();
		}

		public int getNumberOfHostLocalizedAssignments() {
			return numberOfHostLocalizedAssignments.get();
		}

		public void shutdown() {
			runInMainThreadExecutor(() -> slotPool.close());
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			SlotProfile slotProfile,
			Time allocationTimeout) {
			return supplyInMainThreadExecutor(() -> scheduler.allocateSlot(task, slotProfile, allocationTimeout).thenApply(
				(LogicalSlot logicalSlot) -> {
					switch (logicalSlot.getLocality()) {
						case LOCAL:
							numberOfLocalizedAssignments.incrementAndGet();
							break;
						case UNCONSTRAINED:
							numberOfUnconstrainedAssignments.incrementAndGet();
							break;
						case NON_LOCAL:
							numberOfNonLocalizedAssignments.incrementAndGet();
							break;
						case HOST_LOCAL:
							numberOfHostLocalizedAssignments.incrementAndGet();
							break;
						default:
							// ignore
					}

					return logicalSlot;
				}));
		}

		@Override
		public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {

		}

		public TestingSlotPoolImpl getSlotPool() {
			return slotPool;
		}
	}

}
