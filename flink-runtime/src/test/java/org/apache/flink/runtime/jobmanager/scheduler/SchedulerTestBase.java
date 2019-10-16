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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSharingManager;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test base for scheduler related test cases. The test are
 * executed with the {@link SlotPool}.
 */
public class SchedulerTestBase extends TestLogger {

	protected TestingSlotProvider testingSlotProvider;

	@Before
	public void setup() throws Exception {
		final JobID jobId = new JobID();
		final SlotPool slotPool = new TestingSlotPoolImpl(jobId);
		final TestingScheduler testingScheduler = new TestingScheduler(
			new HashMap<>(16),
			LocationPreferenceSlotSelectionStrategy.createDefault(),
			slotPool);

		testingSlotProvider = new TestingSlotPoolSlotProvider(slotPool, testingScheduler);

		final JobMasterId jobMasterId = JobMasterId.generate();
		final String jobManagerAddress = "localhost";
		ComponentMainThreadExecutor executor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
		slotPool.start(jobMasterId, jobManagerAddress, executor);
		testingScheduler.start(executor);
	}

	@After
	public void teardown() throws Exception {
		if (testingSlotProvider != null) {
			testingSlotProvider.shutdown();
			testingSlotProvider = null;
		}
	}

	protected interface TestingSlotProvider extends SlotProvider {
		TaskManagerLocation addTaskManager(int numberSlots);

		void releaseTaskManager(ResourceID resourceId);

		int getNumberOfAvailableSlots();

		int getNumberOfLocalizedAssignments();

		int getNumberOfNonLocalizedAssignments();

		int getNumberOfUnconstrainedAssignments();

		int getNumberOfHostLocalizedAssignments();

		int getNumberOfSlots(SlotSharingGroup slotSharingGroup);

		int getNumberOfAvailableSlotsForGroup(SlotSharingGroup slotSharingGroup, JobVertexID jobVertexId);

		void shutdown() throws Exception;
	}

	private static final class TestingSlotPoolSlotProvider implements TestingSlotProvider {

		private final SlotPool slotPool;

		private final TestingScheduler scheduler;

		private final AtomicInteger numberOfLocalizedAssignments;

		private final AtomicInteger numberOfNonLocalizedAssignments;

		private final AtomicInteger numberOfUnconstrainedAssignments;

		private final AtomicInteger numberOfHostLocalizedAssignments;

		private TestingSlotPoolSlotProvider(SlotPool slotPool, TestingScheduler testingScheduler) {
			this.slotPool = Preconditions.checkNotNull(slotPool);

			this.scheduler = testingScheduler;

			this.numberOfLocalizedAssignments = new AtomicInteger();
			this.numberOfNonLocalizedAssignments = new AtomicInteger();
			this.numberOfUnconstrainedAssignments = new AtomicInteger();
			this.numberOfHostLocalizedAssignments = new AtomicInteger();
		}

		@Override
		public TaskManagerLocation addTaskManager(int numberSlots) {
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final ResourceID resourceId = taskManagerLocation.getResourceID();
			final SlotPool slotPoolGateway = slotPool;

			try {
				slotPoolGateway.registerTaskManager(resourceId);
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
				acceptedSlotOffers = slotPoolGateway.offerSlots(
					taskManagerLocation,
					taskManagerGateway,
					slotOffers);
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception occurred. This indicates a programming bug.", e);
			}

			Preconditions.checkState(acceptedSlotOffers.size() == numberSlots);

			return taskManagerLocation;
		}

		@Override
		public void releaseTaskManager(ResourceID resourceId) {
			try {
				slotPool.releaseTaskManager(resourceId, null);
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
		}

		@Override
		public int getNumberOfAvailableSlots() {
			return slotPool.getAvailableSlotsInformation().size();
		}

		@Override
		public int getNumberOfLocalizedAssignments() {
			return numberOfLocalizedAssignments.get();
		}

		@Override
		public int getNumberOfNonLocalizedAssignments() {
			return numberOfNonLocalizedAssignments.get();
		}

		@Override
		public int getNumberOfUnconstrainedAssignments() {
			return numberOfUnconstrainedAssignments.get();
		}

		@Override
		public int getNumberOfHostLocalizedAssignments() {
			return numberOfHostLocalizedAssignments.get();
		}

		@Override
		public int getNumberOfSlots(SlotSharingGroup slotSharingGroup) {
				return scheduler.getNumberOfSharedSlots(slotSharingGroup.getSlotSharingGroupId());
		}

		@Override
		public int getNumberOfAvailableSlotsForGroup(SlotSharingGroup slotSharingGroup, JobVertexID jobVertexId) {
				return scheduler.getNumberOfAvailableSlotsForGroup(slotSharingGroup.getSlotSharingGroupId(), jobVertexId);
		}

		@Override
		public void shutdown() {
			slotPool.close();
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			SlotProfile slotProfile,
			boolean allowQueued,
			Time allocationTimeout) {
			return scheduler.allocateSlot(task, allowQueued, slotProfile, allocationTimeout).thenApply(
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
				});
		}

		@Override
		public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		}
	}

	/**
	 * Test implementation of scheduler that offers a bit more introspection.
	 */
	private static final class TestingScheduler extends SchedulerImpl {

		private final Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap;

		public TestingScheduler(
			@Nonnull Map<SlotSharingGroupId, SlotSharingManager> slotSharingManagersMap,
			@Nonnull SlotSelectionStrategy slotSelectionStrategy,
			@Nonnull SlotPool slotPoolGateway) {

			super(slotSelectionStrategy, slotPoolGateway, slotSharingManagersMap);
			this.slotSharingManagersMap = slotSharingManagersMap;
		}

		public int getNumberOfSharedSlots(SlotSharingGroupId slotSharingGroupId) {
			final SlotSharingManager multiTaskSlotManager = slotSharingManagersMap.get(slotSharingGroupId);

			if (multiTaskSlotManager != null) {
				return multiTaskSlotManager.getResolvedRootSlots().size();
			} else {
				throw new FlinkRuntimeException("No MultiTaskSlotManager registered under " + slotSharingGroupId + '.');
			}
		}

		public int getNumberOfAvailableSlotsForGroup(SlotSharingGroupId slotSharingGroupId, JobVertexID jobVertexId) {
			final SlotSharingManager multiTaskSlotManager = slotSharingManagersMap.get(slotSharingGroupId);

			if (multiTaskSlotManager != null) {
				int availableSlots = 0;

				for (SlotSharingManager.MultiTaskSlot multiTaskSlot : multiTaskSlotManager.getResolvedRootSlots()) {
					if (!multiTaskSlot.contains(jobVertexId)) {
						availableSlots++;
					}
				}

				return availableSlots;
			} else {
				throw new FlinkRuntimeException("No MultiTaskSlotmanager registered under " + slotSharingGroupId + '.');
			}
		}
	}
}
