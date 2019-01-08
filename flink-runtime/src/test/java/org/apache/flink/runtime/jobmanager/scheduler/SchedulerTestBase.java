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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSchedulingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolGateway;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSharingManager;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestBase.SchedulerType.SCHEDULER;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestBase.SchedulerType.SLOT_POOL;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;

/**
 * Test base for scheduler related test cases. The test are
 * executed with the {@link Scheduler} and the {@link SlotPool}.
 */
public class SchedulerTestBase extends TestLogger {

	protected TestingSlotProvider testingSlotProvider;

	protected SchedulerType schedulerType;

	private RpcService rpcService;

	public enum SchedulerType {
		SCHEDULER,
		SLOT_POOL
	}

	@Parameterized.Parameters(name = "Scheduler type = {0}")
	public static Collection<Object[]> schedulerTypes() {
		return Arrays.asList(
			new Object[]{SCHEDULER},
			new Object[]{SLOT_POOL});
	}

	protected SchedulerTestBase(SchedulerType schedulerType) {
		this.schedulerType = Preconditions.checkNotNull(schedulerType);
		rpcService = null;
	}

	@Before
	public void setup() throws Exception {
		switch (schedulerType) {
			case SCHEDULER:
				testingSlotProvider = new TestingSchedulerSlotProvider(
					new Scheduler(
						TestingUtils.defaultExecutionContext()));
				break;
			case SLOT_POOL:
				rpcService = new TestingRpcService();
				final JobID jobId = new JobID();
				final TestingSlotPool slotPool = new TestingSlotPool(
					rpcService,
					jobId,
					LocationPreferenceSchedulingStrategy.getInstance());
				testingSlotProvider = new TestingSlotPoolSlotProvider(slotPool);

				final JobMasterId jobMasterId = JobMasterId.generate();
				final String jobManagerAddress = "localhost";
				slotPool.start(jobMasterId, jobManagerAddress);
				break;
		}
	}

	@After
	public void teardown() throws Exception {
		if (testingSlotProvider != null) {
			testingSlotProvider.shutdown();
			testingSlotProvider = null;
		}

		if (rpcService != null) {
			rpcService.stopService().get();
			rpcService = null;
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

	private static final class TestingSchedulerSlotProvider implements TestingSlotProvider {
		private final Scheduler scheduler;

		private TestingSchedulerSlotProvider(Scheduler scheduler) {
			this.scheduler = Preconditions.checkNotNull(scheduler);
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			boolean allowQueued,
			SlotProfile slotProfile,
			Time allocationTimeout) {
			return scheduler.allocateSlot(task, allowQueued, slotProfile, allocationTimeout);
		}

		@Override
		public CompletableFuture<Acknowledge> cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public TaskManagerLocation addTaskManager(int numberSlots) {
			final Instance instance = getRandomInstance(numberSlots);
			scheduler.newInstanceAvailable(instance);

			return instance.getTaskManagerLocation();
		}

		@Override
		public void releaseTaskManager(ResourceID resourceId) {
			final Instance instance = scheduler.getInstance(resourceId);

			if (instance != null) {
				scheduler.instanceDied(instance);
			}
		}

		@Override
		public int getNumberOfAvailableSlots() {
			return scheduler.getNumberOfAvailableSlots();
		}

		@Override
		public int getNumberOfLocalizedAssignments() {
			return scheduler.getNumberOfLocalizedAssignments();
		}

		@Override
		public int getNumberOfNonLocalizedAssignments() {
			return scheduler.getNumberOfNonLocalizedAssignments();
		}

		@Override
		public int getNumberOfUnconstrainedAssignments() {
			return scheduler.getNumberOfUnconstrainedAssignments();
		}

		@Override
		public int getNumberOfHostLocalizedAssignments() {
			return 0;
		}

		@Override
		public int getNumberOfSlots(SlotSharingGroup slotSharingGroup) {
			return slotSharingGroup.getTaskAssignment().getNumberOfSlots();
		}

		@Override
		public int getNumberOfAvailableSlotsForGroup(SlotSharingGroup slotSharingGroup, JobVertexID jobVertexId) {
			return slotSharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForGroup(jobVertexId);
		}

		@Override
		public void shutdown() {
			scheduler.shutdown();
		}
	}

	private static final class TestingSlotPoolSlotProvider implements TestingSlotProvider {

		private final TestingSlotPool slotPool;

		private final SlotProvider slotProvider;

		private final AtomicInteger numberOfLocalizedAssignments;

		private final AtomicInteger numberOfNonLocalizedAssignments;

		private final AtomicInteger numberOfUnconstrainedAssignments;

		private final AtomicInteger numberOfHostLocalizedAssignments;

		private TestingSlotPoolSlotProvider(TestingSlotPool slotPool) {
			this.slotPool = Preconditions.checkNotNull(slotPool);
			this.slotProvider = slotPool.getSlotProvider();

			this.numberOfLocalizedAssignments = new AtomicInteger();
			this.numberOfNonLocalizedAssignments = new AtomicInteger();
			this.numberOfUnconstrainedAssignments = new AtomicInteger();
			this.numberOfHostLocalizedAssignments = new AtomicInteger();
		}

		@Override
		public TaskManagerLocation addTaskManager(int numberSlots) {
			final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			final ResourceID resourceId = taskManagerLocation.getResourceID();
			final SlotPoolGateway slotPoolGateway = slotPool.getSelfGateway(SlotPoolGateway.class);

			try {
				slotPoolGateway.registerTaskManager(resourceId).get();
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception occurred. This indicates a programming bug.", e);
			}

			final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
			final Collection<SlotOffer> slotOffers = new ArrayList<>(numberSlots);

			for (int i = 0; i < numberSlots; i++) {
				final SlotOffer slotOffer = new SlotOffer(
					new AllocationID(),
					i,
					ResourceProfile.UNKNOWN);

				slotOffers.add(slotOffer);
			}

			final Collection<SlotOffer> acceptedSlotOffers;

			try {
				acceptedSlotOffers = slotPoolGateway.offerSlots(
					taskManagerLocation,
					taskManagerGateway,
					slotOffers).get();
			} catch (Exception e) {
				throw new RuntimeException("Unexpected exception occurred. This indicates a programming bug.", e);
			}

			Preconditions.checkState(acceptedSlotOffers.size() == numberSlots);

			return taskManagerLocation;
		}

		@Override
		public void releaseTaskManager(ResourceID resourceId) {
			try {
				slotPool.releaseTaskManager(resourceId, null).get();
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
		}

		@Override
		public int getNumberOfAvailableSlots() {
			try {
				return slotPool.getNumberOfAvailableSlots().get();
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
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
			try {
				return slotPool.getNumberOfSharedSlots(slotSharingGroup.getSlotSharingGroupId()).get();
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
		}

		@Override
		public int getNumberOfAvailableSlotsForGroup(SlotSharingGroup slotSharingGroup, JobVertexID jobVertexId) {
			try {
				return slotPool.getNumberOfAvailableSlotsForGroup(slotSharingGroup.getSlotSharingGroupId(), jobVertexId).get();
			} catch (Exception e) {
				throw new RuntimeException("Should not have happened.", e);
			}
		}

		@Override
		public void shutdown() throws Exception {
			RpcUtils.terminateRpcEndpoint(slotPool, TestingUtils.TIMEOUT());
		}

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			boolean allowQueued,
			SlotProfile slotProfile,
			Time allocationTimeout) {
			return slotProvider.allocateSlot(task, allowQueued, slotProfile, allocationTimeout).thenApply(
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
		public CompletableFuture<Acknowledge> cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}
	}

	private static final class TestingSlotPool extends SlotPool {

		public TestingSlotPool(RpcService rpcService, JobID jobId, SchedulingStrategy schedulingStrategy) {
			super(rpcService, jobId, schedulingStrategy);
		}

		CompletableFuture<Integer> getNumberOfAvailableSlots() {
			return callAsync(
				() -> getAvailableSlots().size(),
				TestingUtils.infiniteTime());
		}

		CompletableFuture<Integer> getNumberOfSharedSlots(SlotSharingGroupId slotSharingGroupId) {
			return callAsync(
				() -> {
					final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

					if (multiTaskSlotManager != null) {
						return multiTaskSlotManager.getResolvedRootSlots().size();
					} else {
						throw new FlinkException("No MultiTaskSlotManager registered under " + slotSharingGroupId + '.');
					}
				},
				TestingUtils.infiniteTime());
		}

		CompletableFuture<Integer> getNumberOfAvailableSlotsForGroup(SlotSharingGroupId slotSharingGroupId, JobVertexID jobVertexId) {
			return callAsync(
				() -> {
					final SlotSharingManager multiTaskSlotManager = slotSharingManagers.get(slotSharingGroupId);

					if (multiTaskSlotManager != null) {
						int availableSlots = 0;

						for (SlotSharingManager.MultiTaskSlot multiTaskSlot : multiTaskSlotManager.getResolvedRootSlots()) {
							if (!multiTaskSlot.contains(jobVertexId)) {
								availableSlots++;
							}
						}

						return availableSlots;
					} else {
						throw new FlinkException("No MultiTaskSlotmanager registered under " + slotSharingGroupId + '.');
					}
				},
				TestingUtils.infiniteTime());
		}
	}

}
