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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Testing implementation of {@link SlotProvider}.
 */
public class TestingSlotPoolSlotProvider implements SlotProvider {

	private final TestingSlotPool slotPool;

	private final SlotProvider slotProvider;

	private final AtomicInteger numberOfLocalizedAssignments;

	private final AtomicInteger numberOfNonLocalizedAssignments;

	private final AtomicInteger numberOfUnconstrainedAssignments;

	private final AtomicInteger numberOfHostLocalizedAssignments;

	public TestingSlotPoolSlotProvider(TestingSlotPool slotPool) {
		this.slotPool = Preconditions.checkNotNull(slotPool);
		this.slotProvider = slotPool.getSlotProvider();

		this.numberOfLocalizedAssignments = new AtomicInteger();
		this.numberOfNonLocalizedAssignments = new AtomicInteger();
		this.numberOfUnconstrainedAssignments = new AtomicInteger();
		this.numberOfHostLocalizedAssignments = new AtomicInteger();
	}

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

	public void releaseTaskManager(ResourceID resourceId) {
		try {
			slotPool.releaseTaskManager(resourceId, null).get();
		} catch (Exception e) {
			throw new RuntimeException("Should not have happened.", e);
		}
	}

	public int getNumberOfAvailableSlots() {
		try {
			return slotPool.getNumberOfAvailableSlots().get();
		} catch (Exception e) {
			throw new RuntimeException("Should not have happened.", e);
		}
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

	public int getNumberOfSlots(SlotSharingGroup slotSharingGroup) {
		try {
			return slotPool.getNumberOfSharedSlots(slotSharingGroup.getSlotSharingGroupId()).get();
		} catch (Exception e) {
			throw new RuntimeException("Should not have happened.", e);
		}
	}

	public int getNumberOfAvailableSlotsForGroup(SlotSharingGroup slotSharingGroup, JobVertexID jobVertexId) {
		try {
			return slotPool.getNumberOfAvailableSlotsForGroup(slotSharingGroup.getSlotSharingGroupId(), jobVertexId).get();
		} catch (Exception e) {
			throw new RuntimeException("Should not have happened.", e);
		}
	}

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
