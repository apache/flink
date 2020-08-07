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

package org.apache.flink.runtime.executiongraph.utils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A testing utility slot provider that simply has a predefined pool of slots.
 */
public class SimpleSlotProvider implements SlotProvider, SlotOwner {

	private final Object lock = new Object();

	private final ArrayDeque<SlotContext> slots;

	private final HashMap<SlotRequestId, SlotContext> allocatedSlots;

	public SimpleSlotProvider(int numSlots) {
		this(numSlots, new SimpleAckingTaskManagerGateway());
	}

	public SimpleSlotProvider(int numSlots, TaskManagerGateway taskManagerGateway) {
		checkArgument(numSlots >= 0, "numSlots must be >= 0");

		this.slots = new ArrayDeque<>(numSlots);

		for (int i = 0; i < numSlots; i++) {
			SimpleSlotContext as = new SimpleSlotContext(
				new AllocationID(),
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + i),
				0,
				taskManagerGateway,
				ResourceProfile.ANY);
			slots.add(as);
		}

		allocatedSlots = new HashMap<>(slots.size());
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			SlotProfile slotProfile,
			Time allocationTimeout) {
		final SlotContext slot;

		synchronized (lock) {
			if (slots.isEmpty()) {
				slot = null;
			} else {
				slot = slots.removeFirst();
			}
			if (slot != null) {
				TestingLogicalSlot result = new TestingLogicalSlotBuilder()
					.setTaskManagerLocation(slot.getTaskManagerLocation())
					.setTaskManagerGateway(slot.getTaskManagerGateway())
					.setSlotNumber(slot.getPhysicalSlotNumber())
					.setAllocationId(slot.getAllocationId())
					.setSlotRequestId(slotRequestId)
					.setSlotSharingGroupId(task.getSlotSharingGroupId())
					.setSlotOwner(this)
					.createTestingLogicalSlot();
				allocatedSlots.put(slotRequestId, slot);
				return CompletableFuture.completedFuture(result);
			} else {
				return FutureUtils.completedExceptionally(new NoResourceAvailableException());
			}
		}
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		synchronized (lock) {
			final SlotContext slotContext = allocatedSlots.remove(slotRequestId);

			if (slotContext != null) {
				slots.add(slotContext);
			} else {
				throw new FlinkRuntimeException("Unknown slot request id " + slotRequestId + '.');
			}
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		synchronized (lock) {
			SimpleSlotContext as = new SimpleSlotContext(
				logicalSlot.getAllocationId(),
				logicalSlot.getTaskManagerLocation(),
				logicalSlot.getPhysicalSlotNumber(),
				logicalSlot.getTaskManagerGateway(),
				ResourceProfile.ANY);

			slots.add(as);
			allocatedSlots.remove(logicalSlot.getSlotRequestId());
		}
	}

	public int getNumberOfAvailableSlots() {
		synchronized (lock) {
			return slots.size();
		}
	}
}
