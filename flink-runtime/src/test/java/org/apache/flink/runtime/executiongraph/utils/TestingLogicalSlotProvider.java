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
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.AllocatedSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A testing utility slot provider that simply has a predefined pool of slots.
 */
public class TestingLogicalSlotProvider implements SlotProvider, SlotOwner {

	private final Object lock = new Object();

	private final ArrayDeque<SlotContext> slots;

	private final HashMap<SlotRequestId, TestingLogicalSlot> allocatedSlots;

	public TestingLogicalSlotProvider(int numSlots) {
		this(numSlots, new SimpleAckingTaskManagerGateway());
	}

	public TestingLogicalSlotProvider(int numSlots, TaskManagerGateway taskManagerGateway) {
		checkArgument(numSlots >= 0, "numSlots must be >= 0");

		this.slots = new ArrayDeque<>(numSlots);

		for (int i = 0; i < numSlots; i++) {
			AllocatedSlot as = new AllocatedSlot(
				new AllocationID(),
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + i),
				0,
				ResourceProfile.UNKNOWN,
				taskManagerGateway);
			slots.add(as);
		}

		allocatedSlots = new HashMap<>(slots.size());
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			SlotProfile slotProfile,
			boolean allowQueued,
			Time allocationTimeout) {
		final SlotContext slot;

		synchronized (lock) {
			if (slots.isEmpty()) {
				slot = null;
			} else {
				slot = slots.removeFirst();
			}
			if (slot != null) {
				TestingLogicalSlot result = new TestingLogicalSlot(
					slot.getTaskManagerLocation(),
					slot.getTaskManagerGateway(),
					slot.getPhysicalSlotNumber(),
					slot.getAllocationId(),
					slotRequestId,
					new SlotSharingGroupId(),
					null,
					this);
				allocatedSlots.put(slotRequestId, result);
				return CompletableFuture.completedFuture(result);
			}
			else {
				return FutureUtils.completedExceptionally(new NoResourceAvailableException());
			}
		}
	}

	@Override
	public void cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		synchronized (lock) {
			final TestingLogicalSlot logicalSlot = allocatedSlots.remove(slotRequestId);

			if (logicalSlot != null) {
				SlotContext as = getSlotContext(logicalSlot);
				slots.add(as);
			} else {
				throw new FlinkRuntimeException("Unknown slot request id " + slotRequestId + '.');
			}
		}
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		synchronized (lock) {
			allocatedSlots.remove(logicalSlot.getSlotRequestId());

			SlotContext as = getSlotContext(logicalSlot);
			slots.add(as);
		}
	}

	public int getNumberOfAvailableSlots() {
		synchronized (lock) {
			return slots.size();
		}
	}

	public void releaseAllocatedSlots() {
		synchronized (lock) {
			List<TestingLogicalSlot> slots = new ArrayList<>(allocatedSlots.values());

			for (TestingLogicalSlot slot : slots) {
				slot.releaseSlot(new Exception("Test Exception"));
			}
		}
	}

	private SlotContext getSlotContext(LogicalSlot logicalSlot) {
		return new AllocatedSlot(
			logicalSlot.getAllocationId(),
			logicalSlot.getTaskManagerLocation(),
			logicalSlot.getPhysicalSlotNumber(),
			ResourceProfile.UNKNOWN,
			logicalSlot.getTaskManagerGateway());
	}
}
