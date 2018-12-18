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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A testing utility slot provider that simply has a predefined pool of slots.
 */
public class SimpleSlotProvider implements SlotProvider, SlotOwner {

	private final Object lock = new Object();

	private final ArrayDeque<SlotContext> slots;

	private final HashMap<SlotRequestId, SlotContext> allocatedSlots;

	public SimpleSlotProvider(JobID jobId, int numSlots) {
		this(jobId, numSlots, new SimpleAckingTaskManagerGateway());
	}

	public SimpleSlotProvider(JobID jobId, int numSlots, TaskManagerGateway taskManagerGateway) {
		checkNotNull(jobId, "jobId");
		checkArgument(numSlots >= 0, "numSlots must be >= 0");

		this.slots = new ArrayDeque<>(numSlots);

		for (int i = 0; i < numSlots; i++) {
			SimpleSlotContext as = new SimpleSlotContext(
				new AllocationID(),
				new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + i),
				0,
				taskManagerGateway);
			slots.add(as);
		}

		allocatedSlots = new HashMap<>(slots.size());
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			boolean allowQueued,
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
				SimpleSlot result = new SimpleSlot(slot, this, 0);
				allocatedSlots.put(slotRequestId, slot);
				return CompletableFuture.completedFuture(result);
			}
			else {
				return FutureUtils.completedExceptionally(new NoResourceAvailableException());
			}
		}
	}

	@Override
	public CompletableFuture<Acknowledge> cancelSlotRequest(SlotRequestId slotRequestId, @Nullable SlotSharingGroupId slotSharingGroupId, Throwable cause) {
		synchronized (lock) {
			final SlotContext slotContext = allocatedSlots.remove(slotRequestId);

			if (slotContext != null) {
				slots.add(slotContext);
				return CompletableFuture.completedFuture(Acknowledge.get());
			} else {
				return FutureUtils.completedExceptionally(new FlinkException("Unknown slot request id " + slotRequestId + '.'));
			}
		}
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {
		Preconditions.checkArgument(logicalSlot instanceof Slot);

		final Slot slot = ((Slot) logicalSlot);

		synchronized (lock) {
			slots.add(slot.getSlotContext());
			allocatedSlots.remove(logicalSlot.getSlotRequestId());
		}
		return CompletableFuture.completedFuture(true);
	}

	public int getNumberOfAvailableSlots() {
		synchronized (lock) {
			return slots.size();
		}
	}
}
