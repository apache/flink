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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A testing utility slot provider that simply has a predefined pool of slots.
 */
public class SimpleSlotProvider implements SlotProvider, SlotOwner {

	private final ArrayDeque<SlotContext> slots;

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
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			ScheduledUnit task,
			boolean allowQueued,
			Collection<TaskManagerLocation> preferredLocations) {
		final SlotContext slot;

		synchronized (slots) {
			if (slots.isEmpty()) {
				slot = null;
			} else {
				slot = slots.removeFirst();
			}
		}

		if (slot != null) {
			SimpleSlot result = new SimpleSlot(slot, this, 0);
			return CompletableFuture.completedFuture(result);
		}
		else {
			return FutureUtils.completedExceptionally(new NoResourceAvailableException());
		}
	}

	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {
		Preconditions.checkArgument(logicalSlot instanceof Slot);

		final Slot slot = ((Slot) logicalSlot);

		synchronized (slots) {
			slots.add(slot.getSlotContext());
		}
		return CompletableFuture.completedFuture(true);
	}

	public int getNumberOfAvailableSlots() {
		synchronized (slots) {
			return slots.size();
		}
	}
}
