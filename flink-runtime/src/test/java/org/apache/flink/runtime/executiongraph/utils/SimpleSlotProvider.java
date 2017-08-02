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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A testing utility slot provider that simply has a predefined pool of slots.
 */
public class SimpleSlotProvider implements SlotProvider, SlotOwner {

	private final ArrayDeque<AllocatedSlot> slots;

	public SimpleSlotProvider(JobID jobId, int numSlots) {
		this(jobId, numSlots, new SimpleAckingTaskManagerGateway());
	}

	public SimpleSlotProvider(JobID jobId, int numSlots, TaskManagerGateway taskManagerGateway) {
		checkNotNull(jobId, "jobId");
		checkArgument(numSlots >= 0, "numSlots must be >= 0");

		this.slots = new ArrayDeque<>(numSlots);

		for (int i = 0; i < numSlots; i++) {
			AllocatedSlot as = new AllocatedSlot(
					new AllocationID(),
					jobId,
					new TaskManagerLocation(ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + i),
					0,
					ResourceProfile.UNKNOWN,
					taskManagerGateway);
			slots.add(as);
		}
	}

	@Override
	public CompletableFuture<SimpleSlot> allocateSlot(ScheduledUnit task, boolean allowQueued) {
		final AllocatedSlot slot;

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
	public boolean returnAllocatedSlot(Slot slot) {
		synchronized (slots) {
			slots.add(slot.getAllocatedSlot());
		}
		return true;
	}

	public int getNumberOfAvailableSlots() {
		synchronized (slots) {
			return slots.size();
		}
	}
}
