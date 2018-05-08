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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Implementation of the {@link LogicalSlot} which is used by the {@link SlotPool}.
 */
public class SingleLogicalSlot implements LogicalSlot, AllocatedSlot.Payload {

	private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, Payload> PAYLOAD_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		SingleLogicalSlot.class,
		Payload.class,
		"payload");

	private final SlotRequestId slotRequestId;

	private final SlotContext slotContext;

	// null if the logical slot does not belong to a slot sharing group, otherwise non-null
	@Nullable
	private final SlotSharingGroupId slotSharingGroupId;

	// locality of this slot wrt the requested preferred locations
	private final Locality locality;

	// owner of this slot to which it is returned upon release
	private final SlotOwner slotOwner;

	// LogicalSlot.Payload of this slot
	private volatile Payload payload;

	public SingleLogicalSlot(
			SlotRequestId slotRequestId,
			SlotContext slotContext,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			Locality locality,
			SlotOwner slotOwner) {
		this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
		this.slotContext = Preconditions.checkNotNull(slotContext);
		this.slotSharingGroupId = slotSharingGroupId;
		this.locality = Preconditions.checkNotNull(locality);
		this.slotOwner = Preconditions.checkNotNull(slotOwner);

		payload = null;
	}

	@Override
	public TaskManagerLocation getTaskManagerLocation() {
		return slotContext.getTaskManagerLocation();
	}

	@Override
	public TaskManagerGateway getTaskManagerGateway() {
		return slotContext.getTaskManagerGateway();
	}

	@Override
	public Locality getLocality() {
		return locality;
	}

	@Override
	public boolean isAlive() {
		final Payload currentPayload = payload;

		if (currentPayload != null) {
			return !currentPayload.getTerminalStateFuture().isDone();
		} else {
			// We are always alive if there is no payload assigned yet.
			// If this slot is released and no payload is assigned, then the TERMINATED_PAYLOAD is assigned
			return true;
		}
	}

	@Override
	public boolean tryAssignPayload(Payload payload) {
		Preconditions.checkNotNull(payload);
		return PAYLOAD_UPDATER.compareAndSet(this, null, payload);
	}

	@Nullable
	@Override
	public Payload getPayload() {
		return payload;
	}

	@Override
	public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
		// set an already terminated payload if the payload of this slot is still empty
		tryAssignPayload(TERMINATED_PAYLOAD);

		// notify the payload that the slot will be released
		payload.fail(cause);

		// Wait until the payload has been terminated. Only then, we return the slot to its rightful owner
		return payload.getTerminalStateFuture()
			.whenComplete((Object ignored, Throwable throwable) -> slotOwner.returnAllocatedSlot(this));
	}

	@Override
	public int getPhysicalSlotNumber() {
		return slotContext.getPhysicalSlotNumber();
	}

	@Override
	public AllocationID getAllocationId() {
		return slotContext.getAllocationId();
	}

	@Override
	public SlotRequestId getSlotRequestId() {
		return slotRequestId;
	}

	@Nullable
	@Override
	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	// -------------------------------------------------------------------------
	// AllocatedSlot.Payload implementation
	// -------------------------------------------------------------------------

	/**
	 * A release of the payload by the {@link AllocatedSlot} triggers a release of the payload of
	 * the logical slot.
	 *
	 * @param cause of the payload release
	 * @return true if the logical slot's payload could be released, otherwise false
	 */
	@Override
	public boolean release(Throwable cause) {
		return releaseSlot(cause).isDone();
	}
}
