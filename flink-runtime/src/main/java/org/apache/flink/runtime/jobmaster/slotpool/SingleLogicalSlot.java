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

import org.apache.flink.annotation.VisibleForTesting;
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
 * Implementation of the {@link LogicalSlot} which is used by the {@link SlotPoolImpl}.
 */
public class SingleLogicalSlot implements LogicalSlot, PhysicalSlot.Payload {

	private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, Payload> PAYLOAD_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		SingleLogicalSlot.class,
		Payload.class,
		"payload");

	private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, State> STATE_UPDATER = AtomicReferenceFieldUpdater.newUpdater(
		SingleLogicalSlot.class,
		State.class,
		"state");

	private final SlotRequestId slotRequestId;

	private final SlotContext slotContext;

	// null if the logical slot does not belong to a slot sharing group, otherwise non-null
	@Nullable
	private final SlotSharingGroupId slotSharingGroupId;

	// locality of this slot wrt the requested preferred locations
	private final Locality locality;

	// owner of this slot to which it is returned upon release
	private final SlotOwner slotOwner;

	private final CompletableFuture<Void> releaseFuture;

	private volatile State state;

	// LogicalSlot.Payload of this slot
	private volatile Payload payload;

	/** Whether this logical slot will be occupied indefinitely. */
	private boolean willBeOccupiedIndefinitely;

	@VisibleForTesting
	public SingleLogicalSlot(
		SlotRequestId slotRequestId,
		SlotContext slotContext,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		Locality locality,
		SlotOwner slotOwner) {

		this(slotRequestId, slotContext, slotSharingGroupId, locality, slotOwner, true);
	}

	public SingleLogicalSlot(
			SlotRequestId slotRequestId,
			SlotContext slotContext,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			Locality locality,
			SlotOwner slotOwner,
			boolean willBeOccupiedIndefinitely) {
		this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
		this.slotContext = Preconditions.checkNotNull(slotContext);
		this.slotSharingGroupId = slotSharingGroupId;
		this.locality = Preconditions.checkNotNull(locality);
		this.slotOwner = Preconditions.checkNotNull(slotOwner);
		this.willBeOccupiedIndefinitely = willBeOccupiedIndefinitely;
		this.releaseFuture = new CompletableFuture<>();

		this.state = State.ALIVE;
		this.payload = null;
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
		return state == State.ALIVE;
	}

	@Override
	public boolean tryAssignPayload(Payload payload) {
		return PAYLOAD_UPDATER.compareAndSet(this, null, payload);
	}

	@Nullable
	@Override
	public Payload getPayload() {
		return payload;
	}

	@Override
	public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
			signalPayloadRelease(cause);
			returnSlotToOwner(payload.getTerminalStateFuture());
		}

		return releaseFuture;
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

	public static SingleLogicalSlot allocateFromPhysicalSlot(
			final SlotRequestId slotRequestId,
			final PhysicalSlot physicalSlot,
			final Locality locality,
			final SlotOwner slotOwner,
			final boolean slotWillBeOccupiedIndefinitely) {

		final SingleLogicalSlot singleTaskSlot = new SingleLogicalSlot(
			slotRequestId,
			physicalSlot,
			null,
			locality,
			slotOwner,
			slotWillBeOccupiedIndefinitely);

		if (physicalSlot.tryAssignPayload(singleTaskSlot)) {
			return singleTaskSlot;
		} else {
			throw new IllegalStateException("BUG: Unexpected physical slot payload assignment failure!");
		}
	}

	// -------------------------------------------------------------------------
	// AllocatedSlot.Payload implementation
	// -------------------------------------------------------------------------

	/**
	 * A release of the payload by the {@link AllocatedSlot} triggers a release of the payload of
	 * the logical slot.
	 *
	 * @param cause of the payload release
	 */
	@Override
	public void release(Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
			signalPayloadRelease(cause);
		}
		markReleased();
		releaseFuture.complete(null);
	}

	@Override
	public boolean willOccupySlotIndefinitely() {
		return willBeOccupiedIndefinitely;
	}

	private void signalPayloadRelease(Throwable cause) {
		tryAssignPayload(TERMINATED_PAYLOAD);
		payload.fail(cause);
	}

	private void returnSlotToOwner(CompletableFuture<?> terminalStateFuture) {
		terminalStateFuture
			.whenComplete((Object ignored, Throwable throwable) -> {

				if (state == State.RELEASING) {
					slotOwner.returnLogicalSlot(this);
				}

				markReleased();

				if (throwable != null) {
					releaseFuture.completeExceptionally(throwable);
				} else {
					releaseFuture.complete(null);
				}
			});
	}

	private void markReleased() {
		state = State.RELEASED;
	}

	// -------------------------------------------------------------------------
	// Internal classes
	// -------------------------------------------------------------------------

	enum State {
		ALIVE,
		RELEASING,
		RELEASED
	}
}
