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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A SimpleSlot represents a single slot on a TaskManager instance, or a slot within a shared slot.
 *
 * <p>If this slot is part of a {@link SharedSlot}, then the parent attribute will point to that shared slot.
 * If not, then the parent attribute is null.
 */
public class SimpleSlot extends Slot implements LogicalSlot {

	/** The updater used to atomically swap in the payload */
	private static final AtomicReferenceFieldUpdater<SimpleSlot, Payload> PAYLOAD_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(SimpleSlot.class, Payload.class, "payload");

	// ------------------------------------------------------------------------

	private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

	/** Id of the task being executed in the slot. Volatile to force a memory barrier and allow for correct double-checking */
	private volatile Payload payload;

	/** The locality attached to the slot, defining whether the slot was allocated at the desired location. */
	private volatile Locality locality = Locality.UNCONSTRAINED;

	// ------------------------------------------------------------------------
	//  Old Constructors (legacy mode)
	// ------------------------------------------------------------------------

	/**
	 * Creates a new simple slot that stands alone and does not belong to shared slot.
	 * 
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the task slot on the instance.
	 * @param taskManagerGateway The gateway to communicate with the TaskManager of this slot
	 */
	public SimpleSlot(
		SlotOwner owner, TaskManagerLocation location, int slotNumber,
		TaskManagerGateway taskManagerGateway) {
		this(owner, location, slotNumber, taskManagerGateway, null, null);
	}

	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID.
	 *
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of the simple slot in its parent shared slot.
	 * @param taskManagerGateway to communicate with the associated task manager.
	 * @param parent The parent shared slot.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	public SimpleSlot(
			SlotOwner owner,
			TaskManagerLocation location,
			int slotNumber,
			TaskManagerGateway taskManagerGateway,
			@Nullable SharedSlot parent,
			@Nullable AbstractID groupID) {

		super(
			parent != null ?
				parent.getSlotContext() :
				new SimpleSlotContext(
					NO_ALLOCATION_ID,
					location,
					slotNumber,
					taskManagerGateway),
			owner,
			slotNumber,
			parent,
			groupID);
	}

	// ------------------------------------------------------------------------
	//  Constructors
	// ------------------------------------------------------------------------

	/**
	 * Creates a new simple slot that stands alone and does not belong to shared slot.
	 *
	 * @param slotContext The slot context of this simple slot
	 * @param owner The component from which this slot is allocated.
	 */
	public SimpleSlot(SlotContext slotContext, SlotOwner owner, int slotNumber) {
		this(slotContext, owner, slotNumber, null, null);
	}

	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID..
	 *
	 * @param parent The parent shared slot.
	 * @param owner The component from which this slot is allocated.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	public SimpleSlot(SharedSlot parent, SlotOwner owner, int slotNumber, AbstractID groupID) {
		this(parent.getSlotContext(), owner, slotNumber, parent, groupID);
	}
	
	/**
	 * Creates a new simple slot that belongs to the given shared slot and
	 * is identified by the given ID..
	 *
	 * @param slotContext The slot context of this simple slot
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of the simple slot in its parent shared slot.
	 * @param parent The parent shared slot.
	 * @param groupID The ID that identifies the group that the slot belongs to.
	 */
	private SimpleSlot(
			SlotContext slotContext,
			SlotOwner owner,
			int slotNumber,
			@Nullable SharedSlot parent,
			@Nullable AbstractID groupID) {
		super(slotContext, owner, slotNumber, parent, groupID);
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	@Override
	public int getNumberLeaves() {
		return 1;
	}

	/**
	 * Atomically sets the executed vertex, if no vertex has been assigned to this slot so far.
	 *
	 * @param payload The vertex to assign to this slot.
	 * @return True, if the vertex was assigned, false, otherwise.
	 */
	@Override
	public boolean tryAssignPayload(Payload payload) {
		Preconditions.checkNotNull(payload);

		// check that we can actually run in this slot
		if (isCanceled()) {
			return false;
		}

		// atomically assign the vertex
		if (!PAYLOAD_UPDATER.compareAndSet(this, null, payload)) {
			return false;
		}

		// we need to do a double check that we were not cancelled in the meantime
		if (isCanceled()) {
			this.payload = null;
			return false;
		}

		return true;
	}

	@Nullable
	@Override
	public Payload getPayload() {
		return payload;
	}

	/**
	 * Gets the locality information attached to this slot.
	 * @return The locality attached to the slot.
	 */
	public Locality getLocality() {
		return locality;
	}

	/**
	 * Attached locality information to this slot.
	 * @param locality The locality attached to the slot.
	 */
	public void setLocality(Locality locality) {
		this.locality = locality;
	}

	// ------------------------------------------------------------------------
	//  Cancelling & Releasing
	// ------------------------------------------------------------------------

	@Override
	public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
		if (!isCanceled()) {
			final CompletableFuture<?> terminationFuture;

			if (payload != null) {
				// trigger the failure of the slot payload
				payload.fail(cause != null ? cause : new FlinkException("TaskManager was lost/killed: " + getTaskManagerLocation()));

				// wait for the termination of the payload before releasing the slot
				terminationFuture = payload.getTerminalStateFuture();
			} else {
				terminationFuture = CompletableFuture.completedFuture(null);
			}

			terminationFuture.whenComplete(
				(Object ignored, Throwable throwable) -> {
					// release directly (if we are directly allocated),
					// otherwise release through the parent shared slot
					if (getParent() == null) {
						// we have to give back the slot to the owning instance
						if (markCancelled()) {
							getOwner().returnAllocatedSlot(this).whenComplete(
								(value, t) -> {
									if (t != null) {
										releaseFuture.completeExceptionally(t);
									} else {
										releaseFuture.complete(null);
									}
								});
						}
					} else {
						// we have to ask our parent to dispose us
						getParent().releaseChild(this);

						releaseFuture.complete(null);
					}
				});
		}

		return releaseFuture;
	}

	@Override
	public int getPhysicalSlotNumber() {
		return getRootSlotNumber();
	}

	@Override
	public AllocationID getAllocationId() {
		return getSlotContext().getAllocationId();
	}

	@Override
	public SlotRequestId getSlotRequestId() {
		return NO_SLOT_REQUEST_ID;
	}

	@Nullable
	@Override
	public SlotSharingGroupId getSlotSharingGroupId() {
		return NO_SLOT_SHARING_GROUP_ID;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "SimpleSlot " + super.toString();
	}
}
