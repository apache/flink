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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for slots that the Scheduler / ExecutionGraph take from the SlotPool and use to place
 * tasks to execute into. A slot corresponds to an AllocatedSlot (a slice of a TaskManager's resources),
 * plus additional fields to track what is currently executed in that slot, or if the slot is still
 * used or disposed (ExecutionGraph gave it back to the pool).
 *
 * <p>In the simplest case, a slot holds a single task ({@link SimpleSlot}). In the more complex
 * case, a slot is shared ({@link SharedSlot}) and contains a set of tasks. Shared slots may contain
 * other shared slots which in turn can hold simple slots. That way, a shared slot may define a tree
 * of slots that belong to it.
 */
public abstract class Slot {

	/** Updater for atomic state transitions */
	private static final AtomicIntegerFieldUpdater<Slot> STATUS_UPDATER =
			AtomicIntegerFieldUpdater.newUpdater(Slot.class, "status");

	/** State where slot is fresh and alive. Tasks may be added to the slot. */
	private static final int ALLOCATED_AND_ALIVE = 0;

	/** State where the slot has been canceled and is in the process of being released */
	private static final int CANCELLED = 1;

	/** State where all tasks in this slot have been canceled and the slot been given back to the instance */
	private static final int RELEASED = 2;

	// temporary placeholder for Slots that are not constructed from an AllocatedSlot (by legacy code)
	protected static final AllocationID NO_ALLOCATION_ID = new AllocationID(0L, 0L);
	protected static final SlotRequestId NO_SLOT_REQUEST_ID = new SlotRequestId(0L, 0L);
	protected static final SlotSharingGroupId NO_SLOT_SHARING_GROUP_ID = new SlotSharingGroupId(0L, 0L);

	// ------------------------------------------------------------------------

	/** Context of this logical slot. */
	private final SlotContext slotContext;

	/** The owner of this slot - the slot was taken from that owner and must be disposed to it */
	private final SlotOwner owner;

	/** The parent of this slot in the hierarchy, or null, if this is the parent */
	@Nullable
	private final SharedSlot parent;

	/** The id of the group that this slot is allocated to. May be null. */
	@Nullable
	private final AbstractID groupID;

	private final int slotNumber;

	/** The state of the vertex, only atomically updated */
	private volatile int status = ALLOCATED_AND_ALIVE;

	// --------------------------------------------------------------------------------------------

	/**
	 * Base constructor for slots.
	 * 
	 * <p>This is the old way of constructing slots by the legacy code
	 * 
	 * @param owner The component from which this slot is allocated.
	 * @param location The location info of the TaskManager where the slot was allocated from
	 * @param slotNumber The number of this slot.
	 * @param taskManagerGateway The actor gateway to communicate with the TaskManager
	 * @param parent The parent slot that contains this slot. May be null, if this slot is the root.
	 * @param groupID The ID that identifies the task group for which this slot is allocated. May be null
	 *                if the slot does not belong to any task group.   
	 */
	protected Slot(
			SlotOwner owner,
			TaskManagerLocation location,
			int slotNumber,
			TaskManagerGateway taskManagerGateway,
			@Nullable SharedSlot parent,
			@Nullable AbstractID groupID) {

		checkArgument(slotNumber >= 0);

		// create a simple slot context
		this.slotContext = new SimpleSlotContext(
			NO_ALLOCATION_ID,
			location,
			slotNumber,
			taskManagerGateway);

		this.owner = checkNotNull(owner);
		this.parent = parent; // may be null
		this.groupID = groupID; // may be null
		this.slotNumber = slotNumber;
	}

	/**
	 * Base constructor for slots.
	 *
	 * @param slotContext The slot context of this slot.
	 * @param owner The component from which this slot is allocated.
	 * @param slotNumber The number of this slot.
	 * @param parent The parent slot that contains this slot. May be null, if this slot is the root.
	 * @param groupID The ID that identifies the task group for which this slot is allocated. May be null
	 *                if the slot does not belong to any task group.   
	 */
	protected Slot(
			SlotContext slotContext,
			SlotOwner owner,
			int slotNumber,
			@Nullable SharedSlot parent,
			@Nullable AbstractID groupID) {

		this.slotContext = checkNotNull(slotContext);
		this.owner = checkNotNull(owner);
		this.parent = parent; // may be null
		this.groupID = groupID; // may be null
		this.slotNumber = slotNumber;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the allocated slot that this slot refers to.
	 * 
	 * @return This slot's allocated slot.
	 */
	public SlotContext getSlotContext() {
		return slotContext;
	}

	/**
	 * Gets the ID of the TaskManager that offers this slot.
	 *
	 * @return The ID of the TaskManager that offers this slot
	 */
	public ResourceID getTaskManagerID() {
		return slotContext.getTaskManagerLocation().getResourceID();
	}

	/**
	 * Gets the location info of the TaskManager that offers this slot.
	 *
	 * @return The location info of the TaskManager that offers this slot
	 */
	public TaskManagerLocation getTaskManagerLocation() {
		return slotContext.getTaskManagerLocation();
	}

	/**
	 * Gets the actor gateway that can be used to send messages to the TaskManager.
	 *
	 * <p>This method should be removed once the new interface-based RPC abstraction is in place
	 *
	 * @return The actor gateway that can be used to send messages to the TaskManager.
	 */
	public TaskManagerGateway getTaskManagerGateway() {
		return slotContext.getTaskManagerGateway();
	}

	/**
	 * Gets the owner of this slot. The owner is the component that the slot was created from
	 * and to which it needs to be returned after the executed tasks are done.
	 * 
	 * @return The owner of this slot.
	 */
	public SlotOwner getOwner() {
		return owner;
	}

	/**
	 * Gets the number of the slot. For a simple slot, that is the number of the slot
	 * on its instance. For a non-root slot, this returns the number of the slot in the
	 * list amongst its siblings in the tree.
	 *
	 * @return The number of the slot on the instance or amongst its siblings that share the same slot.
	 */
	public int getSlotNumber() {
		return slotNumber;
	}

	/**
	 * Gets the number of the root slot. This code behaves equal to {@code getRoot().getSlotNumber()}.
	 * If this slot is the root of the tree of shared slots, then this method returns the same
	 * value as {@link #getSlotNumber()}.
	 *
	 * @return The slot number of the root slot.
	 */
	public int getRootSlotNumber() {
		if (parent == null) {
			return slotNumber;
		} else {
			return parent.getRootSlotNumber();
		}
	}

	/**
	 * Gets the ID that identifies the logical group to which this slot belongs:
	 * <ul>
	 *     <li>If the slot does not belong to any group in particular, this field is null.</li>
	 *     <li>If this slot was allocated as a sub-slot of a
	 *         {@link org.apache.flink.runtime.instance.SlotSharingGroupAssignment}, 
	 *         then this ID will be the JobVertexID of the vertex whose task the slot
	 *         holds in its shared slot.</li>
	 *     <li>In case that the slot represents the shared slot of a co-location constraint, this ID will be the
	 *         ID of the co-location constraint.</li>
	 * </ul>
	 * 
	 * @return The ID identifying the logical group of slots.
	 */
	@Nullable
	public AbstractID getGroupID() {
		return groupID;
	}

	/**
	 * Gets the parent slot of this slot. Returns null, if this slot has no parent.
	 * 
	 * @return The parent slot, or null, if no this slot has no parent.
	 */
	@Nullable
	public SharedSlot getParent() {
		return parent;
	}

	/**
	 * Gets the root slot of the tree containing this slot. If this slot is the root,
	 * the method returns this slot directly, otherwise it recursively goes to the parent until
	 * it reaches the root.
	 * 
	 * @return The root slot of the tree containing this slot
	 */
	public Slot getRoot() {
		if (parent == null) {
			return this;
		} else {
			return parent.getRoot();
		}
	}

	/**
	 * Gets the number of simple slots that are at the leaves of the tree of slots.
	 *
	 * @return The number of simple slots at the leaves.
	 */
	public abstract int getNumberLeaves();

	// --------------------------------------------------------------------------------------------
	//  Status and life cycle
	// --------------------------------------------------------------------------------------------

	/**
	 * Checks of the slot is still alive, i.e. in state {@link #ALLOCATED_AND_ALIVE}.
	 *
	 * @return True if the slot is alive, false otherwise.
	 */
	public boolean isAlive() {
		return status == ALLOCATED_AND_ALIVE;
	}

	/**
	 * Checks of the slot has been cancelled. Note that a released slot is also cancelled.
	 *
	 * @return True if the slot is cancelled or released, false otherwise.
	 */
	public boolean isCanceled() {
		return status != ALLOCATED_AND_ALIVE;
	}

	/**
	 * Checks of the slot has been released.
	 *
	 * @return True if the slot is released, false otherwise.
	 */
	public boolean isReleased() {
		return status == RELEASED;
	}

	/**
	 * Atomically marks the slot as cancelled, if it was alive before.
	 *
	 * @return True, if the state change was successful, false otherwise.
	 */
	final boolean markCancelled() {
		return STATUS_UPDATER.compareAndSet(this, ALLOCATED_AND_ALIVE, CANCELLED);
	}

	/**
	 * Atomically marks the slot as released, if it was cancelled before.
	 *
	 * @return True, if the state change was successful, false otherwise.
	 */
	final boolean markReleased() {
		return STATUS_UPDATER.compareAndSet(this, CANCELLED, RELEASED);
	}

	/**
	 * This method cancels and releases the slot and all its sub-slots.
	 * 
	 * After this method completed successfully, the slot will be in state "released", and the
	 * {@link #isReleased()} method will return {@code true}.
	 * 
	 * If this slot is a simple slot, it will be returned to its instance. If it is a shared slot,
	 * it will release all of its sub-slots and release itself.
	 */
	public abstract CompletableFuture<?> releaseSlot(@Nullable Throwable cause);


	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Slots must always has based on reference identity.
	 */
	@Override
	public final int hashCode() {
		return super.hashCode();
	}

	/**
	 * Slots must always compare on referential equality.
	 */
	@Override
	public final boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public String toString() {
		return hierarchy() + " - " + getTaskManagerLocation() + " - " + getStateName(status);
	}

	protected String hierarchy() {
		return (getParent() != null ? getParent().hierarchy() : "") + '(' + getSlotNumber() + ')';
	}

	private static String getStateName(int state) {
		switch (state) {
			case ALLOCATED_AND_ALIVE:
				return "ALLOCATED/ALIVE";
			case CANCELLED:
				return "CANCELLED";
			case RELEASED:
				return "RELEASED";
			default:
				return "(unknown)";
		}
	}
}
