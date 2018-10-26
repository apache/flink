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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAvailabilityListener;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An instance represents a {@link org.apache.flink.runtime.taskmanager.TaskManager}
 * registered at a JobManager and ready to receive work.
 */
public class Instance implements SlotOwner {

	private final static Logger LOG = LoggerFactory.getLogger(Instance.class);

	/** The lock on which to synchronize allocations and failure state changes */
	private final Object instanceLock = new Object();

	/** The instance gateway to communicate with the instance */
	private final TaskManagerGateway taskManagerGateway;

	/** The instance connection information for the data transfer. */
	private final TaskManagerLocation location;

	/** A description of the resources of the task manager */
	private final HardwareDescription resources;

	/** The ID identifying the taskManager. */
	private final InstanceID instanceId;

	/** The number of task slots available on the node */
	private final int numberOfSlots;

	/** A list of available slot positions */
	private final Queue<Integer> availableSlots;

	/** Allocated slots on this taskManager */
	private final Set<Slot> allocatedSlots = new HashSet<Slot>();

	/** A listener to be notified upon new slot availability */
	private SlotAvailabilityListener slotAvailabilityListener;

	/** Time when last heat beat has been received from the task manager running on this taskManager. */
	private volatile long lastReceivedHeartBeat = System.currentTimeMillis();

	/** Flag marking the instance as alive or as dead. */
	private volatile boolean isDead;


	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs an instance reflecting a registered TaskManager.
	 *
	 * @param taskManagerGateway The actor gateway to communicate with the remote instance
	 * @param location The remote connection where the task manager receives requests.
	 * @param id The id under which the taskManager is registered.
	 * @param resources The resources available on the machine.
	 * @param numberOfSlots The number of task slots offered by this taskManager.
	 */
	public Instance(
			TaskManagerGateway taskManagerGateway,
			TaskManagerLocation location,
			InstanceID id,
			HardwareDescription resources,
			int numberOfSlots) {
		this.taskManagerGateway = Preconditions.checkNotNull(taskManagerGateway);
		this.location = Preconditions.checkNotNull(location);
		this.instanceId = Preconditions.checkNotNull(id);
		this.resources = Preconditions.checkNotNull(resources);
		this.numberOfSlots = numberOfSlots;

		this.availableSlots = new ArrayDeque<>(numberOfSlots);
		for (int i = 0; i < numberOfSlots; i++) {
			this.availableSlots.add(i);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Properties
	// --------------------------------------------------------------------------------------------

	public ResourceID getTaskManagerID() {
		return location.getResourceID();
	}

	public InstanceID getId() {
		return instanceId;
	}

	public HardwareDescription getResources() {
		return this.resources;
	}

	public int getTotalNumberOfSlots() {
		return numberOfSlots;
	}

	// --------------------------------------------------------------------------------------------
	// Life and Death
	// --------------------------------------------------------------------------------------------

	public boolean isAlive() {
		return !isDead;
	}

	public void markDead() {

		// create a copy of the slots to avoid concurrent modification exceptions
		List<Slot> slots;

		synchronized (instanceLock) {
			if (isDead) {
				return;
			}
			isDead = true;

			// no more notifications for the slot releasing
			this.slotAvailabilityListener = null;

			slots = new ArrayList<Slot>(allocatedSlots);

			allocatedSlots.clear();
			availableSlots.clear();
		}

		/*
		 * releaseSlot must not own the instanceLock in order to avoid dead locks where a slot
		 * owning the assignment group lock wants to give itself back to the instance which requires
		 * the instance lock
		 */
		final FlinkException cause = new FlinkException("Instance " + this + " has been marked as dead.");
		for (Slot slot : slots) {
			slot.releaseSlot(cause);
		}
	}


	// --------------------------------------------------------------------------------------------
	// Heartbeats
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the timestamp of the last heartbeat.
	 *
	 * @return The timestamp of the last heartbeat.
	 */
	public long getLastHeartBeat() {
		return this.lastReceivedHeartBeat;
	}

	/**
	 * Updates the time of last received heart beat to the current system time.
	 */
	public void reportHeartBeat() {
		this.lastReceivedHeartBeat = System.currentTimeMillis();
	}

	/**
	 * Checks whether the last heartbeat occurred within the last {@code n} milliseconds
	 * before the given timestamp {@code now}.
	 *
	 * @param now The timestamp representing the current time.
	 * @param cleanUpInterval The maximum time (in msecs) that the last heartbeat may lie in the past.
	 * @return True, if this taskManager is considered alive, false otherwise.
	 */
	public boolean isStillAlive(long now, long cleanUpInterval) {
		return this.lastReceivedHeartBeat + cleanUpInterval > now;
	}

	// --------------------------------------------------------------------------------------------
	// Resource allocation
	// --------------------------------------------------------------------------------------------

	/**
	 * Allocates a simple slot on this TaskManager instance. This method returns {@code null}, if no slot
	 * is available at the moment.
	 *
	 * @return A simple slot that represents a task slot on this TaskManager instance, or null, if the
	 *         TaskManager instance has no more slots available.
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the
	 *                               slot is allocated. 
	 */
	public SimpleSlot allocateSimpleSlot() throws InstanceDiedException {
		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SimpleSlot slot = new SimpleSlot(this, location, nextSlot, taskManagerGateway);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}

	/**
	 * Allocates a shared slot on this TaskManager instance. This method returns {@code null}, if no slot
	 * is available at the moment. The shared slot will be managed by the given  SlotSharingGroupAssignment.
	 *
	 * @param sharingGroupAssignment The assignment group that manages this shared slot.
	 *
	 * @return A shared slot that represents a task slot on this TaskManager instance and can hold other
	 *         (shared) slots, or null, if the TaskManager instance has no more slots available.
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the slot is allocated. 
	 */
	public SharedSlot allocateSharedSlot(SlotSharingGroupAssignment sharingGroupAssignment)
			throws InstanceDiedException {

		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SharedSlot slot = new SharedSlot(
					this,
					location,
					nextSlot,
					taskManagerGateway,
					sharingGroupAssignment);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}

	/**
	 * Returns a slot that has been allocated from this instance. The slot needs have been canceled
	 * prior to calling this method.
	 * 
	 * <p>The method will transition the slot to the "released" state. If the slot is already in state
	 * "released", this method will do nothing.</p>
	 * 
	 * @param logicalSlot The slot to return.
	 * @return Future which is completed with true, if the slot was returned, false if not.
	 */
	@Override
	public CompletableFuture<Boolean> returnAllocatedSlot(LogicalSlot logicalSlot) {
		checkNotNull(logicalSlot);
		checkArgument(logicalSlot instanceof Slot);

		final Slot slot = ((Slot) logicalSlot);
		checkArgument(!slot.isAlive(), "slot is still alive");
		checkArgument(slot.getOwner() == this, "slot belongs to the wrong TaskManager.");

		if (slot.markReleased()) {
			LOG.debug("Return allocated slot {}.", slot);
			synchronized (instanceLock) {
				if (isDead) {
					return CompletableFuture.completedFuture(false);
				}

				if (this.allocatedSlots.remove(slot)) {
					this.availableSlots.add(slot.getSlotNumber());

					if (this.slotAvailabilityListener != null) {
						this.slotAvailabilityListener.newSlotAvailable(this);
					}

					return CompletableFuture.completedFuture(true);
				}
				else {
					throw new IllegalArgumentException("Slot was not allocated from this TaskManager.");
				}
			}
		}
		else {
			return CompletableFuture.completedFuture(false);
		}
	}

	public void cancelAndReleaseAllSlots() {
		// we need to do this copy because of concurrent modification exceptions
		List<Slot> copy;
		synchronized (instanceLock) {
			copy = new ArrayList<Slot>(this.allocatedSlots);
		}

		final FlinkException cause = new FlinkException("Cancel and release all slots of instance " + this + '.');
		for (Slot slot : copy) {
			slot.releaseSlot(cause);
		}
	}

	/**
	 * Returns the InstanceGateway of this Instance. This gateway can be used to communicate with
	 * it.
	 *
	 * @return InstanceGateway associated with this instance
	 */
	public TaskManagerGateway getTaskManagerGateway() {
		return taskManagerGateway;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return location;
	}

	public int getNumberOfAvailableSlots() {
		return this.availableSlots.size();
	}

	public int getNumberOfAllocatedSlots() {
		return this.allocatedSlots.size();
	}

	public boolean hasResourcesAvailable() {
		return !isDead && getNumberOfAvailableSlots() > 0;
	}

	// --------------------------------------------------------------------------------------------
	// Listeners
	// --------------------------------------------------------------------------------------------

	/**
	 * Sets the listener that receives notifications for slot availability.
	 * 
	 * @param slotAvailabilityListener The listener.
	 */
	public void setSlotAvailabilityListener(SlotAvailabilityListener slotAvailabilityListener) {
		synchronized (instanceLock) {
			if (this.slotAvailabilityListener != null) {
				throw new IllegalStateException("Instance has already a slot listener.");
			} else {
				this.slotAvailabilityListener = slotAvailabilityListener;
			}
		}
	}

	/**
	 * Removes the listener that receives notifications for slot availability.
	 */
	public void removeSlotListener() {
		synchronized (instanceLock) {
			this.slotAvailabilityListener = null;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Standard Utilities
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s @ %s - %d slots - URL: %s", instanceId, location.getHostname(),
				numberOfSlots, (taskManagerGateway != null ? taskManagerGateway.getAddress() : "No instance gateway"));
	}
}
