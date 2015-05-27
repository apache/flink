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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import akka.actor.ActorRef;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAvailabilityListener;

/**
 * An instance represents a {@link org.apache.flink.runtime.taskmanager.TaskManager}
 * registered at a JobManager and ready to receive work.
 */
public class Instance {

	/** The lock on which to synchronize allocations and failure state changes */
	private final Object instanceLock = new Object();

	/** The actor ref to the task manager represented by this taskManager. */
	private final ActorRef taskManager;

	/** The instance connection information for the data transfer. */
	private final InstanceConnectionInfo connectionInfo;

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

	private byte[] lastMetricsReport;

	/** Flag marking the instance as alive or as dead. */
	private volatile boolean isDead;


	// --------------------------------------------------------------------------------------------

	/**
	 * Constructs an instance reflecting a registered TaskManager.
	 *
	 * @param taskManager The actor reference of the represented task manager.
	 * @param connectionInfo The remote connection where the task manager receives requests.
	 * @param id The id under which the taskManager is registered.
	 * @param resources The resources available on the machine.
	 * @param numberOfSlots The number of task slots offered by this taskManager.
	 */
	public Instance(ActorRef taskManager, InstanceConnectionInfo connectionInfo, InstanceID id,
					HardwareDescription resources, int numberOfSlots) {
		this.taskManager = taskManager;
		this.connectionInfo = connectionInfo;
		this.instanceId = id;
		this.resources = resources;
		this.numberOfSlots = numberOfSlots;

		this.availableSlots = new ArrayDeque<Integer>(numberOfSlots);
		for (int i = 0; i < numberOfSlots; i++) {
			this.availableSlots.add(i);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Properties
	// --------------------------------------------------------------------------------------------

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
		for (Slot slot : slots) {
			slot.releaseSlot();
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

	public void setMetricsReport(byte[] lastMetricsReport) {
		this.lastMetricsReport = lastMetricsReport;
	}

	public byte[] getLastMetricsReport() {
		return lastMetricsReport;
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
	 * @param jobID The ID of the job that the slot is allocated for.
	 *
	 * @return A simple slot that represents a task slot on this TaskManager instance, or null, if the
	 *         TaskManager instance has no more slots available.
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the
	 *                               slot is allocated. 
	 */
	public SimpleSlot allocateSimpleSlot(JobID jobID) throws InstanceDiedException {
		if (jobID == null) {
			throw new IllegalArgumentException();
		}

		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SimpleSlot slot = new SimpleSlot(jobID, this, nextSlot);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}

	/**
	 * Allocates a shared slot on this TaskManager instance. This method returns {@code null}, if no slot
	 * is available at the moment. The shared slot will be managed by the given  SlotSharingGroupAssignment.
	 *
	 * @param jobID The ID of the job that the slot is allocated for.
	 * @param sharingGroupAssignment The assignment group that manages this shared slot.
	 *
	 * @return A shared slot that represents a task slot on this TaskManager instance and can hold other
	 *         (shared) slots, or null, if the TaskManager instance has no more slots available.
	 *
	 * @throws InstanceDiedException Thrown if the instance is no longer alive by the time the slot is allocated. 
	 */
	public SharedSlot allocateSharedSlot(JobID jobID, SlotSharingGroupAssignment sharingGroupAssignment)
			throws InstanceDiedException
	{
		// the slot needs to be in the returned to taskManager state
		if (jobID == null) {
			throw new IllegalArgumentException();
		}

		synchronized (instanceLock) {
			if (isDead) {
				throw new InstanceDiedException(this);
			}

			Integer nextSlot = availableSlots.poll();
			if (nextSlot == null) {
				return null;
			}
			else {
				SharedSlot slot = new SharedSlot(jobID, this, nextSlot, sharingGroupAssignment);
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
	 * @param slot The slot to return.
	 * @return True, if the slot was returned, false if not.
	 */
	public boolean returnAllocatedSlot(Slot slot) {
		if (slot == null || slot.getInstance() != this) {
			throw new IllegalArgumentException("Slot is null or belongs to the wrong TaskManager.");
		}
		if (slot.isAlive()) {
			throw new IllegalArgumentException("Slot is still alive");
		}

		if (slot.markReleased()) {
			synchronized (instanceLock) {
				if (isDead) {
					return false;
				}

				if (this.allocatedSlots.remove(slot)) {
					this.availableSlots.add(slot.getSlotNumber());

					if (this.slotAvailabilityListener != null) {
						this.slotAvailabilityListener.newSlotAvailable(this);
					}

					return true;
				}
				else {
					throw new IllegalArgumentException("Slot was not allocated from this TaskManager.");
				}
			}
		}
		else {
			return false;
		}
	}

	public void cancelAndReleaseAllSlots() {
		// we need to do this copy because of concurrent modification exceptions
		List<Slot> copy;
		synchronized (instanceLock) {
			copy = new ArrayList<Slot>(this.allocatedSlots);
		}

		for (Slot slot : copy) {
			slot.releaseSlot();
		}
	}

	public ActorRef getTaskManager() {
		return taskManager;
	}

	public String getPath(){
		return taskManager.path().toString();
	}

	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return connectionInfo;
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
		return String.format("%s @ %s - %d slots - URL: %s", instanceId, connectionInfo.getHostname(),
				numberOfSlots, (taskManager != null ? taskManager.path() : "ActorRef.noSender"));
	}
}