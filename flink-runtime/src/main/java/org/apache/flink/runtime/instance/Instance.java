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

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import akka.actor.ActorRef;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAvailabilityListener;

/**
 * An taskManager represents a resource a {@link org.apache.flink.runtime.taskmanager.TaskManager} runs on.
 */
public class Instance implements Serializable {

	static final long serialVersionUID = 42L;
	
	/** The lock on which to synchronize allocations and failure state changes */
	private transient final Object instanceLock = new Object();
	
	/** The actor ref to the task manager represented by this taskManager. */
	private transient final ActorRef taskManager;

	/** The instance connection information for the data transfer. */
	private final InstanceConnectionInfo connectionInfo;
	
	/** A description of the resources of the task manager */
	private final HardwareDescription resources;
	
	/** The ID identifying the taskManager. */
	private final InstanceID instanceId;

	/** The number of task slots available on the node */
	private final int numberOfSlots;

	/** A list of available slot positions */
	private transient final Queue<Integer> availableSlots;
	
	/** Allocated slots on this taskManager */
	private final Set<AllocatedSlot> allocatedSlots = new HashSet<AllocatedSlot>();

	
	/** A listener to be notified upon new slot availability */
	private transient SlotAvailabilityListener slotAvailabilityListener;
	
	/**
	 * Time when last heat beat has been received from the task manager running on this taskManager.
	 */
	private volatile long lastReceivedHeartBeat = System.currentTimeMillis();
	
	private volatile boolean isDead;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs an abstract taskManager object.
	 * 
	 * @param taskManager The actor reference of the represented task manager.
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
		if (isDead) {
			return;
		}
		
		isDead = true;
		
		synchronized (instanceLock) {
			
			// no more notifications for the slot releasing
			this.slotAvailabilityListener = null;
			
			for (AllocatedSlot slot : allocatedSlots) {
				slot.releaseSlot();
			}
			allocatedSlots.clear();
			availableSlots.clear();
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
	
	public AllocatedSlot allocateSlot(JobID jobID) throws InstanceDiedException {
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
			} else {
				AllocatedSlot slot = new AllocatedSlot(jobID, this, nextSlot);
				allocatedSlots.add(slot);
				return slot;
			}
		}
	}
	
	public boolean returnAllocatedSlot(AllocatedSlot slot) {
		// the slot needs to be in the returned to taskManager state
		if (slot == null || slot.getInstance() != this) {
			throw new IllegalArgumentException("Slot is null or belongs to the wrong taskManager.");
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
				} else {
					throw new IllegalArgumentException("Slot was not allocated from the taskManager.");
				}
			}
		} else {
			return false;
		}
	}
	
	public void cancelAndReleaseAllSlots() {
		synchronized (instanceLock) {
			// we need to do this copy because of concurrent modification exceptions
			List<AllocatedSlot> copy = new ArrayList<AllocatedSlot>(this.allocatedSlots);
			
			for (AllocatedSlot slot : copy) {
				slot.releaseSlot();
			}
			allocatedSlots.clear();
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
	
	public void setSlotAvailabilityListener(SlotAvailabilityListener slotAvailabilityListener) {
		synchronized (instanceLock) {
			if (this.slotAvailabilityListener != null) {
				throw new IllegalStateException("Instance has already a slot listener.");
			} else {
				this.slotAvailabilityListener = slotAvailabilityListener;
			}
		}
	}
	
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
		return instanceId + " @" + (taskManager != null ? taskManager.path() : "ActorRef.noSender") + " " +
				numberOfSlots + " slots";
	}
}
