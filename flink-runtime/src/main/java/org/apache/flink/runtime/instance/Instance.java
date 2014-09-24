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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAvailablilityListener;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.eclipse.jetty.util.log.Log;

/**
 * An instance represents a resource a {@link org.apache.flink.runtime.taskmanager.TaskManager} runs on.
 */
public class Instance {
	
	/** The lock on which to synchronize allocations and failure state changes */
	private final Object instanceLock = new Object();
	
	/** The connection info to connect to the task manager represented by this instance. */
	private final InstanceConnectionInfo instanceConnectionInfo;
	
	/** A description of the resources of the task manager */
	private final HardwareDescription resources;
	
	/** The ID identifying the instance. */
	private final InstanceID instanceId;

	/** The number of task slots available on the node */
	private final int numberOfSlots;

	/** A list of available slot positons */
	private final Queue<Integer> availableSlots;
	
	/** Allocated slots on this instance */
	private final Set<AllocatedSlot> allocatedSlots = new HashSet<AllocatedSlot>();

	
	/** A listener to be notified upon new slot availability */
	private SlotAvailablilityListener slotListener;
	
	
	/** The RPC proxy to send calls to the task manager represented by this instance */
	private volatile TaskOperationProtocol taskManager;

	/**
	 * Time when last heat beat has been received from the task manager running on this instance.
	 */
	private volatile long lastReceivedHeartBeat = System.currentTimeMillis();
	
	private volatile boolean isDead;

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs an abstract instance object.
	 * 
	 * @param instanceConnectionInfo The connection info under which to reach the TaskManager instance.
	 * @param id The id under which the instance is registered.
	 * @param resources The resources available on the machine.
	 * @param numberOfSlots The number of task slots offered by this instance.
	 */
	public Instance(InstanceConnectionInfo instanceConnectionInfo, InstanceID id, HardwareDescription resources, int numberOfSlots) {
		this.instanceConnectionInfo = instanceConnectionInfo;
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
	
	/**
	 * Returns the instance's connection information object.
	 * 
	 * @return the instance's connection information object
	 */
	public InstanceConnectionInfo getInstanceConnectionInfo() {
		return this.instanceConnectionInfo;
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
			this.slotListener = null;
			
			for (AllocatedSlot slot : allocatedSlots) {
				slot.releaseSlot();
			}
			allocatedSlots.clear();
			availableSlots.clear();
		}
		
		destroyTaskManagerProxy();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Connection to the TaskManager
	// --------------------------------------------------------------------------------------------
	
	public TaskOperationProtocol getTaskManagerProxy() throws IOException {
		TaskOperationProtocol tm = this.taskManager;
		
		if (tm == null) {
			synchronized (this) {
				if (this.taskManager == null) {
					this.taskManager = RPC.getProxy(TaskOperationProtocol.class,
						new InetSocketAddress(getInstanceConnectionInfo().address(),
							getInstanceConnectionInfo().ipcPort()), NetUtils.getSocketFactory());
				}
				tm = this.taskManager;
			}
		}
		
		return tm;
	}

	/**  Destroys and removes the RPC stub object for this instance's task manager. */
	private void destroyTaskManagerProxy() {
		synchronized (this) {
			if (this.taskManager != null) {
				try {
					RPC.stopProxy(this.taskManager);
				} catch (Throwable t) {
					Log.debug("Error shutting down RPC proxy.", t);
				}
			}
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
	 * @return True, if this instance is considered alive, false otherwise.
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
		// the slot needs to be in the returned to instance state
		if (slot == null || slot.getInstance() != this) {
			throw new IllegalArgumentException("Slot is null or belongs to the wrong instance.");
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
					
					if (this.slotListener != null) {
						this.slotListener.newSlotAvailable(this);
					}
					
					return true;
				} else {
					throw new IllegalArgumentException("Slot was not allocated from the instance.");
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
	
	public void setSlotAvailabilityListener(SlotAvailablilityListener slotListener) {
		synchronized (instanceLock) {
			if (this.slotListener != null) {
				throw new IllegalStateException("Instance has already a slot listener.");
			} else {
				this.slotListener = slotListener;
			}
		}
	}
	
	public void removeSlotListener() {
		synchronized (instanceLock) {
			this.slotListener = null;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Standard Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return instanceId + " @" + this.instanceConnectionInfo + ' ' + numberOfSlots + " slots";
	}
}
