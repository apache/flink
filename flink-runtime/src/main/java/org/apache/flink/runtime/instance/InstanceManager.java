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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple manager that keeps track of which TaskManager are available and alive.
 */
public class InstanceManager {

	private static final Logger LOG = LoggerFactory.getLogger(InstanceManager.class);

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	/** Global lock */
	private final Object lock = new Object();

	/** Set of hosts known to run a task manager that are thus able to execute tasks (by ID). */
	private final Map<InstanceID, Instance> registeredHostsById;

	/** Set of hosts known to run a task manager that are thus able to execute tasks (by ResourceID). */
	private final Map<ResourceID, Instance> registeredHostsByResource;

	/** Set of hosts that were present once and have died */
	private final Set<ResourceID> deadHosts;

	/** Listeners that want to be notified about availability and disappearance of instances */
	private final List<InstanceListener> instanceListeners = new ArrayList<>();

	/** The total number of task slots that the system has */
	private int totalNumberOfAliveTaskSlots;

	/** Flag marking the system as shut down */
	private volatile boolean isShutdown;

	// ------------------------------------------------------------------------
	// Constructor and set-up
	// ------------------------------------------------------------------------

	/**
	 * Creates an new instance manager.
	 */
	public InstanceManager() {
		this.registeredHostsById = new LinkedHashMap<>();
		this.registeredHostsByResource = new LinkedHashMap<>();
		this.deadHosts = new HashSet<>();
	}

	public void shutdown() {
		synchronized (this.lock) {
			if (this.isShutdown) {
				return;
			}
			this.isShutdown = true;

			for (Instance i : this.registeredHostsById.values()) {
				i.markDead();
			}

			this.registeredHostsById.clear();
			this.registeredHostsByResource.clear();
			this.deadHosts.clear();
			this.totalNumberOfAliveTaskSlots = 0;
		}
	}

	public boolean reportHeartBeat(InstanceID instanceId) {
		if (instanceId == null) {
			throw new IllegalArgumentException("InstanceID may not be null.");
		}

		synchronized (this.lock) {
			if (this.isShutdown) {
				return false;
			}

			Instance host = registeredHostsById.get(instanceId);

			if (host == null){
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received heartbeat from unknown TaskManager with instance ID " + instanceId.toString() +
							" Possibly TaskManager was marked as dead (timed-out) earlier. " +
							"Reporting back that task manager is no longer known.");
				}
				return false;
			}

			host.reportHeartBeat();

			LOG.trace("Received heartbeat from TaskManager {}", host);

			return true;
		}
	}

	/**
	 * Registers a task manager. Registration of a task manager makes it available to be used
	 * for the job execution.
	 *
	 * @param taskManagerGateway gateway to the task manager
	 * @param taskManagerLocation Location info of the TaskManager
	 * @param resources Hardware description of the TaskManager
	 * @param numberOfSlots Number of available slots on the TaskManager
	 * @return The assigned InstanceID of the registered task manager
	 */
	public InstanceID registerTaskManager(
			TaskManagerGateway taskManagerGateway,
			TaskManagerLocation taskManagerLocation,
			HardwareDescription resources,
			int numberOfSlots) {
		
		synchronized (this.lock) {
			if (this.isShutdown) {
				throw new IllegalStateException("InstanceManager is shut down.");
			}

			Instance prior = registeredHostsByResource.get(taskManagerLocation.getResourceID());
			if (prior != null) {
				throw new IllegalStateException("Registration attempt from TaskManager at "
					+ taskManagerLocation.addressString() +
					". This connection is already registered under ID " + prior.getId());
			}

			boolean wasDead = this.deadHosts.remove(taskManagerLocation.getResourceID());
			if (wasDead) {
				LOG.info("Registering TaskManager at " + taskManagerLocation.addressString() +
						" which was marked as dead earlier because of a heart-beat timeout.");
			}

			InstanceID instanceID = new InstanceID();

			Instance host = new Instance(
				taskManagerGateway,
				taskManagerLocation,
				instanceID,
				resources,
				numberOfSlots);

			registeredHostsById.put(instanceID, host);
			registeredHostsByResource.put(taskManagerLocation.getResourceID(), host);

			totalNumberOfAliveTaskSlots += numberOfSlots;

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Registered TaskManager at %s (%s) as %s. " +
								"Current number of registered hosts is %d. " +
								"Current number of alive task slots is %d.",
						taskManagerLocation.getHostname(),
						taskManagerGateway.getAddress(),
						instanceID,
						registeredHostsById.size(),
						totalNumberOfAliveTaskSlots));
			}

			host.reportHeartBeat();

			// notify all listeners (for example the scheduler)
			notifyNewInstance(host);

			return instanceID;
		}
	}

	/**
	 * Unregisters the TaskManager with the given instance id. Unregistering means to mark
	 * the given instance as dead and notify {@link InstanceListener} about the dead instance.
	 *
	 * @param instanceId TaskManager which is about to be marked dead.
	 */
	public void unregisterTaskManager(InstanceID instanceId, boolean terminated){
		Instance instance = registeredHostsById.get(instanceId);

		if (instance != null){
			registeredHostsById.remove(instance.getId());
			registeredHostsByResource.remove(instance.getTaskManagerID());

			if (terminated) {
				deadHosts.add(instance.getTaskManagerID());
			}

			instance.markDead();

			totalNumberOfAliveTaskSlots -= instance.getTotalNumberOfSlots();

			notifyDeadInstance(instance);

			LOG.info(
				"Unregistered task manager " + instance.getTaskManagerLocation().addressString() +
				". Number of registered task managers " + getNumberOfRegisteredTaskManagers() +
				". Number of available slots " + getTotalNumberOfSlots() + ".");
		} else {
			LOG.warn("Tried to unregister instance {} but it is not registered.", instanceId);
		}
	}

	/**
	 * Unregisters all currently registered TaskManagers from the InstanceManager.
	 */
	public void unregisterAllTaskManagers() {
		for(Instance instance: registeredHostsById.values()) {
			deadHosts.add(instance.getTaskManagerID());

			instance.markDead();

			totalNumberOfAliveTaskSlots -= instance.getTotalNumberOfSlots();

			notifyDeadInstance(instance);
		}

		registeredHostsById.clear();
		registeredHostsByResource.clear();
	}

	public boolean isRegistered(InstanceID instanceId) {
		return registeredHostsById.containsKey(instanceId);
	}

	public boolean isRegistered(ResourceID resourceId) {
		return registeredHostsByResource.containsKey(resourceId);
	}

	public int getNumberOfRegisteredTaskManagers() {
		return this.registeredHostsById.size();
	}

	public int getTotalNumberOfSlots() {
		return this.totalNumberOfAliveTaskSlots;
	}
	
	public int getNumberOfAvailableSlots() {
		synchronized (this.lock) {
			int numSlots = 0;
			
			for (Instance i : this.registeredHostsById.values()) {
				numSlots += i.getNumberOfAvailableSlots();
			}
			
			return numSlots;
		}
	}

	public Collection<Instance> getAllRegisteredInstances() {
		synchronized (this.lock) {
			// return a copy (rather than a Collections.unmodifiable(...) wrapper), such that
			// concurrent modifications do not interfere with the traversals or lookups
			return new HashSet<Instance>(registeredHostsById.values());
		}
	}

	public Instance getRegisteredInstanceById(InstanceID instanceID) {
		return registeredHostsById.get(instanceID);
	}

	public Instance getRegisteredInstance(ResourceID ref) {
		return registeredHostsByResource.get(ref);
	}

	// --------------------------------------------------------------------------------------------

	public void addInstanceListener(InstanceListener listener) {
		synchronized (this.instanceListeners) {
			this.instanceListeners.add(listener);
		}
	}

	public void removeInstanceListener(InstanceListener listener) {
		synchronized (this.instanceListeners) {
			this.instanceListeners.remove(listener);
		}
	}

	private void notifyNewInstance(Instance instance) {
		synchronized (this.instanceListeners) {
			for (InstanceListener listener : this.instanceListeners) {
				try {
					listener.newInstanceAvailable(instance);
				}
				catch (Throwable t) {
					LOG.error("Notification of new instance availability failed.", t);
				}
			}
		}
	}

	private void notifyDeadInstance(Instance instance) {
		synchronized (this.instanceListeners) {
			for (InstanceListener listener : this.instanceListeners) {
				try {
					listener.instanceDied(instance);
				} catch (Throwable t) {
					LOG.error("Notification of dead instance failed.", t);
				}
			}
		}
	}
}
