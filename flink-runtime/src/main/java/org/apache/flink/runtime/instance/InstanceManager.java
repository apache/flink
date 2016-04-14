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
import java.util.UUID;

import akka.actor.ActorRef;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
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

	/** Set of hosts known to run a task manager that are thus able to execute tasks (by connection). */
	private final Map<ActorRef, Instance> registeredHostsByConnection;

	/** Set of hosts known to run a task manager that are thus able to execute tasks (by ResourceID). */
	private final Map<ResourceID, Instance> registeredHostsByResource;

	/** Set of hosts that were present once and have died */
	private final Set<ActorRef> deadHosts;

	/** Listeners that want to be notified about availability and disappearance of instances */
	private final List<InstanceListener> instanceListeners = new ArrayList<InstanceListener>();

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
		this.registeredHostsByConnection = new LinkedHashMap<>();
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
			this.registeredHostsByConnection.clear();
			this.registeredHostsByResource.clear();
			this.deadHosts.clear();
			this.totalNumberOfAliveTaskSlots = 0;
		}
	}

	public boolean reportHeartBeat(InstanceID instanceId, byte[] lastMetricsReport) {
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
			host.setMetricsReport(lastMetricsReport);

			if (LOG.isDebugEnabled()) {
				LOG.debug("Received heartbeat from TaskManager " + host);
			}

			return true;
		}
	}

	/**
	 * Registers a task manager. Registration of a task manager makes it available to be used
	 * for the job execution.
	 *
	 * @param taskManager ActorRef to the TaskManager which wants to be registered
	 * @param resourceID The resource id of the TaskManager
	 * @param connectionInfo ConnectionInfo of the TaskManager
	 * @param resources Hardware description of the TaskManager
	 * @param numberOfSlots Number of available slots on the TaskManager
	 * @param leaderSessionID The current leader session ID of the JobManager
	 * @return The assigned InstanceID of the registered task manager
	 */
	public InstanceID registerTaskManager(
			ActorRef taskManager,
			ResourceID resourceID,
			InstanceConnectionInfo connectionInfo,
			HardwareDescription resources,
			int numberOfSlots,
			UUID leaderSessionID){
		synchronized(this.lock){
			if (this.isShutdown) {
				throw new IllegalStateException("InstanceManager is shut down.");
			}

			Instance prior = registeredHostsByConnection.get(taskManager);
			if (prior != null) {
				throw new IllegalStateException("Registration attempt from TaskManager at "
					+ taskManager.path() +
					". This connection is already registered under ID " + prior.getId());
			}

			boolean wasDead = this.deadHosts.remove(taskManager);
			if (wasDead) {
				LOG.info("Registering TaskManager at " + taskManager.path() +
						" which was marked as dead earlier because of a heart-beat timeout.");
			}

			ActorGateway actorGateway = new AkkaActorGateway(taskManager, leaderSessionID);

			InstanceID instanceID = new InstanceID();

			Instance host = new Instance(actorGateway, connectionInfo, resourceID, instanceID,
				resources, numberOfSlots);

			registeredHostsById.put(instanceID, host);
			registeredHostsByConnection.put(taskManager, host);
			registeredHostsByResource.put(resourceID, host);

			totalNumberOfAliveTaskSlots += numberOfSlots;

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Registered TaskManager at %s (%s) as %s. " +
								"Current number of registered hosts is %d. " +
								"Current number of alive task slots is %d.",
						connectionInfo.getHostname(),
						taskManager.path(),
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
	 * Unregisters the TaskManager with the given {@link ActorRef}. Unregistering means to mark
	 * the given instance as dead and notify {@link InstanceListener} about the dead instance.
	 *
	 * @param instanceID TaskManager which is about to be marked dead.
	 */
	public void unregisterTaskManager(ActorRef instanceID, boolean terminated){
		Instance instance = registeredHostsByConnection.get(instanceID);

		if (instance != null){
			ActorRef host = instance.getActorGateway().actor();

			registeredHostsByConnection.remove(host);
			registeredHostsById.remove(instance.getId());
			registeredHostsByResource.remove(instance.getResourceId());

			if (terminated) {
				deadHosts.add(instance.getActorGateway().actor());
			}

			instance.markDead();

			totalNumberOfAliveTaskSlots -= instance.getTotalNumberOfSlots();

			notifyDeadInstance(instance);

			LOG.info("Unregistered task manager " + host.path() + ". Number of " +
					"registered task managers " + getNumberOfRegisteredTaskManagers() + ". Number" +
					" of available slots " + getTotalNumberOfSlots() + ".");
		} else {
			LOG.warn("Tried to unregister instance {} but it is not registered.", instanceID);
		}
	}

	/**
	 * Unregisters all currently registered TaskManagers from the InstanceManager.
	 */
	public void unregisterAllTaskManagers() {
		for(Instance instance: registeredHostsById.values()) {
			deadHosts.add(instance.getActorGateway().actor());

			instance.markDead();

			totalNumberOfAliveTaskSlots -= instance.getTotalNumberOfSlots();

			notifyDeadInstance(instance);
		}

		registeredHostsById.clear();
		registeredHostsByConnection.clear();
		registeredHostsByResource.clear();
	}

	public boolean isRegistered(ActorRef taskManager) {
		return registeredHostsByConnection.containsKey(taskManager);
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

	public Instance getRegisteredInstance(ActorRef ref) {
		return registeredHostsByConnection.get(ref);
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
