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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
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
	private final Map<InstanceConnectionInfo, Instance> registeredHostsByConnection;
	
	/** Set of hosts that were present once and have died */
	private final Set<InstanceConnectionInfo> deadHosts;
	
	/** Listeners that want to be notified about availability and disappearance of instances */
	private final List<InstanceListener> instanceListeners = new ArrayList<InstanceListener>();

	/** Duration after which a task manager is considered dead if it did not send a heart-beat message. */
	private final long heartbeatTimeout;
	
	/** The total number of task slots that the system has */
	private int totalNumberOfAliveTaskSlots;

	/** Flag marking the system as shut down */
	private volatile boolean shutdown;

	// ------------------------------------------------------------------------
	// Constructor and set-up
	// ------------------------------------------------------------------------
	
	/**
	 * Creates an instance manager, using the global configuration value for maximum interval between heartbeats
	 * where a task manager is still considered alive.
	 */
	public InstanceManager() {
		this(1000 * GlobalConfiguration.getLong(
				ConfigConstants.JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT_KEY,
				ConfigConstants.DEFAULT_JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT));
	}
	
	public InstanceManager(long heartbeatTimeout) {
		this(heartbeatTimeout, heartbeatTimeout);
	}
	
	public InstanceManager(long heartbeatTimeout, long cleanupInterval) {
		if (heartbeatTimeout <= 0 || cleanupInterval <= 0) {
			throw new IllegalArgumentException("Heartbeat timeout and cleanup interval must be positive.");
		}
		
		this.registeredHostsById = new HashMap<InstanceID, Instance>();
		this.registeredHostsByConnection = new HashMap<InstanceConnectionInfo, Instance>();
		this.deadHosts = new HashSet<InstanceConnectionInfo>();
		this.heartbeatTimeout = heartbeatTimeout;

		new Timer(true).schedule(cleanupStaleMachines, cleanupInterval, cleanupInterval);
	}

	public void shutdown() {
		synchronized (this.lock) {
			if (this.shutdown) {
				return;
			}
			this.shutdown = true;

			this.cleanupStaleMachines.cancel();

			for (Instance i : this.registeredHostsById.values()) {
				i.markDead();
			}
			
			this.registeredHostsById.clear();
			this.registeredHostsByConnection.clear();
			this.deadHosts.clear();
			this.totalNumberOfAliveTaskSlots = 0;
		}
	}

	public boolean reportHeartBeat(InstanceID instanceId) {
		if (instanceId == null) {
			throw new IllegalArgumentException("InstanceID may not be null.");
		}
		
		synchronized (this.lock) {
			if (this.shutdown) {
				throw new IllegalStateException("InstanceManager is shut down.");
			}
			
			Instance host = registeredHostsById.get(instanceId);

			if (host == null){
				if (LOG.isDebugEnabled()) {
					LOG.debug("Received hearbeat from unknown TaskManager with instance ID " + instanceId.toString() + 
							" Possibly TaskManager was maked as dead (timed-out) earlier. " +
							"Reporting back that task manager is no longer known.");
				}
				return false;
			}

			host.reportHeartBeat();
			return true;
		}
	}

	public InstanceID registerTaskManager(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription resources, int numberOfSlots){
		synchronized(this.lock){
			if (this.shutdown) {
				throw new IllegalStateException("InstanceManager is shut down.");
			}
			
			Instance prior = registeredHostsByConnection.get(instanceConnectionInfo);
			if (prior != null) {
				LOG.error("Registration attempt from TaskManager with connection info " + instanceConnectionInfo + 
						". This connection is already registered under ID " + prior.getId());
				return null;
			}
			
			boolean wasDead = this.deadHosts.remove(instanceConnectionInfo);
			if (wasDead) {
				LOG.info("Registering TaskManager with connection info " + instanceConnectionInfo + 
						" which was marked as dead earlier because of a heart-beat timeout.");
			}

			InstanceID id = null;
			do {
				id = new InstanceID();
			} while (registeredHostsById.containsKey(id));
			
			
			Instance host = new Instance(instanceConnectionInfo, id, resources, numberOfSlots);
			
			registeredHostsById.put(id, host);
			registeredHostsByConnection.put(instanceConnectionInfo, host);
			
			totalNumberOfAliveTaskSlots += numberOfSlots;
			
			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Registered TaskManager at %s as %s. Current number of registered hosts is %d.",
						instanceConnectionInfo, id, registeredHostsById.size()));
			}

			host.reportHeartBeat();
			
			// notify all listeners (for example the scheduler)
			notifyNewInstance(host);
			
			return id;
		}
	}

	public int getNumberOfRegisteredTaskManagers() {
		return this.registeredHostsById.size();
	}

	public int getTotalNumberOfSlots() {
		return this.totalNumberOfAliveTaskSlots;
	}
	
	public Map<InstanceID, Instance> getAllRegisteredInstances() {
		synchronized (this.lock) {
			// return a copy (rather than a Collections.unmodifiable(...) wrapper), such that
			// concurrent modifications do not interfere with the traversals or lookups
			return new HashMap<InstanceID, Instance>(this.registeredHostsById);
		}
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
				}
				catch (Throwable t) {
					LOG.error("Notification of dead instance failed.", t);
				}
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private void checkForDeadInstances() {
		final long now = System.currentTimeMillis();
		final long timeout = InstanceManager.this.heartbeatTimeout;
		
		synchronized (InstanceManager.this.lock) {
			if (InstanceManager.this.shutdown) {
				return;
			}

			final Iterator<Map.Entry<InstanceID, Instance>> entries = registeredHostsById.entrySet().iterator();
			
			// check all hosts whether they did not send heart-beat messages.
			while (entries.hasNext()) {
				
				final Map.Entry<InstanceID, Instance> entry = entries.next();
				final Instance host = entry.getValue();
				
				if (!host.isStillAlive(now, timeout)) {
					
					// remove from the living
					entries.remove();
					registeredHostsByConnection.remove(host.getInstanceConnectionInfo());
					
					// add to the dead
					deadHosts.add(host.getInstanceConnectionInfo());
					
					host.markDead();
					
					totalNumberOfAliveTaskSlots -= host.getTotalNumberOfSlots();
					
					LOG.info(String.format("TaskManager %s at %s did not report a heartbeat for %d msecs - marking as dead. Current number of registered hosts is %d.",
							host.getId(), host.getInstanceConnectionInfo(), heartbeatTimeout, registeredHostsById.size()));
					
					// report to all listeners
					notifyDeadInstance(host);
				}
			}
		}
	}
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Periodic task that checks whether hosts have not sent their heart-beat
	 * messages and purges the hosts in this case.
	 */
	private final TimerTask cleanupStaleMachines = new TimerTask() {
		@Override
		public void run() {
			try {
				checkForDeadInstances();
			}
			catch (Throwable t) {
				LOG.error("Checking for dead instances failed.", t);
			}
		}
	};
}
