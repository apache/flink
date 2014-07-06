/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Collection;
import java.util.TimerTask;
import java.util.Timer;

/**
 * In Nephele an instance manager maintains the set of available compute resources. It is responsible for allocating new
 * compute resources,
 * provisioning available compute resources to the JobManager and keeping track of the availability of the utilized
 * compute resources in order
 * to report unexpected resource outages.
 * 
 */
public class DefaultInstanceManager implements InstanceManager {

	// ------------------------------------------------------------------------
	// Internal Constants
	// ------------------------------------------------------------------------

	/**
	 * The log object used to report debugging and error information.
	 */
	private static final Log LOG = LogFactory.getLog(DefaultInstanceManager.class);

	/**
	 * Default duration after which a host is purged in case it did not send
	 * a heart-beat message.
	 */
	private static final int DEFAULT_CLEANUP_INTERVAL = 2 * 60; // 2 min.

	/**
	 * The key to retrieve the clean up interval from the configuration.
	 */
	private static final String CLEANUP_INTERVAL_KEY = "instancemanager.cluster.cleanupinterval";

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

	private final Object lock = new Object();

	/**
	 * Duration after which a host is purged in case it did not send a
	 * heart-beat message.
	 */
	private final long cleanUpInterval;

	/**
	 * Set of hosts known to run a task manager that are thus able to execute
	 * tasks.
	 */
	private final Map<InstanceConnectionInfo, Instance> registeredHosts;

	/**
	 * The network topology of the cluster.
	 */
	private final NetworkTopology networkTopology;

	/**
	 * Object that is notified if instances become available or vanish.
	 */
	private InstanceListener instanceListener;

	private final SchedulingStrategy schedulingStrategy;


	private boolean shutdown;

	/**
	 * Periodic task that checks whether hosts have not sent their heart-beat
	 * messages and purges the hosts in this case.
	 */
	private final TimerTask cleanupStaleMachines = new TimerTask() {

		@Override
		public void run() {

			synchronized (DefaultInstanceManager.this.lock) {

				final List<Map.Entry<InstanceConnectionInfo, Instance>> hostsToRemove =
						new ArrayList<Map.Entry<InstanceConnectionInfo, Instance>>();

				final Map<JobID, List<AllocatedResource>> staleResources = new HashMap<JobID, List<AllocatedResource>>();

				// check all hosts whether they did not send heart-beat messages.
				for (Map.Entry<InstanceConnectionInfo, Instance> entry : registeredHosts.entrySet()) {

					final Instance host = entry.getValue();
					if (!host.isStillAlive(cleanUpInterval)) {

						// this host has not sent the heart-beat messages
						// -> we terminate all instances running on this host and notify the jobs
						final Collection<AllocatedSlot> slots = host.removeAllocatedSlots();
						for (AllocatedSlot slot : slots) {

							final JobID jobID = slot.getJobID();

							List<AllocatedResource> staleResourcesOfJob = staleResources.get(jobID);
							if (staleResourcesOfJob == null) {
								staleResourcesOfJob = new ArrayList<AllocatedResource>();
								staleResources.put(jobID, staleResourcesOfJob);
							}

							staleResourcesOfJob.add(new AllocatedResource(host,	slot.getAllocationID()));
						}

						hostsToRemove.add(entry);
					}
				}

				registeredHosts.entrySet().removeAll(hostsToRemove);

				final Iterator<Map.Entry<JobID, List<AllocatedResource>>> it = staleResources.entrySet().iterator();
				while (it.hasNext()) {
					final Map.Entry<JobID, List<AllocatedResource>> entry = it.next();
					if (instanceListener != null) {
						instanceListener.allocatedResourcesDied(entry.getKey(), entry.getValue());
					}
				}
			}
		}
	};

	// ------------------------------------------------------------------------
	// Constructor and set-up
	// ------------------------------------------------------------------------

	/**
	 * Constructor.
	 */
	public DefaultInstanceManager() {

		this.registeredHosts = new HashMap<InstanceConnectionInfo, Instance>();

		long tmpCleanUpInterval = (long) GlobalConfiguration.getInteger(CLEANUP_INTERVAL_KEY, DEFAULT_CLEANUP_INTERVAL) * 1000;

		if (tmpCleanUpInterval < 10) { // Clean up interval must be at least ten seconds
			LOG.warn("Invalid clean up interval. Reverting to default cleanup interval of " + DEFAULT_CLEANUP_INTERVAL
					+ " secs.");
			tmpCleanUpInterval = DEFAULT_CLEANUP_INTERVAL;
		}

		this.cleanUpInterval = tmpCleanUpInterval;

		this.networkTopology = NetworkTopology.createEmptyTopology();

		// look every BASEINTERVAL milliseconds for crashed hosts
		final boolean runTimerAsDaemon = true;
		new Timer(runTimerAsDaemon).schedule(cleanupStaleMachines, 1000, 1000);

		int schedulingStrategyOrdinal = GlobalConfiguration.getInteger(ConfigConstants.SCHEDULING_STRATEGY, -1);

		SchedulingStrategy resolvedStrategy = SchedulingStrategy.FILLFIRST;

		if(schedulingStrategyOrdinal != -1){
			for(SchedulingStrategy s : SchedulingStrategy.values()){
				if(s.ordinal() == schedulingStrategyOrdinal){
					resolvedStrategy = s;
				}
			}
		}

		schedulingStrategy = resolvedStrategy;
	}

	@Override
	public void shutdown() {
		synchronized (this.lock) {
			if (this.shutdown) {
				return;
			}

			this.cleanupStaleMachines.cancel();

			Iterator<Instance> it = this.registeredHosts.values().iterator();
			while (it.hasNext()) {
				it.next().destroyProxies();
			}
			this.registeredHosts.clear();

			this.shutdown = true;
		}
	}

	@Override
	public void releaseAllocatedResource(AllocatedResource allocatedResource) throws InstanceException
	{
		synchronized (this.lock) {
			// release the instance from the host
			final Instance clusterInstance = allocatedResource.getInstance();
			clusterInstance.releaseSlot(allocatedResource.getAllocationID());
		}
	}

	/**
	 * Creates a new {@link Instance} object to manage instances that can
	 * be executed on that host.
	 *
	 * @param instanceConnectionInfo
	 *        the connection information for the instance
	 * @param hardwareDescription
	 *        the hardware description provided by the new instance
	 * @param numberOfSlots
	 * 		  number of slots available on the instance
	 * @return a new {@link Instance} object or <code>null</code> if the cluster instance could not be created
	 */
	private Instance createNewHost(final InstanceConnectionInfo instanceConnectionInfo,
							final HardwareDescription hardwareDescription, int numberOfSlots) {

		// Try to match new host with a stub host from the existing topology
		String instanceName = instanceConnectionInfo.hostname();
		NetworkNode parentNode = this.networkTopology.getRootNode();
		NetworkNode currentStubNode = null;

		// Try to match new host using the host name
		while (true) {

			currentStubNode = this.networkTopology.getNodeByName(instanceName);
			if (currentStubNode != null) {
				break;
			}

			final int pos = instanceName.lastIndexOf('.');
			if (pos == -1) {
				break;
			}

			/*
			 * If host name is reported as FQDN, iterative remove parts
			 * of the domain name until a match occurs or no more dots
			 * can be found in the host name.
			 */
			instanceName = instanceName.substring(0, pos);
		}

		// Try to match the new host using the IP address
		if (currentStubNode == null) {
			instanceName = instanceConnectionInfo.address().toString();
			instanceName = instanceName.replaceAll("/", ""); // Remove any / characters
			currentStubNode = this.networkTopology.getNodeByName(instanceName);
		}

		if (currentStubNode != null) {
			/*
			 * The instance name will be the same as the one of the stub node. That way
			 * the stub now will be removed from the network topology and replaced be
			 * the new node.
			 */
			if (currentStubNode.getParentNode() != null) {
				parentNode = currentStubNode.getParentNode();
			}
			// Remove the stub node from the tree
			currentStubNode.remove();
		}

		LOG.info("Creating instance for " + instanceConnectionInfo + ", parent is "
				+ parentNode.getName());
		final Instance host = new Instance(instanceConnectionInfo, parentNode,
				this.networkTopology, hardwareDescription, numberOfSlots);

		return host;
	}

	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo) {

		synchronized (this.lock) {
			Instance host = registeredHosts.get(instanceConnectionInfo);

			if(host == null){
				LOG.error("Task manager with connection info " + instanceConnectionInfo + " has not been registered.");
				return;
			}

			host.reportHeartBeat();
		}
	}

	@Override
	public void registerTaskManager(InstanceConnectionInfo instanceConnectionInfo,
									HardwareDescription hardwareDescription, int numberOfSlots){
		synchronized(this.lock){
			if(registeredHosts.containsKey(instanceConnectionInfo)){
				LOG.error("Task manager with connection info " + instanceConnectionInfo + " has already been " +
						"registered.");
				return;
			}

			Instance host = createNewHost(instanceConnectionInfo, hardwareDescription, numberOfSlots);

			if(host == null){
				LOG.error("Could not create a new host object for register task manager for connection info " +
						instanceConnectionInfo);
				return;
			}

			this.registeredHosts.put(instanceConnectionInfo, host);
			LOG.info("New number of registered hosts is " + this.registeredHosts.size());

			host.reportHeartBeat();
		}
	}

	@Override
	public void requestInstance(JobID jobID, Configuration conf,  int requiredSlots)
			throws InstanceException
	{

		synchronized(this.lock) {
			Iterator<Instance> clusterIterator = this.registeredHosts.values().iterator();
			Instance instance = null;
			List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();
			int allocatedSlots = 0;

			switch(schedulingStrategy){
				case FILLFIRST:
					while(clusterIterator.hasNext()) {
						instance = clusterIterator.next();
						while(instance.getNumberOfAvailableSlots() >0  && allocatedSlots < requiredSlots){
							AllocatedResource resource = instance.allocateSlot(jobID);
							allocatedResources.add(resource);
							allocatedSlots++;
						}
					}
					break;
				case SPREADOUT:
					List<Instance> increasingLoad = new ArrayList<Instance>(this.registeredHosts.values());

					if(increasingLoad.size() == 0){
						throw new InstanceException("There are no instances available for scheduling.");
					}

					Collections.sort(increasingLoad, new Comparator<Instance>() {
						@Override
						public int compare(Instance o1, Instance o2) {
							if(o1.getLoad() < o2.getLoad()){
								return -1;
							}else if(o2.getLoad() < o1.getLoad()){
								return 1;
							}else {
								return 0;
							}
						}
					});

					Instance headInstance = increasingLoad.get(0);
					instance = headInstance;
					int index = 0;
					double nextLoad = 1.0;

					if(increasingLoad.size()>1){
						nextLoad = increasingLoad.get(1).getLoad();
					}

					while(allocatedSlots < requiredSlots && instance.getLoad() < 1.0){
						AllocatedResource resource = instance.allocateSlot(jobID);
						allocatedResources.add(resource);
						allocatedSlots++;

						if(headInstance.getLoad() < instance.getLoad()){
							index = 0;
							instance = headInstance;
						}else if(instance.getLoad() > nextLoad){
							index++;
							instance = increasingLoad.get(index);
							if(increasingLoad.size() > index+1){
								nextLoad = increasingLoad.get(index+1).getLoad();
							}else{
								nextLoad = 1.0;
							}
						}
					}
					break;
				default:
					throw new InstanceException("Scheduling strategy is not supported.");
			}

			if(allocatedSlots < requiredSlots){
				throw new InstanceException("Cannot allocate the required number of slots: " + requiredSlots + ".");
			}

			if (this.instanceListener != null) {
				final InstanceNotifier instanceNotifier = new InstanceNotifier(
						this.instanceListener, jobID, allocatedResources);
				instanceNotifier.start();
			}
		}
	}

	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) {
		return this.networkTopology;
	}

	@Override
	public void setInstanceListener(InstanceListener instanceListener) {
		synchronized (this.lock) {
			this.instanceListener = instanceListener;
		}
	}

	@Override
	public Instance getInstanceByName(String name) {
		if (name == null) {
			throw new IllegalArgumentException("Argument name must not be null");
		}

		synchronized (this.lock) {
			final Iterator<Instance> it = this.registeredHosts.values().iterator();
			while (it.hasNext()) {
				final Instance instance = it.next();
				if (name.equals(instance.getName())) {
					return instance;
				}
			}
		}

		return null;
	}

	@Override
	public int getNumberOfTaskTrackers() {
		return this.registeredHosts.size();
	}

	@Override
	public int getNumberOfSlots() {
		int slots = 0;

		for(Instance instance: registeredHosts.values()){
			slots += instance.getNumberOfSlots();
		}

		return slots;
	}
}
