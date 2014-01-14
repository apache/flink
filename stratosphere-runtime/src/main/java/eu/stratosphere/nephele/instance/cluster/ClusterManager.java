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

package eu.stratosphere.nephele.instance.cluster;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceRequestMap;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.SerializableHashMap;

/**
 * Instance Manager for a static cluster.
 * <p>
 * The cluster manager can handle heterogeneous instances (compute nodes). Each instance type used in the cluster must
 * be described in the configuration.
 * <p>
 * This is a sample configuration: <code>
 * # definition of instances in format
 * # instancename,numComputeUnits,numCores,memorySize,diskCapacity,pricePerHour
 * instancemanager.cluster.type.1 = m1.small,2,1,2048,10,10
 * instancemanager.cluster.type. = c1.medium,2,1,2048,10,10
 * instancemanager.cluster.type. = m1.large,4,2,2048,10,10
 * instancemanager.cluster.type. = m1.xlarge,8,4,8192,20,20
 * instancemanager.cluster.type. = c1.xlarge,8,4,16384,20,40
 * 
 * # default instance type
 * instancemanager.cluster.defaulttype = 1 (pointing to m1.small)
 * </code> Each instance is expected to run exactly one {@link eu.stratosphere.nephele.taskmanager.TaskManager}. When
 * the {@link eu.stratosphere.nephele.taskmanager.TaskManager} registers with the
 * {@link eu.stratosphere.nephele.jobmanager.JobManager} it sends a {@link HardwareDescription} which describes the
 * actual hardware characteristics of the instance (compute node). The cluster manage will attempt to match the report
 * hardware characteristics with one of the configured instance types. Moreover, the cluster manager is capable of
 * partitioning larger instances (compute nodes) into smaller, less powerful instances.
 */
public class ClusterManager implements InstanceManager {

	// ------------------------------------------------------------------------
	// Internal Constants
	// ------------------------------------------------------------------------

	/**
	 * The log object used to report debugging and error information.
	 */
	private static final Log LOG = LogFactory.getLog(ClusterManager.class);

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
	 * The default instance type.
	 */
	private final InstanceType defaultInstanceType;

	/**
	 * Set of hosts known to run a task manager that are thus able to execute
	 * tasks.
	 */
	private final Map<InstanceConnectionInfo, ClusterInstance> registeredHosts;

	/**
	 * Map of a {@link JobID} to all {@link AllocatedSlice}s that belong to this job.
	 */
	private final Map<JobID, List<AllocatedSlice>> slicesOfJobs;

	/**
	 * List of instance types that can be executed on this cluster, sorted by
	 * price (cheapest to most expensive).
	 */
	private final InstanceType[] availableInstanceTypes;

	/**
	 * Map of instance type descriptions which can be queried by the job manager.
	 */
	private final Map<InstanceType, InstanceTypeDescription> instanceTypeDescriptionMap;

	/**
	 * Map of IP addresses to instance types.
	 */
	private final Map<InetAddress, InstanceType> ipToInstanceTypeMapping = new HashMap<InetAddress, InstanceType>();

	/**
	 * Map of pending requests of a job, i.e. the instance requests that could not be fulfilled during the initial
	 * instance request.
	 */
	private final Map<JobID, PendingRequestsMap> pendingRequestsOfJob = new LinkedHashMap<JobID, PendingRequestsMap>();

	/**
	 * The network topology of the cluster.
	 */
	private final NetworkTopology networkTopology;

	/**
	 * Object that is notified if instances become available or vanish.
	 */
	private InstanceListener instanceListener;

	/**
	 * Matrix storing how many instances of a particular type and be accommodated in another instance type.
	 */
	private final int[][] instanceAccommodationMatrix;

	private boolean shutdown;
	
	/**
	 * Periodic task that checks whether hosts have not sent their heart-beat
	 * messages and purges the hosts in this case.
	 */
	private final TimerTask cleanupStaleMachines = new TimerTask() {

		@Override
		public void run() {

			synchronized (ClusterManager.this.lock) {

				final List<Map.Entry<InstanceConnectionInfo, ClusterInstance>> hostsToRemove =
					new ArrayList<Map.Entry<InstanceConnectionInfo, ClusterInstance>>();

				final Map<JobID, List<AllocatedResource>> staleResources = new HashMap<JobID, List<AllocatedResource>>();

				// check all hosts whether they did not send heat-beat messages.
				for (Map.Entry<InstanceConnectionInfo, ClusterInstance> entry : registeredHosts.entrySet()) {

					final ClusterInstance host = entry.getValue();
					if (!host.isStillAlive(cleanUpInterval)) {

						// this host has not sent the heat-beat messages
						// -> we terminate all instances running on this host and notify the jobs
						final List<AllocatedSlice> removedSlices = host.removeAllAllocatedSlices();
						for (AllocatedSlice removedSlice : removedSlices) {

							final JobID jobID = removedSlice.getJobID();
							final List<AllocatedSlice> slicesOfJob = slicesOfJobs.get(jobID);
							if (slicesOfJob == null) {
								LOG.error("Cannot find allocated slices for job with ID + " + jobID);
								continue;
							}

							slicesOfJob.remove(removedSlice);

							// Clean up
							if (slicesOfJob.isEmpty()) {
								slicesOfJobs.remove(jobID);
							}

							List<AllocatedResource> staleResourcesOfJob = staleResources.get(removedSlice.getJobID());
							if (staleResourcesOfJob == null) {
								staleResourcesOfJob = new ArrayList<AllocatedResource>();
								staleResources.put(removedSlice.getJobID(), staleResourcesOfJob);
							}

							staleResourcesOfJob.add(new AllocatedResource(removedSlice.getHostingInstance(),
								removedSlice.getType(),
								removedSlice.getAllocationID()));
						}

						hostsToRemove.add(entry);
					}
				}

				registeredHosts.entrySet().removeAll(hostsToRemove);

				updateInstaceTypeDescriptionMap();

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
	public ClusterManager() {

		this.registeredHosts = new HashMap<InstanceConnectionInfo, ClusterInstance>();

		this.slicesOfJobs = new HashMap<JobID, List<AllocatedSlice>>();

		// Load the instance type this cluster can offer
		this.defaultInstanceType = InstanceTypeFactory.constructFromDescription(ConfigConstants.DEFAULT_INSTANCE_TYPE);
		
		this.availableInstanceTypes = new InstanceType[] { this.defaultInstanceType };
		
		this.instanceAccommodationMatrix = calculateInstanceAccommodationMatrix();

		this.instanceTypeDescriptionMap = new SerializableHashMap<InstanceType, InstanceTypeDescription>();

		long tmpCleanUpInterval = (long) GlobalConfiguration.getInteger(CLEANUP_INTERVAL_KEY, DEFAULT_CLEANUP_INTERVAL) * 1000;

		if (tmpCleanUpInterval < 10) { // Clean up interval must be at least ten seconds
			LOG.warn("Invalid clean up interval. Reverting to default cleanup interval of " + DEFAULT_CLEANUP_INTERVAL
				+ " secs.");
			tmpCleanUpInterval = DEFAULT_CLEANUP_INTERVAL;
		}

		this.cleanUpInterval = tmpCleanUpInterval;

		// sort available instances by CPU core
		sortAvailableInstancesByNumberOfCPUCores();

		this.networkTopology = NetworkTopology.createEmptyTopology();

		// look every BASEINTERVAL milliseconds for crashed hosts
		final boolean runTimerAsDaemon = true;
		new Timer(runTimerAsDaemon).schedule(cleanupStaleMachines, 1000, 1000);

		// Load available instance types into the instance description list
		updateInstaceTypeDescriptionMap();
	}

	/**
	 * Sorts the list of available instance types by the number of CPU cores in a descending order.
	 */
	private void sortAvailableInstancesByNumberOfCPUCores() {

		if (this.availableInstanceTypes.length < 2) {
			return;
		}

		for (int i = 1; i < this.availableInstanceTypes.length; i++) {
			final InstanceType it = this.availableInstanceTypes[i];
			int j = i;
			while (j > 0 && this.availableInstanceTypes[j - 1].getNumberOfCores() < it.getNumberOfCores()) {
				this.availableInstanceTypes[j] = this.availableInstanceTypes[j - 1];
				--j;
			}
			this.availableInstanceTypes[j] = it;
		}
	}

	@Override
	public void shutdown() {
		synchronized (this.lock) {
			if (this.shutdown) {
				return;
			}
			
			this.cleanupStaleMachines.cancel();
			
			Iterator<ClusterInstance> it = this.registeredHosts.values().iterator();
			while (it.hasNext()) {
				it.next().destroyProxies();
			}
			this.registeredHosts.clear();
			
			this.shutdown = true;
		}
	}

	@Override
	public InstanceType getDefaultInstanceType() {
		return this.defaultInstanceType;
	}

	@Override
	public InstanceType getInstanceTypeByName(String instanceTypeName) {
		synchronized (this.lock) {
			for (InstanceType it : availableInstanceTypes) {
				if (it.getIdentifier().equals(instanceTypeName)) {
					return it;
				}
			}
		}

		return null;
	}


	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores,
			int minMemorySize, int minDiskCapacity, int maxPricePerHour)
	{
		// the instances are sorted by price -> the first instance that
		// fulfills/ the requirements is suitable and the cheapest

		synchronized (this.lock) {
			for (InstanceType i : availableInstanceTypes) {
				if (i.getNumberOfComputeUnits() >= minNumComputeUnits && i.getNumberOfCores() >= minNumCPUCores
					&& i.getMemorySize() >= minMemorySize && i.getDiskCapacity() >= minDiskCapacity
					&& i.getPricePerHour() <= maxPricePerHour) {
					return i;
				}
			}
		}
		return null;
	}


	@Override
	public void releaseAllocatedResource(JobID jobID, Configuration conf,
			AllocatedResource allocatedResource) throws InstanceException
	{
		synchronized (this.lock) {
			// release the instance from the host
			final ClusterInstance clusterInstance = (ClusterInstance) allocatedResource.getInstance();
			final AllocatedSlice removedSlice = clusterInstance.removeAllocatedSlice(allocatedResource.getAllocationID());

			// remove the local association between instance and job
			final List<AllocatedSlice> slicesOfJob = this.slicesOfJobs.get(jobID);
			if (slicesOfJob == null) {
				LOG.error("Cannot find allocated slice to release allocated slice for job " + jobID);
				return;
			}

			slicesOfJob.remove(removedSlice);

			// Clean up
			if (slicesOfJob.isEmpty()) {
				this.slicesOfJobs.remove(jobID);
			}

			// Check pending requests
			checkPendingRequests();
		}
	}

	/**
	 * Creates a new {@link ClusterInstance} object to manage instances that can
	 * be executed on that host.
	 * 
	 * @param instanceConnectionInfo
	 *        the connection information for the instance
	 * @param hardwareDescription
	 *        the hardware description provided by the new instance
	 * @return a new {@link ClusterInstance} object or <code>null</code> if the cluster instance could not be created
	 */
	private ClusterInstance createNewHost(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) {

		// Check if there is a user-defined instance type for this IP address
		InstanceType instanceType = this.ipToInstanceTypeMapping.get(instanceConnectionInfo.address());
		if (instanceType != null) {
			LOG.info("Found user-defined instance type for cluster instance with IP "
				+ instanceConnectionInfo.address() + ": " + instanceType);
		} else {
			instanceType = matchHardwareDescriptionWithInstanceType(hardwareDescription);
			if (instanceType != null) {
				LOG.info("Hardware profile of cluster instance with IP " + instanceConnectionInfo.address()
					+ " matches with instance type " + instanceType);
			} else {
				LOG.error("No matching instance type, cannot create cluster instance");
				return null;
			}
		}

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

		LOG.info("Creating instance of type " + instanceType + " for " + instanceConnectionInfo + ", parent is "
			+ parentNode.getName());
		final ClusterInstance host = new ClusterInstance(instanceConnectionInfo, instanceType, parentNode,
			this.networkTopology, hardwareDescription);

		return host;
	}

	/**
	 * Attempts to match the hardware characteristics provided by the {@link HardwareDescription} object with one
	 * of the instance types set in the configuration. The matching is pessimistic, i.e. the hardware characteristics of
	 * the chosen instance type never exceed the actually reported characteristics from the hardware description.
	 * 
	 * @param hardwareDescription
	 *        the hardware description as reported by the instance
	 * @return the best matching instance type or <code>null</code> if no matching instance type can be found
	 */
	private InstanceType matchHardwareDescriptionWithInstanceType(final HardwareDescription hardwareDescription) {

		// Assumes that the available instance types are ordered by number of CPU cores in descending order
		for (int i = 0; i < this.availableInstanceTypes.length; i++) {

			final InstanceType candidateInstanceType = this.availableInstanceTypes[i];
			// Check if number of CPU cores match
			if (candidateInstanceType.getNumberOfCores() > hardwareDescription.getNumberOfCPUCores()) {
				continue;
			}

			// Check if size of physical memory matches
			final int memoryInMB = (int) (hardwareDescription.getSizeOfPhysicalMemory() / (1024L * 1024L));
			if (candidateInstanceType.getMemorySize() > memoryInMB) {
				continue;
			}

			return candidateInstanceType;
		}

		LOG.error("Cannot find matching instance type for hardware description ("
			+ hardwareDescription.getNumberOfCPUCores() + " cores, " + hardwareDescription.getSizeOfPhysicalMemory()
			+ " bytes of memory)");

		return null;
	}


	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription) {

		synchronized (this.lock) {
			ClusterInstance host = registeredHosts.get(instanceConnectionInfo);
	
			// check whether we have discovered a new host
			if (host == null) {
				host = createNewHost(instanceConnectionInfo, hardwareDescription);
	
				if (host == null) {
					LOG.error("Could not create a new host object for incoming heart-beat. "
						+ "Probably the configuration file is lacking some entries.");
					return;
				}
	
				this.registeredHosts.put(instanceConnectionInfo, host);
				LOG.info("New number of registered hosts is " + this.registeredHosts.size());
	
				// Update the list of instance type descriptions
				updateInstaceTypeDescriptionMap();
	
				// Check if a pending request can be fulfilled by the new host
				checkPendingRequests();
			}
			
			host.reportHeartBeat();
		}
	}

	/**
	 * Checks if a pending request can be fulfilled.
	 */
	private void checkPendingRequests() {

		final Iterator<Map.Entry<JobID, PendingRequestsMap>> it = this.pendingRequestsOfJob.entrySet().iterator();
		while (it.hasNext()) {

			final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();
			final Map.Entry<JobID, PendingRequestsMap> entry = it.next();
			final JobID jobID = entry.getKey();
			final PendingRequestsMap pendingRequestsMap = entry.getValue();
			final Iterator<Map.Entry<InstanceType, Integer>> it2 = pendingRequestsMap.iterator();
			while (it2.hasNext()) {

				final Map.Entry<InstanceType, Integer> entry2 = it2.next();
				final InstanceType requestedInstanceType = entry2.getKey();
				int numberOfPendingInstances = entry2.getValue().intValue();

				// Consistency check
				if (numberOfPendingInstances <= 0) {
					LOG.error("Inconsistency: Job " + jobID + " has " + numberOfPendingInstances
						+ " requests for instance type " + requestedInstanceType.getIdentifier());
					continue;
				}

				while (numberOfPendingInstances > 0) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Trying to allocate instance of type " + requestedInstanceType.getIdentifier());
					}

					// TODO: Introduce topology awareness here
					final AllocatedSlice slice = getSliceOfType(jobID, requestedInstanceType);
					if (slice == null) {
						break;
					} else {

						LOG.info("Allocated instance of type " + requestedInstanceType.getIdentifier()
							+ " as a result of pending request for job " + jobID);

						// Decrease number of pending instances
						--numberOfPendingInstances;
						pendingRequestsMap.decreaseNumberOfPendingInstances(requestedInstanceType);

						List<AllocatedSlice> allocatedSlices = this.slicesOfJobs.get(jobID);
						if (allocatedSlices == null) {
							allocatedSlices = new ArrayList<AllocatedSlice>();
							this.slicesOfJobs.put(jobID, allocatedSlices);
						}
						allocatedSlices.add(slice);

						allocatedResources.add(new AllocatedResource(slice.getHostingInstance(), slice.getType(), slice
							.getAllocationID()));
					}
				}
			}

			if (!allocatedResources.isEmpty() && this.instanceListener != null) {

				final ClusterInstanceNotifier clusterInstanceNotifier = new ClusterInstanceNotifier(
					this.instanceListener, jobID, allocatedResources);

				clusterInstanceNotifier.start();
			}
		}
	}

	/**
	 * Attempts to allocate a slice of the given type for the given job. The method first attempts to allocate this
	 * slice by finding a physical host which exactly matches the given instance type. If this attempt failed, it tries
	 * to allocate the slice by partitioning the resources of a more powerful host.
	 * 
	 * @param jobID
	 *        the ID of the job the slice shall be allocated for
	 * @param instanceType
	 *        the instance type of the requested slice
	 * @return the allocated slice or <code>null</code> if no such slice could be allocated
	 */
	private AllocatedSlice getSliceOfType(final JobID jobID, final InstanceType instanceType) {

		AllocatedSlice slice = null;

		// Try to match the instance type without slicing first
		for (final ClusterInstance host : this.registeredHosts.values()) {
			if (host.getType().equals(instanceType)) {
				slice = host.createSlice(instanceType, jobID);
				if (slice != null) {
					break;
				}
			}
		}

		// Use slicing now if necessary
		if (slice == null) {

			for (final ClusterInstance host : this.registeredHosts.values()) {
				slice = host.createSlice(instanceType, jobID);
				if (slice != null) {
					break;
				}
			}
		}

		return slice;
	}


	@Override
	public void requestInstance(JobID jobID, Configuration conf,  InstanceRequestMap instanceRequestMap, List<String> splitAffinityList)
		throws InstanceException
	{
		final List<AllocatedSlice> newlyAllocatedSlicesOfJob = new ArrayList<AllocatedSlice>();
		final Map<InstanceType, Integer> pendingRequests = new HashMap<InstanceType, Integer>();

		synchronized(this.lock) {
			// Iterate over all instance types
			for (Iterator<Map.Entry<InstanceType, Integer>> it = instanceRequestMap.getMaximumIterator(); it.hasNext();) {
	
				// Iterate over all requested instances of a specific type
				final Map.Entry<InstanceType, Integer> entry = it.next();
				final int maximumNumberOfInstances = entry.getValue().intValue();
	
				for (int i = 0; i < maximumNumberOfInstances; i++) {
	
					LOG.info("Trying to allocate instance of type " + entry.getKey().getIdentifier());
					
					final AllocatedSlice slice = getSliceOfType(jobID, entry.getKey());
	
					if (slice == null) {
						if (i < instanceRequestMap.getMinimumNumberOfInstances(entry.getKey())) {
							// The request cannot be fulfilled, release the slices again and throw an exception
							for (final AllocatedSlice sliceToRelease : newlyAllocatedSlicesOfJob) {
								sliceToRelease.getHostingInstance().removeAllocatedSlice(sliceToRelease.getAllocationID());
							}
	
							// TODO: Remove previously allocated slices again
							throw new InstanceException("Could not find a suitable instance");
						} else {
	
							// Remaining instances are pending
							final int numberOfRemainingInstances = maximumNumberOfInstances - i;
							if (numberOfRemainingInstances > 0) {
	
								// Store the request for the missing instances
								Integer val = pendingRequests.get(entry.getKey());
								if (val == null) {
									val = Integer.valueOf(0);
								}
								val = Integer.valueOf(val.intValue() + numberOfRemainingInstances);
								pendingRequests.put(entry.getKey(), val);
							}
	
							break;
						}
					}
	
					newlyAllocatedSlicesOfJob.add(slice);
				}
			}
	
			// The request could be processed successfully, so update internal bookkeeping.
			List<AllocatedSlice> allAllocatedSlicesOfJob = this.slicesOfJobs.get(jobID);
			if (allAllocatedSlicesOfJob == null) {
				allAllocatedSlicesOfJob = new ArrayList<AllocatedSlice>();
				this.slicesOfJobs.put(jobID, allAllocatedSlicesOfJob);
			}
			allAllocatedSlicesOfJob.addAll(newlyAllocatedSlicesOfJob);
	
			PendingRequestsMap allPendingRequestsOfJob = this.pendingRequestsOfJob.get(jobID);
			if (allPendingRequestsOfJob == null) {
				allPendingRequestsOfJob = new PendingRequestsMap();
				this.pendingRequestsOfJob.put(jobID, allPendingRequestsOfJob);
			}
			for (final Iterator<Map.Entry<InstanceType, Integer>> it = pendingRequests.entrySet().iterator(); it.hasNext();) {
				final Map.Entry<InstanceType, Integer> entry = it.next();
	
				allPendingRequestsOfJob.addRequest(entry.getKey(), entry.getValue().intValue());
			}
	
			// Finally, create the list of allocated resources for the scheduler
			final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();
			for (final AllocatedSlice slice : newlyAllocatedSlicesOfJob) {
				allocatedResources.add(new AllocatedResource(slice.getHostingInstance(), slice.getType(), slice
					.getAllocationID()));
			}
	
			if (this.instanceListener != null) {
				final ClusterInstanceNotifier clusterInstanceNotifier = new ClusterInstanceNotifier(
					this.instanceListener, jobID, allocatedResources);
				clusterInstanceNotifier.start();
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
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {
		Map<InstanceType, InstanceTypeDescription> copyToReturn = new SerializableHashMap<InstanceType, InstanceTypeDescription>();
		synchronized (this.lock) {
			copyToReturn.putAll(this.instanceTypeDescriptionMap);
		}
		return copyToReturn;
	}

	/**
	 * Updates the list of instance type descriptions based on the currently registered hosts.
	 */
	private void updateInstaceTypeDescriptionMap() {

		// this.registeredHosts.values().iterator()
		this.instanceTypeDescriptionMap.clear();

		final List<InstanceTypeDescription> instanceTypeDescriptionList = new ArrayList<InstanceTypeDescription>();

		// initialize array which stores the availability counter for each instance type
		final int[] numberOfInstances = new int[this.availableInstanceTypes.length];
		for (int i = 0; i < numberOfInstances.length; i++) {
			numberOfInstances[i] = 0;
		}

		// Shuffle through instance types
		for (int i = 0; i < this.availableInstanceTypes.length; i++) {

			final InstanceType currentInstanceType = this.availableInstanceTypes[i];
			int numberOfMatchingInstances = 0;
			int minNumberOfCPUCores = Integer.MAX_VALUE;
			long minSizeOfPhysicalMemory = Long.MAX_VALUE;
			long minSizeOfFreeMemory = Long.MAX_VALUE;
			final Iterator<ClusterInstance> it = this.registeredHosts.values().iterator();
			while (it.hasNext()) {
				final ClusterInstance clusterInstance = it.next();
				if (clusterInstance.getType().equals(currentInstanceType)) {
					++numberOfMatchingInstances;
					final HardwareDescription hardwareDescription = clusterInstance.getHardwareDescription();
					minNumberOfCPUCores = Math.min(minNumberOfCPUCores, hardwareDescription.getNumberOfCPUCores());
					minSizeOfPhysicalMemory = Math.min(minSizeOfPhysicalMemory,
						hardwareDescription.getSizeOfPhysicalMemory());
					minSizeOfFreeMemory = Math.min(minSizeOfFreeMemory, hardwareDescription.getSizeOfFreeMemory());
				}
			}

			// Update number of instances
			int highestAccommodationNumber = -1;
			int highestAccommodationIndex = -1;
			for (int j = 0; j < this.availableInstanceTypes.length; j++) {
				final int accommodationNumber = canBeAccommodated(j, i);
				// LOG.debug(this.availableInstanceTypes[j].getIdentifier() + " fits into "
				// + this.availableInstanceTypes[i].getIdentifier() + " " + accommodationNumber + " times");
				if (accommodationNumber > 0) {
					numberOfInstances[j] += numberOfMatchingInstances * accommodationNumber;
					if (accommodationNumber > highestAccommodationNumber) {
						highestAccommodationNumber = accommodationNumber;
						highestAccommodationIndex = j;
					}
				}
			}

			// Calculate hardware description
			HardwareDescription pessimisticHardwareDescription = null;
			if (minNumberOfCPUCores < Integer.MAX_VALUE && minSizeOfPhysicalMemory < Long.MAX_VALUE
				&& minSizeOfFreeMemory < Long.MAX_VALUE) {

				pessimisticHardwareDescription = HardwareDescriptionFactory.construct(minNumberOfCPUCores,
					minSizeOfPhysicalMemory, minSizeOfFreeMemory);

			} else {

				if (highestAccommodationIndex < i) { // Since highestAccommodationIndex smaller than my index, the
														// target instance must be more powerful

					final InstanceTypeDescription descriptionOfLargerInstanceType = instanceTypeDescriptionList
						.get(highestAccommodationIndex);
					if (descriptionOfLargerInstanceType.getHardwareDescription() != null) {
						final HardwareDescription hardwareDescriptionOfLargerInstanceType = descriptionOfLargerInstanceType
							.getHardwareDescription();

						final int numCores = hardwareDescriptionOfLargerInstanceType.getNumberOfCPUCores()
							/ highestAccommodationNumber;
						final long physMem = hardwareDescriptionOfLargerInstanceType.getSizeOfPhysicalMemory()
							/ highestAccommodationNumber;
						final long freeMem = hardwareDescriptionOfLargerInstanceType.getSizeOfFreeMemory()
							/ highestAccommodationNumber;

						pessimisticHardwareDescription = HardwareDescriptionFactory.construct(numCores, physMem,
							freeMem);
					}
				}
			}

			instanceTypeDescriptionList.add(InstanceTypeDescriptionFactory.construct(currentInstanceType,
				pessimisticHardwareDescription, numberOfInstances[i]));
		}

		final Iterator<InstanceTypeDescription> it = instanceTypeDescriptionList.iterator();
		while (it.hasNext()) {

			final InstanceTypeDescription itd = it.next();
			this.instanceTypeDescriptionMap.put(itd.getInstanceType(), itd);
		}
	}

	/**
	 * Calculates the instance accommodation matrix which stores how many times a particular instance type can be
	 * accommodated inside another instance type based on the list of available instance types.
	 * 
	 * @return the instance accommodation matrix
	 */
	private int[][] calculateInstanceAccommodationMatrix() {

		if (this.availableInstanceTypes == null) {
			LOG.error("Cannot compute instance accommodation matrix: availableInstanceTypes is null");
			return null;
		}

		final int matrixSize = this.availableInstanceTypes.length;
		final int[][] am = new int[matrixSize][matrixSize];

		// Populate matrix
		for (int i = 0; i < matrixSize; i++) {
			for (int j = 0; j < matrixSize; j++) {

				if (i == j) {
					am[i][j] = 1;
				} else {

					final InstanceType sourceType = this.availableInstanceTypes[i];
					InstanceType targetType = this.availableInstanceTypes[j];

					// How many times can we accommodate source type into target type?
					final int cores = targetType.getNumberOfCores() / sourceType.getNumberOfCores();
					final int cu = targetType.getNumberOfComputeUnits() / sourceType.getNumberOfComputeUnits();
					final int mem = targetType.getMemorySize() / sourceType.getMemorySize();
					final int disk = targetType.getDiskCapacity() / sourceType.getDiskCapacity();

					am[i][j] = Math.min(cores, Math.min(cu, Math.min(mem, disk)));
				}
			}
		}

		return am;
	}

	/**
	 * Returns how many times the instance type stored at index <code>sourceTypeIndex</code> can be accommodated inside
	 * the instance type stored at index <code>targetTypeIndex</code> in the list of available instance types.
	 * 
	 * @param sourceTypeIndex
	 *        the index of the source instance type in the list of available instance types
	 * @param targetTypeIndex
	 *        the index of the target instance type in the list of available instance types
	 * @return the number of times the source type instance can be accommodated inside the target instance
	 */
	private int canBeAccommodated(int sourceTypeIndex, int targetTypeIndex) {

		if (sourceTypeIndex >= this.availableInstanceTypes.length
			|| targetTypeIndex >= this.availableInstanceTypes.length) {
			LOG.error("Cannot determine number of instance accomodations: invalid index");
			return 0;
		}

		return this.instanceAccommodationMatrix[targetTypeIndex][sourceTypeIndex];
	}


	@Override
	public AbstractInstance getInstanceByName(String name) {
		if (name == null) {
			throw new IllegalArgumentException("Argument name must not be null");
		}

		synchronized (this.lock) {
			final Iterator<ClusterInstance> it = this.registeredHosts.values().iterator();
			while (it.hasNext()) {
				final AbstractInstance instance = it.next();
				if (name.equals(instance.getName())) {
					return instance;
				}
			}
		}

		return null;
	}


	@Override
	public void cancelPendingRequests(JobID jobID) {
		synchronized (this.lock) {
			this.pendingRequestsOfJob.remove(jobID);
		}
	}

	@Override
	public int getNumberOfTaskTrackers() {
		return this.registeredHosts.size();
	}
}
