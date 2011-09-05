/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance.cluster;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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
 * <p>
 * This class is thread-safe.
 * 
 * @author battre
 * @author warneke
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
	 * The name of the file which contains the IP to instance type mapping.
	 */
	private static final String SLAVE_FILE_NAME = "slaves";

	/**
	 * The key to extract the configuration directory from the global configuration.
	 */
	private final static String CONFIG_DIR_KEY = "config.dir";

	/**
	 * Default duration after which a host is purged in case it did not send
	 * a heart-beat message.
	 */
	private static final int DEFAULT_CLEANUP_INTERVAL = 2 * 60; // 2 min.

	/**
	 * The key prefix for the configuration parameters that define the different available instance types.
	 */
	private static final String INSTANCE_TYPE_PREFIX_KEY = "instancemanager.cluster.type.";

	/**
	 * The key to retrieve the index of the default instance type from the configuration.
	 */
	private static final String DEFAULT_INSTANCE_TYPE_INDEX_KEY = "instancemanager.cluster.defaulttype";

	/**
	 * The key to retrieve the clean up interval from the configuration.
	 */
	private static final String CLEANUP_INTERVAL_KEY = "instancemanager.cluster.cleanupinterval";

	/**
	 * Regular expression to extract the IP and the instance type of a cluster instance from the slave file.
	 */
	private static final Pattern IP_TO_INSTANCE_TYPE_PATTERN = Pattern.compile("^(\\S+)\\s*(\\S*)\\s*$");

	// ------------------------------------------------------------------------
	// Fields
	// ------------------------------------------------------------------------

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
	 * The network topology of the cluster.
	 */
	private final NetworkTopology networkTopology;

	/**
	 * Object that is notified if instances become available or vanish
	 */
	private InstanceListener instanceListener;

	/**
	 * Matrix storing how many instances of a particular type and be accommodated in another instance type.
	 */
	private final int[][] instanceAccommodationMatrix;

	/**
	 * Periodic task that checks whether hosts have not sent their heart-beat
	 * messages and purges the hosts in this case.
	 */
	private final TimerTask cleanupStaleMachines = new TimerTask() {

		@Override
		public void run() {

			synchronized (ClusterManager.this) {

				final List<Map.Entry<InstanceConnectionInfo, ClusterInstance>> hostsToRemove = new ArrayList<Map.Entry<InstanceConnectionInfo, ClusterInstance>>();
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

				final Iterator<Map.Entry<JobID, List<AllocatedResource>>> it = staleResources.entrySet().iterator();
				while (it.hasNext()) {
					final Map.Entry<JobID, List<AllocatedResource>> entry = it.next();
					if (instanceListener != null) {
						instanceListener.allocatedResourcesDied(entry.getKey(), entry.getValue());
					}
				}

				registeredHosts.entrySet().removeAll(hostsToRemove);

				updateInstaceTypeDescriptionMap();
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
		this.availableInstanceTypes = populateInstanceTypeArray();

		this.instanceAccommodationMatrix = calculateInstanceAccommodationMatrix();

		this.instanceTypeDescriptionMap = new SerializableHashMap<InstanceType, InstanceTypeDescription>();

		long tmpCleanUpInterval = (long) GlobalConfiguration.getInteger(CLEANUP_INTERVAL_KEY, DEFAULT_CLEANUP_INTERVAL) * 1000;

		if (tmpCleanUpInterval < 10) { // Clean up interval must be at least ten seconds
			LOG.warn("Invalid clean up interval. Reverting to default cleanup interval of " + DEFAULT_CLEANUP_INTERVAL
				+ " secs.");
			tmpCleanUpInterval = DEFAULT_CLEANUP_INTERVAL;
		}

		this.cleanUpInterval = tmpCleanUpInterval;

		int tmpDefaultInstanceTypeIndex = GlobalConfiguration.getInteger(DEFAULT_INSTANCE_TYPE_INDEX_KEY,
			ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);

		if (tmpDefaultInstanceTypeIndex > this.availableInstanceTypes.length) {
			LOG.warn("Incorrect index to for default instance type (" + tmpDefaultInstanceTypeIndex
				+ "), switching to default index " + ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);

			tmpDefaultInstanceTypeIndex = ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX;
		}

		this.defaultInstanceType = this.availableInstanceTypes[tmpDefaultInstanceTypeIndex - 1];

		// sort available instances by CPU core
		sortAvailableInstancesByNumberOfCPUCores();

		// load the network topology from the slave file
		this.networkTopology = loadNetworkTopology();

		// load IP to instance type mapping from slave file
		loadIPToInstanceTypeMapping();

		// look every BASEINTERVAL milliseconds for crashed hosts
		final boolean runTimerAsDaemon = true;
		new Timer(runTimerAsDaemon).schedule(cleanupStaleMachines, 1000, 1000);

		// Load available instance types into the instance description list
		updateInstaceTypeDescriptionMap();
	}

	/**
	 * Reads the IP to instance type mapping from the slave file.
	 */
	private void loadIPToInstanceTypeMapping() {

		final String configDir = GlobalConfiguration.getString(CONFIG_DIR_KEY, null);
		if (configDir == null) {
			LOG.error("Cannot find configuration directory to read IP to instance type mapping");
			return;
		}

		final File slaveFile = new File(configDir + File.separator + SLAVE_FILE_NAME);
		if (!slaveFile.exists()) {
			LOG.error("Cannot access slave file to read IP to instance type mapping");
			return;
		}

		try {

			final BufferedReader input = new BufferedReader(new FileReader(slaveFile));

			String line = null;

			while ((line = input.readLine()) != null) {

				final Matcher m = IP_TO_INSTANCE_TYPE_PATTERN.matcher(line);
				if (!m.matches()) {
					LOG.error("Entry does not match format: " + line);
					continue;
				}
				InetAddress address = null;
				String host = m.group(1);
				try {
					final int pos = host.lastIndexOf('/');
					if (pos != -1) {
						host = host.substring(pos + 1);
					}
					address = InetAddress.getByName(host);
				} catch (UnknownHostException e) {
					LOG.error("Cannot resolve " + host + " to a hostname/IP address", e);
					continue;
				}

				InstanceType instanceType = null;
				String instanceTypeName = m.group(2);
				if (instanceTypeName != null && instanceTypeName.length() > 0) {

					instanceType = getInstanceTypeByName(instanceTypeName);
					if (instanceType != null) {
						this.ipToInstanceTypeMapping.put(address, instanceType);
					}
				}
			}

			input.close();

		} catch (IOException e) {
			LOG.error("Cannot load IP to instance type mapping from file " + e);
		}
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

	/**
	 * Attempts to load the current network topology from the slave file. If locating or reading the slave file fails,
	 * the method will return an empty network topology.
	 * 
	 * @return the network topology as read from the slave file
	 */
	private NetworkTopology loadNetworkTopology() {

		// Check if slave file exists
		final String configDir = GlobalConfiguration.getString(CONFIG_DIR_KEY, null);
		if (configDir == null) {
			LOG.error("Cannot find configuration directory to load network topology, using flat topology instead");
			return NetworkTopology.createEmptyTopology();
		}

		final File slaveFile = new File(configDir + File.separator + SLAVE_FILE_NAME);
		if (!slaveFile.exists()) {
			LOG.error("Cannot access slave file to load network topology, using flat topology instead");
			return NetworkTopology.createEmptyTopology();
		}

		try {
			return NetworkTopology.fromFile(slaveFile);
		} catch (IOException ioe) {
			LOG.error("Error while loading the network topology: " + StringUtils.stringifyException(ioe));
		}

		return NetworkTopology.createEmptyTopology();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		this.cleanupStaleMachines.cancel();
	}

	/**
	 * Reads the instance types configured in the config file. Each instance type is defined by a key/value pair. The
	 * format of the key is <code>instancemanager.cluster.type.X</code> where X is an ongoing integer number starting at
	 * 1. The format of the value follows the pattern
	 * "instancename,numComputeUnits,numCores,memorySize,diskCapacity,pricePerHour" (see {@link InstanceType}).
	 * 
	 * @return list of available instance types sorted by price (cheapest to
	 *         most expensive)
	 */
	private InstanceType[] populateInstanceTypeArray() {

		final List<InstanceType> instanceTypes = new ArrayList<InstanceType>();

		// read instance types
		int count = 1;
		while (true) {

			final String key = INSTANCE_TYPE_PREFIX_KEY + Integer.toString(count);
			String descr = GlobalConfiguration.getString(key, null);

			if (descr == null) {
				if (count == 1) {
					LOG.error("Configuration does not contain at least one definition for an instance type, "
						+ "using default instance type: " + ConfigConstants.DEFAULT_INSTANCE_TYPE);

					descr = ConfigConstants.DEFAULT_INSTANCE_TYPE;
				} else {
					break;
				}
			}

			// parse entry
			try {
				// if successful add new instance type
				final InstanceType instanceType = InstanceTypeFactory.constructFromDescription(descr);
				LOG.info("Loaded instance type " + instanceType.getIdentifier() + " from the configuration");
				instanceTypes.add(instanceType);
			} catch (Throwable t) {
				LOG.error("Error parsing " + key + ":" + descr + ". Using default using default instance type: "
					+ ConfigConstants.DEFAULT_INSTANCE_TYPE + " for instance type " + count + ".", t);

				break;
			}

			// Increase key index
			++count;
		}

		return instanceTypes.toArray(new InstanceType[instanceTypes.size()]);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InstanceType getDefaultInstanceType() {

		return this.defaultInstanceType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InstanceType getInstanceTypeByName(String instanceTypeName) {

		for (InstanceType it : availableInstanceTypes) {
			if (it.getIdentifier().equals(instanceTypeName)) {
				return it;
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores, int minMemorySize,
			int minDiskCapacity, int maxPricePerHour) {

		// the instances are sorted by price -> the first instance that
		// fulfills/ the requirements is suitable and the cheapest

		for (InstanceType i : availableInstanceTypes) {
			if (i.getNumberOfComputeUnits() >= minNumComputeUnits && i.getNumberOfCores() >= minNumCPUCores
				&& i.getMemorySize() >= minMemorySize && i.getDiskCapacity() >= minDiskCapacity
				&& i.getPricePerHour() <= maxPricePerHour) {
				return i;
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void releaseAllocatedResource(JobID jobID, Configuration conf,
			AllocatedResource allocatedResource) throws InstanceException {

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
	private ClusterInstance createNewHost(InstanceConnectionInfo instanceConnectionInfo,
			HardwareDescription hardwareDescription) {

		// Check if there is a user-defined instance type for this IP address
		InstanceType instanceType = this.ipToInstanceTypeMapping.get(instanceConnectionInfo.getAddress());
		if (instanceType != null) {
			LOG.info("Found user-defined instance type for cluster instance with IP "
				+ instanceConnectionInfo.getAddress() + ": " + instanceType);
		} else {
			instanceType = matchHardwareDescriptionWithInstanceType(hardwareDescription);
			if (instanceType != null) {
				LOG.info("Hardware profile of cluster instance with IP " + instanceConnectionInfo.getAddress()
					+ " matches with instance type " + instanceType);
			} else {
				LOG.error("No matching instance type, cannot create cluster instance");
				return null;
			}
		}

		// Try to match new host with a stub host from the existing topology
		String instanceName = instanceConnectionInfo.getHostName();
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
			instanceName = instanceConnectionInfo.getAddress().toString();
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
	private InstanceType matchHardwareDescriptionWithInstanceType(HardwareDescription hardwareDescription) {

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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo,
			HardwareDescription hardwareDescription) {

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
		}

		host.reportHeartBeat();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void requestInstance(final JobID jobID, Configuration conf,
			final InstanceRequestMap instanceRequestMap,
			final List<String> splitAffinityList) throws InstanceException {

		final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();

		// Iterate over all instance types
		final Iterator<Map.Entry<InstanceType, Integer>> it = instanceRequestMap.getMaximumIterator();
		while (it.hasNext()) {

			// Iterate over all requested instances of a specific type
			final Map.Entry<InstanceType, Integer> entry = it.next();
			final int maximumNumberOfInstances = entry.getValue().intValue();

			for (int i = 0; i < maximumNumberOfInstances; i++) {

				LOG.info("Trying to allocate instance of type " + entry.getKey().getIdentifier());

				// TODO: Introduce topology awareness here
				// TODO: Daniel: Code taken from AbstractScheduler..
				AllocatedSlice slice = null;

				// Try to match the instance type without slicing first
				for (final ClusterInstance host : this.registeredHosts.values()) {
					if (host.getType().equals(entry.getKey())) {
						slice = host.createSlice(entry.getKey(), jobID);
						if (slice != null) {
							break;
						}
					}
				}

				// Use slicing now if necessary
				if (slice == null) {

					for (final ClusterInstance host : this.registeredHosts.values()) {
						slice = host.createSlice(entry.getKey(), jobID);
						if (slice != null) {
							break;
						}
					}

				}

				if (slice == null) {
					if (i < instanceRequestMap.getMinimumNumberOfInstances(entry.getKey())) {
						removeAllSlicesOfJob(jobID);
						throw new InstanceException("Could not find a suitable instance");
					} else {
						break;
					}
				}

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

		if (this.instanceListener != null) {
			final ClusterInstanceNotifier clusterInstanceNotifier = new ClusterInstanceNotifier(
				this.instanceListener, jobID, allocatedResources);
			clusterInstanceNotifier.start();
		}
	}

	private void removeAllSlicesOfJob(final JobID jobID) {

		final List<AllocatedSlice> allocatedSlices = this.slicesOfJobs.remove(jobID);
		for (final AllocatedSlice slice : allocatedSlices) {
			slice.getHostingInstance().removeAllocatedSlice(slice.getAllocationID());
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) {

		// TODO: Make topology job specific
		return this.networkTopology;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setInstanceListener(InstanceListener instanceListener) {
		this.instanceListener = instanceListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		return this.instanceTypeDescriptionMap;
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
}
