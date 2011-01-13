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
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Instance Manager for a static cluster.
 * <p>
 * The cluster manager can handle heterogeneous instances (compute nodes). Each instance type used in the cluster must
 * be described in the configuration. The configuration must include the number of different instance types as well as
 * description of the hardware profile of each instance type.
 * <p>
 * This is a sample configuration: <code>
 * # number of instance types defined in this cluster
 * clustermgr.nrtypes = 5 
 * 
 * # definition of instances in format
 * # instancename,numComputeUnits,numCores,memorySize,diskCapacity,pricePerHour
 * clustermgr.instancetype.1 = m1.small,2,1,2048,10,10
 * clustermgr.instancetype.2 = c1.medium,2,1,2048,10,10
 * clustermgr.instancetype.3 = m1.large,4,2,2048,10,10
 * clustermgr.instancetype.4 = m1.xlarge,8,4,8192,20,20
 * clustermgr.instancetype.5 = c1.xlarge,8,4,16384,20,40
 * 
 * # default instance type
 * clustermgr.instancetype.defaultInstance = m1.large
 * </code> Each instance is expected to run exactly one {@link TaskManager}. When the {@link TaskManager} registers with
 * the {@link JobManager} it sends a {@link HardwareDescription} which describes the actual hardware characteristics of
 * the instance (compute node). The cluster manage will attempt to match the report hardware characteristics with one of
 * the configured instance types. Moreover, the cluster manager is capable of partitioning larger instances (compute
 * nodes) into smaller, less powerful instances.
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
	 * Period after which we check whether hosts did not send heart-beat
	 * messages.
	 */
	private static final int BASE_INTERVAL = 10 * 1000; // 10 sec.

	/**
	 * Default duration after which a host is purged in case it did not send
	 * a heart-beat message.
	 */
	private static final int DEFAULT_CLEANUP_INTERVAL = 2 * 60; // 2 min.

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
	private final Map<InstanceConnectionInfo, ClusterInstance> registeredHosts = Maps.newHashMap();

	/**
	 * Map of a {@link JobID} to all {@link AllocatedSlice}s that belong to this job.
	 */
	private final Multimap<JobID, AllocatedSlice> slicesOfJob = HashMultimap.create();

	/**
	 * Map of IP addresses to instance types.
	 */
	private final Map<InetAddress, InstanceType> ipToInstanceTypeMapping = Maps.newHashMap();

	/**
	 * List of instance types that can be executed on this cluster, sorted by
	 * price (cheapest to most expensive).
	 */
	private final InstanceType[] availableInstanceTypes;

	/**
	 * 
	 */
	private final NetworkTopology networkTopology;

	/**
	 * Object that is notified if instances become available or vanish
	 */
	private InstanceListener instanceListener;

	/**
	 * Periodic task that checks whether hosts have not sent their heart-beat
	 * messages and purges the hosts in this case.
	 */
	private final TimerTask cleanupStaleMachines = new TimerTask() {

		@Override
		public void run() {

			synchronized (ClusterManager.this) {

				List<Map.Entry<InstanceConnectionInfo, ClusterInstance>> hostsToRemove = Lists.newArrayList();

				// check all hosts whether they did not send heat-beat messages.
				for (Map.Entry<InstanceConnectionInfo, ClusterInstance> entry : registeredHosts.entrySet()) {

					ClusterInstance host = entry.getValue();
					if (!host.isStillAlive(cleanUpInterval)) {

						// this host has not sent the heat-beat messages
						// -> we terminate all instances running on this host and notify the jobs
						final List<AllocatedSlice> removedSlices = host.removeAllAllocatedSlices();
						for (AllocatedSlice removedSlice : removedSlices) {

							slicesOfJob.remove(removedSlice.getJobID(), removedSlice);

							if (instanceListener != null) {
								instanceListener.allocatedResourceDied(removedSlice.getJobID(), new AllocatedResource(
									removedSlice.getHostingInstance(), removedSlice.getAllocationID()));
							}
						}

						hostsToRemove.add(entry);
					}
				}

				registeredHosts.entrySet().removeAll(hostsToRemove);
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

		// Load the instance type this cluster can offer
		this.availableInstanceTypes = populateInstanceTypeArray();

		long tmpCleanUpInterval = (long) GlobalConfiguration.getInteger(
			ConfigConstants.INSTANCE_MANAGER_CLEANUP_INTERVAL_KEY, DEFAULT_CLEANUP_INTERVAL) * 1000;

		if ((tmpCleanUpInterval % BASE_INTERVAL) != 0) {
			LOG.warn("Invalid clean up interval. Reverting to default cleanup interval of " +
				DEFAULT_CLEANUP_INTERVAL + " secs.");
			tmpCleanUpInterval = DEFAULT_CLEANUP_INTERVAL;
		}

		this.cleanUpInterval = tmpCleanUpInterval;

		int tmpDefaultInstanceTypeIndex = GlobalConfiguration.getInteger(
			ConfigConstants.INSTANCE_MANAGER_DEFAULT_INSTANCE_TYPE_INDEX_KEY,
			ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);

		if (tmpDefaultInstanceTypeIndex > this.availableInstanceTypes.length) {
			LOG.warn("Incorrect index to for default instance type (" + tmpDefaultInstanceTypeIndex +
				"), switching to default index " + ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);

			tmpDefaultInstanceTypeIndex = ConfigConstants.DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX;
		}

		this.defaultInstanceType = this.availableInstanceTypes[tmpDefaultInstanceTypeIndex - 1];

		// sort available instances by CPU core
		sortAvailableInstancesByNumberOfCPUCores();

		// load the network topology from the slave file
		this.networkTopology = loadNetworkTopology();

		// look every BASEINTERVAL milliseconds for crashed hosts
		final boolean runTimerAsDaemon = true;
		new Timer(runTimerAsDaemon).schedule(cleanupStaleMachines, BASE_INTERVAL, BASE_INTERVAL);
	}

	/**
	 * Sorts the list of available instance types by the number of CPU cores in an ascending order.
	 */
	private void sortAvailableInstancesByNumberOfCPUCores() {

		if (this.availableInstanceTypes.length < 2) {
			return;
		}

		for (int i = 1; i < this.availableInstanceTypes.length; i++) {
			final InstanceType it = this.availableInstanceTypes[i];
			int j = i;
			while (j > 0 && this.availableInstanceTypes[j - 1].getNumberOfCores() > it.getNumberOfCores()) {
				this.availableInstanceTypes[j] = this.availableInstanceTypes[j - 1];
				--j;
			}
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
	 * Reads the instance types configured in the config file.
	 * The config file needs to contain a key <code>clustermgr.nrtypes</code> that indicates the number of instance
	 * types that are supported by the
	 * cluster. This is followed by entries <code>clustermgr.instancetype.X</code> where X is a number from 1 to
	 * the specified number of entries. Each entry follows the format:
	 * "instancename,numComputeUnits,numCores,memorySize,diskCapacity,pricePerHour"
	 * (see {@link InstanceType}).
	 * 
	 * @return list of available instance types sorted by price (cheapest to
	 *         most expensive)
	 */
	private InstanceType[] populateInstanceTypeArray() {

		final List<InstanceType> instanceTypes = new ArrayList<InstanceType>();

		// read instance types
		int count = 1;
		while (true) {

			final String key = ConfigConstants.INSTANCE_MANAGER_INSTANCE_TYPE_PREFIX_KEY + Integer.toString(count);
			String descr = GlobalConfiguration.getString(key, null);

			if (descr == null) {
				if (count == 1) {
					LOG.error("Configuration does not contain at least one definition for an instance type, " +
							"using default instance type: " + ConfigConstants.DEFAULT_INSTANCE_TYPE);

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
				LOG.error("Error parsing " + key + ":" + descr + ". Using default using default instance type: " +
					ConfigConstants.DEFAULT_INSTANCE_TYPE + " for instance type " + count + ".", t);

				// we need to add an instance type anyways, because otherwise a non-parsable instance description
				// would cause the numbering to be wrong.
				instanceTypes.add(InstanceTypeFactory.constructFromDescription(ConfigConstants.DEFAULT_INSTANCE_TYPE));
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
		this.slicesOfJob.remove(jobID, removedSlice);
	}

	/**
	 * Creates a new {@link ClusterInstance} object to manage instances that can
	 * be executed on that host.
	 * 
	 * @param instanceConnectionInfo
	 *        the connection information for the instance
	 * @return a new {@link ClusterInstance} object
	 */
	private ClusterInstance createNewHost(InstanceConnectionInfo instanceConnectionInfo) {

		InstanceType instanceType = this.ipToInstanceTypeMapping.get(instanceConnectionInfo.getAddress());
		if (instanceType == null) {
			LOG.warn("Received heart beat from unexpected instance " + instanceConnectionInfo);
			instanceType = getDefaultInstanceType();
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
			this.networkTopology);

		return host;
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
			host = createNewHost(instanceConnectionInfo);

			if (host == null) {
				LOG.error("Could not create a new host object for incoming heart-beat. "
					+ "Probably the configuration file is lacking some entries.");
				return;
			}

			this.registeredHosts.put(instanceConnectionInfo, host);
			LOG.info("New number of registered hosts is " + this.registeredHosts.size());
		}

		host.reportHeartBeat();
	}

	@Override
	public synchronized void requestInstance(JobID jobID, Configuration conf, InstanceType instanceType)
			throws InstanceException {

		// TODO: Introduce topology awareness here
		for (ClusterInstance host : registeredHosts.values()) {
			final AllocatedSlice slice = host.createSlice(instanceType, jobID);
			if (slice != null) {
				this.slicesOfJob.put(jobID, slice);

				if (this.instanceListener != null) {
					ClusterInstanceNotifier clusterInstanceNotifier = new ClusterInstanceNotifier(
						this.instanceListener, slice);
					clusterInstanceNotifier.start();
				}
				return;
			}
		}

		throw new InstanceException("Could not find a suitable instance");
	}

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

	@Override
	public List<InstanceTypeDescription> getListOfAvailableInstanceTypes() {
		// TODO Auto-generated method stub
		return null;
	}
}
