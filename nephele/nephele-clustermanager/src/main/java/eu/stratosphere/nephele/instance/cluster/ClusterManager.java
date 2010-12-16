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
import java.util.Collections;
import java.util.Comparator;
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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * Instance Manager for a homogeneous cluster.
 * <p>
 * If Nephele is executed on a compute cluster, no cloud management is available to allocate and release instances
 * (virtual machines). This cluster manager compensates for the lack of a cloud management. The cluster manager acts on
 * the following principle:<br>
 * Each node available for computation in the cluster shall start a task manager. The task manager registers at the job
 * manager and sends periodic heart-beat messages that are forwarded to this {@link ClusterManager}. If a new task
 * manager appears, this means that a new host has been registered that is available for being used. If a task manager
 * stops sending heat-beat messages, the respective host has died.
 * <p>
 * Each host in the cluster executes just one task manager. This host can be partitioned by number of CPUs, memory, etc.
 * The cluster manager shall take care that one host of 8 CPUs can create for example 4 instances of two CPUs each.
 * <p>
 * This is a sample configuration: <code>
 * # machine is dead if it did not sent heat-beat messages for 120 seconds
 * cloud.ec2.cleanupinterval = 120000
 * # name of user who requests resources (not used)
 * job.cloud.username = battre
 * 
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
 * 
 * # type of all nodes in the cluster
 *  # compute units for entire CPU: 8x2500 = 20000
 * clustermgr.standardhost.numComputeUnits = 20000
 *  # number of cores: 8
 * clustermgr.standardhost.numCores        = 8
 *  # memory size in MB: 32 GB = 32*1024 MB = 32768 MB
 * clustermgr.standardhost.memorySize      = 32768
 *  # disk capacity in GB: 200 GB   
 * clustermgr.standardhost.diskCapacity    = 200
 * </code> This class is thread-safe.
 * 
 * @author Dominic Battre
 */
public class ClusterManager implements InstanceManager {

	/** Logging facility */
	private static final Log LOG = LogFactory.getLog(ClusterManager.class);

	/**
	 * Period after which we check whether hosts did not send heart-beat
	 * messages.
	 */
	private static final int BASE_INTERVAL = 10 * 1000; // 10 sec.

	/**
	 * The key to retrieve the index of the default instance type from the configuration.
	 */
	private static final String DEFAULT_INSTANCE_TYPE_INDEX_KEY = "jobmanager.instancemanager.cluster.defaulttype";

	/**
	 * The key to retrieve the clean up interval from the configuration.
	 */
	private static final String CLEANUP_INTERVAL_KEY = "jobmanager.instancemanager.cluster.cleanupinterval";

	/**
	 * Default duration after which a host is purged in case it did not send
	 * a heart-beat message.
	 */
	private static final int DEFAULT_CLEANUP_INTERVAL = 2 * 60; // 2 min.

	/**
	 * Duration after which a host is purged in case it did not send a
	 * heart-beat message.
	 */
	private final long cleanUpInterval;

	/** Object that is notified if instances become available or vanish */
	private InstanceListener instanceListener;

	/**
	 * The key prefix to retrieve the definition of the individual instance types.
	 */
	private static final String INSTANCE_TYPE_PREFIX_KEY = "jobmanager.instancemanager.cluster.type.";

	/**
	 * The default definition for an instance type, if no other configuration is provided.
	 */
	private static final String DEFAULT_INSTANCE_TYPE = "default,2,1,2048,10,10";

	private static final int DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX = 1;

	private final int defaultInstanceTypeIndex;

	private final static String CONFIG_DIR_KEY = "config.dir";

	/**
	 * Set of hosts known to run a task manager that are thus able to execute
	 * tasks.
	 * <p>
	 * The key of this hash set is the socket of the task manager from which we have received a heart-beat. As long as
	 * we keep receiving heart-beats we assume the cluster node is online.
	 */
	private final Map<InstanceConnectionInfo, ClusterInstance> registeredHosts = Maps.newHashMap();

	/**
	 * Map of a {@link JobID} to all {@link AllocatedSlice}s that belong to this job.
	 */
	private final Multimap<JobID, AllocatedSlice> slicesOfJob = HashMultimap.create();

	/**
	 * The name of the file which contains the IP to instance type mapping.
	 */
	private static final String SLAVE_FILE_NAME = "slaves";

	/**
	 * Map of IP addresses to instance types.
	 */
	private final Map<InetAddress, InstanceType> ipToInstanceTypeMapping = Maps.newHashMap();

	/**
	 * List of instance types that can be executed on this cluster, sorted by
	 * price (cheapest to most expensive).
	 */
	private final InstanceType[] availableInstanceTypes;

	private final NetworkTopology networkTopology;

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

	/**
	 * Constructor.
	 */
	public ClusterManager() {

		// Load the instance type this cloud can offer
		this.availableInstanceTypes = populateInstanceTypeArray();

		long tmpCleanUpInterval = (long) GlobalConfiguration.getInteger(CLEANUP_INTERVAL_KEY, DEFAULT_CLEANUP_INTERVAL) * 1000;

		if ((tmpCleanUpInterval % BASE_INTERVAL) != 0) {
			LOG.warn("Invalid clean up interval. Reverting to " + DEFAULT_CLEANUP_INTERVAL);
			tmpCleanUpInterval = DEFAULT_CLEANUP_INTERVAL;
		}

		this.cleanUpInterval = tmpCleanUpInterval;

		int tmpDefaultInstanceTypeIndex = GlobalConfiguration.getInteger(DEFAULT_INSTANCE_TYPE_INDEX_KEY,
			DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);
		if (tmpDefaultInstanceTypeIndex >= this.availableInstanceTypes.length) {
			LOG.warn("Incorrect index to for default instance type, switching to default "
				+ DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX);
			tmpDefaultInstanceTypeIndex = DEFAULT_DEFAULT_INSTANCE_TYPE_INDEX;
		}
		this.defaultInstanceTypeIndex = tmpDefaultInstanceTypeIndex;

		this.networkTopology = loadNetworkTopology();

		// load IP to instance type mapping from slave file
		loadIPToInstanceTypeMapping();

		// look every BASEINTERVAL milliseconds for crashed hosts
		final boolean runTimerAsDaemon = true;
		new Timer(runTimerAsDaemon).schedule(cleanupStaleMachines, BASE_INTERVAL, BASE_INTERVAL);
	}

	/**
	 * Attempts to load the current network topology from the
	 * slave file. If locating or reading the slave file fails, the method
	 * will return an empty network topology.
	 * 
	 * @return
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

		final List<InstanceType> instanceTypes = Lists.newArrayList();

		// read instance types
		int count = 1;
		while (true) {

			final String key = INSTANCE_TYPE_PREFIX_KEY + Integer.toString(count);
			String descr = GlobalConfiguration.getString(key, null);
			if (descr == null) {
				if (count == 1) {
					LOG
						.error("Configuration does not contain at least one definition for an instance type, using default "
							+ DEFAULT_INSTANCE_TYPE);
					descr = DEFAULT_INSTANCE_TYPE;
				} else {
					break;
				}
			}

			// parse entry
			try {
				// if successful add new instance type
				instanceTypes.add(InstanceType.getTypeFromString(descr));
			} catch (Throwable t) {
				LOG.error("Error parsing " + key + ":" + descr, t);
			}

			// Increase key index
			++count;
		}

		// sort by price
		Collections.sort(instanceTypes, new Comparator<InstanceType>() {
			@Override
			public int compare(InstanceType o1, InstanceType o2) {
				return o1.getPricePerHour() - o2.getPricePerHour();
			}
		});

		return instanceTypes.toArray(new InstanceType[0]);
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

		final Pattern pattern = Pattern.compile("^(\\S+)\\s*(\\S*)\\s*$");

		try {

			final BufferedReader input = new BufferedReader(new FileReader(slaveFile));

			String line = null;

			while ((line = input.readLine()) != null) {

				final Matcher m = pattern.matcher(line);
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
				if (instanceTypeName == null || instanceTypeName.length() == 0) {
					instanceType = getDefaultInstanceType();
				} else {
					instanceType = getInstanceTypeByName(instanceTypeName);
					if (instanceType == null) {
						instanceType = getDefaultInstanceType();
						LOG.warn(m.group(2) + " does not refer to a valid instance type, switching to default");
					}
				}

				this.ipToInstanceTypeMapping.put(address, instanceType);
			}

			input.close();

		} catch (IOException e) {
			LOG.error("Cannot load IP to instance type mapping from file: " + e);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InstanceType getDefaultInstanceType() {

		return this.availableInstanceTypes[this.defaultInstanceTypeIndex - 1];
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
	public synchronized void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo) {
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
}
