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

package eu.stratosphere.nephele.instance.cloud;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AbstractInstance;
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
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The task of cloud manager is managing instances (virtual machines) in the cloud. The cloud manager can allocate and
 * release instances.
 * There are three types of instances: reserved instance, cloud instance and floating instance. The reserved instance is
 * reserved for a
 * specific job. The cloud instance is an instance in the running state. The floating instance is idle and not reserved
 * for any job. The
 * type of an instance in the cloud may vary among these three states. One instance belongs to only one user, i.e. the
 * user can use only
 * his own instances in the cloud. The user pays fee for allocated instances hourly. The cloud manager checks the
 * floating instances
 * every 30 seconds in order to terminate the floating instances which expire.
 */
public class CloudManager extends TimerTask implements InstanceManager {

	/** The log for the cloud manager. */
	private static final Log LOG = LogFactory.getLog(CloudManager.class);

	/** Decides whether the data should be encrypted on the wire on the way from or to EC2 Web Services. */
	private static final String EC2WSSECUREKEY = "cloud.ec2ws.secure";

	/** The host name of EC2 Web Services. */
	private static final String EC2WSSERVERKEY = "cloud.ec2ws.server";

	/** The port number of EC2 Web Services. */
	private static final String EC2WSPORTKEY = "cloud.ec2ws.port";

	/**
	 * The cloud manager checks the floating instances every base interval to terminate the floating instances which
	 * expire.
	 */
	private static final int BASEINTERVAL = 60 * 1000; // 1 min

	/**
	 * The interval of cleaning up of the cloud instance. If the cloud instance does not receive the heart beat from the
	 * cloud manager for more than the interval, the cloud manager will regard it as dead.
	 */
	private static final int DEFAULTCLEANUPINTERVAL = 2 * 60 * 1000; // 2 min

	/** The user pays fee for his instances every time unit. */
	private static final long TIMEUNIT = 60 * 60 * 1000; // 1 hour

	/** The array of all available instance types in the cloud. */
	private final InstanceType[] availableInstanceTypes;

	/** Mapping jobs to instances. */
	private final Map<JobID, JobToInstancesMapping> jobToInstancesMap = new HashMap<JobID, JobToInstancesMapping>();

	/** All reserved instances. Mapping instance IDs to job IDs. */
	private final Map<String, JobID> reservedInstances = new HashMap<String, JobID>();

	/** All floating instances. Mapping task managers' addresses to floating instances. */
	private final Map<InstanceConnectionInfo, FloatingInstance> floatingInstances = new HashMap<InstanceConnectionInfo, FloatingInstance>();

	/** The list of all cloud instances. */
	private final List<CloudInstance> cloudInstances = new ArrayList<CloudInstance>();

	/** Mapping floating instance IDs to their configurations. */
	private final Map<String, Configuration> floatingInstanceIDs = new HashMap<String, Configuration>();

	/** Object that is notified if instances become available or vanish. */
	private InstanceListener instanceListener;

	/** If the timer triggers, the cloud manager checks the floating instances which expire and terminates them. */
	private final Timer timer = new Timer(true);

	/**
	 * The interval of cleaning up of the cloud instance. If the cloud instance does not receive the heart beat from the
	 * cloud manager for more than the interval, the cloud manager will regard it as dead.
	 */
	private long cleanUpInterval = DEFAULTCLEANUPINTERVAL;

	/**
	 * The network topology inside the cloud (currently only a fake).
	 */
	private final NetworkTopology networkTopology;

	/**
	 * Creates the cloud manager.
	 */
	public CloudManager() {

		// Load the instance type this cloud can offer
		this.availableInstanceTypes = populateInstanceTypeArray();

		this.cleanUpInterval = (long) GlobalConfiguration.getInteger("cloud.ec2.cleanupinterval",
			DEFAULTCLEANUPINTERVAL);

		if ((this.cleanUpInterval % BASEINTERVAL) != 0) {
			LOG.warn("Invalid clean up interval. Reverting to " + DEFAULTCLEANUPINTERVAL);
			this.cleanUpInterval = DEFAULTCLEANUPINTERVAL;
		}

		this.networkTopology = NetworkTopology.createEmptyTopology();

		// TODO: Clean up seems to be broken, fix it!
		this.timer.schedule(this, (long) BASEINTERVAL, (long) BASEINTERVAL);
	}

	/**
	 * Reads the instance types from a configuration file.
	 * The configuration file contains a key <code>cloudmgr.nrtypes</code> which indicates the number of instance types
	 * in the cloud. This is followed by entries <code>cloudmgr.instancetype.X</code> where X is a number from 1 to the
	 * specified number of entries. Each entry follows the format: "instanceName,numComputeUnits,numCores,memorySize,
	 * diskCapacity,pricePerHour" (see {@link InstanceType}).
	 * 
	 * @return list of instance types sorted by price (cheapest to most expensive)
	 */
	private InstanceType[] populateInstanceTypeArray() {

		final List<InstanceType> instanceTypes = new ArrayList<InstanceType>();

		// read the number of instance types
		final int num = GlobalConfiguration.getInteger("cloudmgr.nrtypes", -1);
		if (num <= 0) {
			throw new RuntimeException("Illegal configuration, cloudmgr.nrtypes is not configured");
		}

		for (int i = 0; i < num; ++i) {

			final String key = "cloudmgr.instancetype." + (i + 1);
			final String type = GlobalConfiguration.getString(key, null);
			if (type == null) {
				throw new RuntimeException("Illegal configuration for " + key);
			}

			instanceTypes.add(InstanceTypeFactory.constructFromDescription(type));
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
	 * Returns the suitable instance type.
	 * 
	 * @param minNumComputeUnits
	 *        the minimal number of compute units
	 * @param minNumCPUCores
	 *        the minimal number of CPU cores
	 * @param minMemorySize
	 *        the minimal main memory size in MB
	 * @param minDiskCapacity
	 *        the minimal disk capacity in GB
	 * @param maxPricePerHour
	 *        the maximal price per hour
	 * @return the suitable instance type
	 */
	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores, int minMemorySize,
			int minDiskCapacity, int maxPricePerHour) {

		// instances are sorted by price, so the first instance that meets the demands is the suitable instance
		for (InstanceType i : availableInstanceTypes) {
			if ((i.getNumberOfComputeUnits() >= minNumComputeUnits) && (i.getNumberOfCores() >= minNumCPUCores)
				&& (i.getMemorySize() >= minMemorySize) && (i.getDiskCapacity() >= minDiskCapacity)
				&& (i.getPricePerHour() <= maxPricePerHour)) {
				return i;
			}
		}

		return null;
	}

	/**
	 * A cloud instance is released and changed into a floating instance, waiting for a new job from its owner.
	 * 
	 * @param jobID
	 *        the ID of the finished job
	 * @param conf
	 *        the configuration of the finished job
	 * @param instance
	 *        the cloud instance which is to be released
	 */
	@Override
	public synchronized void releaseAllocatedResource(JobID jobID, Configuration conf,
			AllocatedResource allocatedResource) {

		final AbstractInstance instance = allocatedResource.getInstance();

		JobToInstancesMapping jobToInstanceMapping = null;

		synchronized (this.jobToInstancesMap) {
			jobToInstanceMapping = this.jobToInstancesMap.get(jobID);

			if (jobToInstanceMapping == null) {
				LOG.error("No mapping for job " + jobID);
				return;
			}
		}

		if (jobToInstanceMapping.getAssignedInstances().contains(instance)) {

			jobToInstanceMapping.unassignedInstanceFromJob((CloudInstance) instance);
			this.cloudInstances.remove(instance);

			final long currentTime = System.currentTimeMillis();
			final long remainingTime = ((CloudInstance) instance).getAllocationTime()
				+ ((currentTime - ((CloudInstance) instance).getAllocationTime()) / TIMEUNIT + 1) * TIMEUNIT
				- currentTime;

			this.floatingInstances.put(instance.getInstanceConnectionInfo(), new FloatingInstance(
				((CloudInstance) instance).getInstanceID(), instance.getInstanceConnectionInfo(), currentTime,
				remainingTime));
			this.floatingInstanceIDs.put(((CloudInstance) instance).getInstanceID(), conf);

			LOG.info("Convert " + ((CloudInstance) instance).getInstanceID()
				+ " from allocated instance to floating instance");

		} else {
			LOG.error("Job " + jobID + " contains no such instance");
			return;
		}
	}

	/**
	 * Terminates an instance (virtual machine) in the cloud.
	 * 
	 * @param conf
	 *        the configuration of the job, whom the terminating instance belongs to.
	 * @param instanceID
	 *        the ID of the terminating instance
	 * @return the ID of the terminated instance
	 * @throws InstanceException
	 *         something wrong happens to the job configuration
	 */
	private String destroyCloudInstance(Configuration conf, String instanceID) throws InstanceException {

		if (conf == null) {
			throw new InstanceException("No job configuration provided, unable to acquire credentials");
		}

		// First check, if all required configuration entries are available
		final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		if (awsAccessId == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS access ID");
		}

		final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);
		if (awsSecretKey == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS secret key");
		}

		AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);

		TerminateInstancesRequest tr = new TerminateInstancesRequest();
		LinkedList<String> instances = new LinkedList<String>();
		instances.add(instanceID);
		tr.setInstanceIds(instances);
		TerminateInstancesResult trr = ec2client.terminateInstances(tr);

		return trr.getTerminatingInstances().get(0).getInstanceId();

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo,
			HardwareDescription hardwareDescription) {

		// Check if heart beat belongs to a floating instance
		if (this.floatingInstances.containsKey(instanceConnectionInfo)) {
			final FloatingInstance floatingInstance = this.floatingInstances.get(instanceConnectionInfo);
			floatingInstance.updateLastReceivedHeartBeat();
			return;
		}

		// Check if heart beat belongs to an assigned instance
		CloudInstance instance = isAssignedInstance(instanceConnectionInfo);
		if (instance != null) {
			// If it's an assigned instance, just update the heart beat time stamp and leave
			instance.updateLastReceivedHeartBeat();
			return;
		}

		// Check if heart beat belongs to a reserved instance
		try {
			instance = isReservedInstance(instanceConnectionInfo);
		} catch (InstanceException e) {
			LOG.error(e);
		}
		if (instance != null) {
			JobID jobID = null;
			jobID = this.reservedInstances.get(instance.getInstanceID());
			if (jobID == null) {
				LOG.error("Cannot find job ID to instance ID " + instance.getInstanceID());
				return;
			}

			this.reservedInstances.remove(instance.getInstanceID());
			instance.updateLastReceivedHeartBeat();

			final JobToInstancesMapping mapping = this.jobToInstancesMap.get(jobID);
			if (mapping == null) {
				LOG.error("Cannot find mapping for job ID " + jobID);
				return;
			}
			mapping.assignInstanceToJob(instance);
			// Trigger notification that instance is available (outside synchronized section)
			this.instanceListener.resourceAllocated(jobID, instance.asAllocatedResource());
			return;
		}

	}

	/**
	 * Returns a cloud instance if this instance is assigned to a job.
	 * 
	 * @param instanceConnectionInfo
	 *        the connection information object identifying the instance
	 * @return a cloud instance
	 */
	private CloudInstance isAssignedInstance(InstanceConnectionInfo instanceConnectionInfo) {

		synchronized (this.jobToInstancesMap) {

			final Iterator<JobID> it = this.jobToInstancesMap.keySet().iterator();

			while (it.hasNext()) {

				final JobID jobID = it.next();
				final JobToInstancesMapping m = this.jobToInstancesMap.get(jobID);
				final CloudInstance instance = m.getInstanceByConnectionInfo(instanceConnectionInfo);
				if (instance != null) {
					return instance;
				}
			}
		}

		return null;
	}

	/**
	 * Converts a reserved instance into a cloud instance and returns it.
	 * 
	 * @param instanceConnectionInfo
	 *        the {@link InstanceConnectionInfo} object identifying the instance
	 * @return a cloud instance
	 * @throws InstanceException
	 *         something wrong happens to the global configuration
	 */
	private CloudInstance isReservedInstance(InstanceConnectionInfo instanceConnectionInfo) throws InstanceException {

		if (instanceConnectionInfo == null) {
			LOG.warn("Supplied instance connection info is null");
			return null;
		}

		synchronized (this.reservedInstances) {

			if (this.reservedInstances.size() == 0) {
				return null;
			}

			// Collect Jobs that have reserved instances
			final HashSet<JobID> jobsWithReservedInstances = new HashSet<JobID>();

			for (JobID id : this.reservedInstances.values()) {
				jobsWithReservedInstances.add(id);
			}

			// Now we call the webservice for each job..

			for (JobID id : jobsWithReservedInstances) {

				JobToInstancesMapping mapping = null;

				synchronized (this.jobToInstancesMap) {
					mapping = this.jobToInstancesMap.get(id);
				}

				if (mapping == null) {
					LOG.error("Unknown mapping for job ID " + id);
					continue;
				}

				AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(mapping.getAwsAccessId(),
					mapping.getAwsSecretKey());

				DescribeInstancesRequest request = new DescribeInstancesRequest();
				DescribeInstancesResult result = ec2client.describeInstances(request);

				// Iterate over all Instances
				for (Reservation r : result.getReservations()) {
					for (Instance t : r.getInstances()) {
						InetAddress candidateAddress = null;

						try {
							candidateAddress = InetAddress.getByName(t.getPrivateIpAddress());
						} catch (UnknownHostException e) {
							LOG.warn("Cannot convert " + t.getPrivateIpAddress() + " into an IP address");
							continue;
						}

						if (instanceConnectionInfo.getAddress().equals(candidateAddress)) {
							return convertIntoCloudInstance(t, instanceConnectionInfo, mapping.getOwner());
						}
					}
				}

			}

		}

		return null;
	}

	/**
	 * Creates a cloud instance and returns it.
	 * 
	 * @param instance
	 *        the instance to be converted into the cloud instance
	 * @param instanceConnectionInfo
	 *        the information required to connect to the instance's task manager later on
	 * @param owner
	 *        the owner of the instance
	 * @return a cloud instance
	 */
	private CloudInstance convertIntoCloudInstance(com.amazonaws.services.ec2.model.Instance instance,
			InstanceConnectionInfo instanceConnectionInfo, String owner) {

		InstanceType type = null;

		// Find out type of the instance
		for (int i = 0; i < this.availableInstanceTypes.length; i++) {

			if (this.availableInstanceTypes[i].getIdentifier().equals(instance.getInstanceType())) {
				type = this.availableInstanceTypes[i];
				break;
			}
		}

		if (type == null) {
			LOG.error("Cannot translate " + instance.getInstanceType() + " into a valid instance type");
			return null;
		}

		final CloudInstance cloudInstance = new CloudInstance(instance.getInstanceId(), type, owner,
			instanceConnectionInfo, instance.getLaunchTime().getTime(), this.networkTopology.getRootNode(),
			this.networkTopology, null); // TODO: Define hardware descriptions for cloud instance types
		this.cloudInstances.add(cloudInstance);
		return cloudInstance;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestInstance(JobID jobID, Configuration conf, Map<InstanceType, Integer> instanceMap,
			List<String> splitAffinityList) throws InstanceException {

		if (conf == null) {
			throw new InstanceException("No job configuration provided, unable to acquire credentials");
		}
		// TODO: maybe load default credentials from config?

		// First check, if all required configuration entries are available
		/*
		 * final String owner = conf.getString("job.cloud.username", null);
		 * if (owner == null) {
		 * throw new InstanceException("Unable to allocate cloud instance: Cannot find username");
		 * }
		 * final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		 * if (awsAccessId == null) {
		 * throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS access ID");
		 * }
		 * final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);
		 * if (awsSecretKey == null) {
		 * throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS secret key");
		 * }
		 */

		final String awsAccessId = "AKIAJYQJNI7QH227NDQA";
		final String awsSecretKey = "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";
		final String owner = "nobody";

		final String sshKeyPair = conf.getString("job.cloud.sshkeypair", null);

		JobToInstancesMapping jobToInstanceMapping = null;

		// Check if there already exist a mapping for this job
		synchronized (this.jobToInstancesMap) {
			jobToInstanceMapping = this.jobToInstancesMap.get(jobID);

			// Create new mapping if it does not yet exist
			if (jobToInstanceMapping == null) {
				LOG.debug("Creating new mapping for job " + jobID);
				jobToInstanceMapping = new JobToInstancesMapping(owner, awsAccessId, awsSecretKey);
				this.jobToInstancesMap.put(jobID, jobToInstanceMapping);
			}
		}

		// Map containing the instances that will actually be requested via the EC2 interface..
		Map<InstanceType, Integer> instancesToBeRequested = new HashMap<InstanceType, Integer>();

		// First we check, if there are any floating instances available that we can use

		// Iterate over all instance types
		final Iterator<Map.Entry<InstanceType, Integer>> it = instanceMap.entrySet().iterator();
		while (it.hasNext()) {
			final Map.Entry<InstanceType, Integer> entry = it.next();
			final InstanceType actualInstanceType = entry.getKey();
			final int neededinstancecount = entry.getValue();

			LOG.info("Request for " + neededinstancecount + " instances of type " + actualInstanceType.getIdentifier());

			// Now check, if floating instances of specific type are available...
			final LinkedList<CloudInstance> floatinginstances = anyFloatingInstanceAvailable(owner, awsAccessId,
				awsSecretKey, actualInstanceType, neededinstancecount);

			// now we assign all found floating instances...
			final JobToInstancesMapping mapping = this.jobToInstancesMap.get(jobID);
			if (mapping == null) {
				LOG.error("Cannot find mapping for job ID " + jobID);
				return;
			}

			LOG.info("Found " + floatinginstances.size() + " suitable floating instances.");
			for (CloudInstance ci : floatinginstances) {
				mapping.assignInstanceToJob(ci);
				CloudInstanceNotifier notifier = new CloudInstanceNotifier(this.instanceListener, jobID, ci);
				notifier.start();
			}

			if (floatinginstances.size() < neededinstancecount) {
				// we (still?) need to request new instances.

				// Add instances that need to be requested to the map..
				final int instancerequestcount = neededinstancecount - floatinginstances.size();
				instancesToBeRequested.put(actualInstanceType, instancerequestcount);

			}

		}// End iterating over instance types

		// Now, we need to request the EC2 instances..

		LinkedList<String> instanceIDs = allocateCloudInstance(awsAccessId, awsSecretKey, instancesToBeRequested,
			sshKeyPair);

		for (String i : instanceIDs) {
			this.reservedInstances.put(i, jobID);

		}
	}

	/**
	 * Requests (allocates) instances (VMs) from Amazon EC2.
	 * 
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 * @param instancesToBeRequested
	 *        Map containing desired instances types and count
	 * @param sshKeyPair
	 *        Optional parameter to insert an EC2 SSH key/value pair
	 * @return
	 */
	private LinkedList<String> allocateCloudInstance(String awsAccessId, String awsSecretKey,
			Map<InstanceType, Integer> instancesToBeRequested, String sshKeyPair) {

		/*
		 * final String imageID = GlobalConfiguration.getString("ec2.image.id", null);
		 * if (imageID == null) {
		 * LOG.error("Unable to allocate instance: Image ID is unknown");
		 * return null;
		 * }
		 */
		final String imageID = "ami-ea5b6b9e";
		final String jobManagerIPAddress = GlobalConfiguration.getString("jobmanager.rpc.address", null);
		if (jobManagerIPAddress == null) {
			LOG.error("JobManager IP address is not set (jobmanager.rpc.address)");
			return null;
		}

		AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);
		LinkedList<String> instanceIDs = new LinkedList<String>();

		// Iterate over instance types..

		final Iterator<Map.Entry<InstanceType, Integer>> it = instancesToBeRequested.entrySet().iterator();

		while (it.hasNext()) {
			final Map.Entry<InstanceType, Integer> entry = it.next();
			final InstanceType actualInstanceType = entry.getKey();
			final int neededinstancecount = entry.getValue();

			RunInstancesRequest request = new RunInstancesRequest(imageID, neededinstancecount, neededinstancecount);
			request.setInstanceType(actualInstanceType.getIdentifier());

			// TODO: Make this configurable!
			BlockDeviceMapping bdm = new BlockDeviceMapping();
			bdm.setVirtualName("ephemeral0");
			bdm.setDeviceName("/dev/sdb1");

			if (sshKeyPair != null) {
				request.setKeyName(sshKeyPair);
			}

			LinkedList<BlockDeviceMapping> bdmlist = new LinkedList<BlockDeviceMapping>();
			bdmlist.add(bdm);
			request.setBlockDeviceMappings(bdmlist);

			// Setting User-Data parameters
			request.setUserData(EC2Utilities.createTaskManagerUserData(jobManagerIPAddress));

			// Request instances!
			RunInstancesResult result = ec2client.runInstances(request);

			for (Instance i : result.getReservation().getInstances()) {
				instanceIDs.add(i.getInstanceId());
			}

		}

		return instanceIDs;
	}

	/**
	 * Checks whether there is a floating instance with the specific type belonging to the owner. If there is a floating
	 * instance, it will be
	 * changed into a cloud instance and returned.
	 * 
	 * @param owner
	 *        the owner of the floating instances
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 * @param type
	 *        the type of the floating instance, which is checked
	 * @return a cloud instance
	 * @throws InstanceException
	 *         something wrong happens to the global configuration
	 */
	private LinkedList<CloudInstance> anyFloatingInstanceAvailable(String owner, String awsAccessId,
			String awsSecretKey, InstanceType type, int count) throws InstanceException {

		LOG.info("Check for floating instance of type" + type.getIdentifier() + " requested count: " + count + ".");

		for (InstanceConnectionInfo i : this.floatingInstances.keySet()) {
			LOG.info("Floating instance available: " + i.getAddress() + " " + i.getHostName() + " hash: "
				+ i.hashCode());
		}

		final LinkedList<CloudInstance> floatinginstances = new LinkedList<CloudInstance>();

		synchronized (this.floatingInstances) {

			if (this.floatingInstances.size() == 0) {
				// There is no floating instance known to the system
				return floatinginstances; // (empty)
			}

			AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);

			DescribeInstancesRequest request = new DescribeInstancesRequest();
			DescribeInstancesResult result = ec2client.describeInstances(request);

			// Iterate over all Instances
			for (Reservation r : result.getReservations()) {
				for (Instance t : r.getInstances()) {

					if (!t.getInstanceType().equals(type.getIdentifier())) {
						continue;
					}

					InetAddress inetAddress = null;
					try {
						inetAddress = InetAddress.getByName(t.getPrivateIpAddress());
					} catch (UnknownHostException e) {
						LOG.error("Cannot resolve " + t.getPrivateDnsName() + " into an IP address: "
							+ StringUtils.stringifyException(e));
						continue;
					}

					// Check if instance entry is a floating instance

					for (Entry<InstanceConnectionInfo, FloatingInstance> e : this.floatingInstances.entrySet()) {
						if (e.getKey().getAddress().equals(inetAddress)) {
							LOG.info("Suitable floating instance found.");

							final FloatingInstance floatingInstance = this.floatingInstances.remove(e.getKey());
							this.floatingInstanceIDs.remove(floatingInstance.getInstanceID());

							// TODO JobToInstanceMapping!

							floatinginstances.add(convertIntoCloudInstance(t,
								floatingInstance.getInstanceConnectionInfo(), owner));

							// If we already have enough floating instances found: return!
							if (floatinginstances.size() >= count) {
								return floatinginstances;
							}
						}

					}

					/*
					 * if (this.floatingInstances.containsKey(inetAddress)) {
					 * LOG.info("FOUND INSTANCE!!!!");
					 * final FloatingInstance floatingInstance = this.floatingInstances.remove(inetAddress);
					 * this.floatingInstanceIDs.remove(floatingInstance.getInstanceID());
					 * floatinginstances.add(convertIntoCloudInstance(t, floatingInstance.getInstanceConnectionInfo(),
					 * owner));
					 * // If we already have enough floating instances found: return!
					 * if (floatinginstances.size() >= count) {
					 * return floatinginstances;
					 * }
					 * }
					 */

				}
			}
		}

		return floatinginstances;

	}

	/**
	 * The cloud manager checks whether there are floating instances which expire and terminates them.
	 */
	public void run() {

		final long currentTime = System.currentTimeMillis();

		synchronized (this.floatingInstances) {

			synchronized (this.floatingInstanceIDs) {

				final Set<Map.Entry<InstanceConnectionInfo, FloatingInstance>> entries = this.floatingInstances
					.entrySet();
				final Iterator<Map.Entry<InstanceConnectionInfo, FloatingInstance>> it = entries.iterator();

				while (it.hasNext()) {

					final Map.Entry<InstanceConnectionInfo, FloatingInstance> entry = it.next();

					if (currentTime - entry.getValue().getAllocationTime() > entry.getValue().getRemainingTime()) {

						try {
							destroyCloudInstance(this.floatingInstanceIDs.get(entry.getValue().getInstanceID()), entry
								.getValue().getInstanceID());
						} catch (InstanceException e) {
							e.printStackTrace();
						}

						this.floatingInstances.remove(entry.getKey());
						this.floatingInstanceIDs.remove(entry.getValue().getInstanceID());

						LOG.info("Instance " + entry.getValue().getInstanceID() + " terminated");
					}

				}

			}

		}

	}

	/**
	 * Returns the default instance type.
	 * 
	 * @return the default instance type
	 */
	@Override
	public InstanceType getDefaultInstanceType() {

		final String instanceIdentifier = GlobalConfiguration.getString("cloudmgr.instancetype.defaultInstance", null);
		if (instanceIdentifier == null) {
			return null;
		}

		return getInstanceTypeByName(instanceIdentifier);
	}

	/**
	 * Returns the instance type by the given name.
	 * 
	 * @param instanceTypeName
	 *        the name of the instance type
	 * @return the instance type by the given name
	 */
	@Override
	public synchronized InstanceType getInstanceTypeByName(String instanceTypeName) {

		for (int i = 0; i < this.availableInstanceTypes.length; i++) {
			if (this.availableInstanceTypes[i].getIdentifier().equals(instanceTypeName)) {
				return this.availableInstanceTypes[i];
			}
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		// Stop the timer task
		LOG.debug("Stopping timer task");
		this.cancel();
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
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {
		// TODO Auto-generated method stub
		return null;
	}

}
