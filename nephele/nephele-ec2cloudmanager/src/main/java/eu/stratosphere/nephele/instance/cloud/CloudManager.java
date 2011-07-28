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
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.HardwareDescription;
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
public final class CloudManager extends TimerTask implements InstanceManager {

	/** The log for the cloud manager. */
	private static final Log LOG = LogFactory.getLog(CloudManager.class);

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

	/** TMs that send HeartBeats but do not belong to any job are kept in this set. */
	private final HashSet<InstanceConnectionInfo> orphanedTMs = new HashSet<InstanceConnectionInfo>();

	/** The array of all available instance types in the cloud. */
	private final InstanceType[] availableInstanceTypes;

	/** Mapping jobs to instances. */
	private final Map<JobID, JobToInstancesMapping> jobToInstancesAssignmentMap = new HashMap<JobID, JobToInstancesMapping>();

	/** All reserved instances. Mapping instance IDs to job IDs. */
	private final Map<String, JobID> reservedInstancesToJobMapping = new HashMap<String, JobID>();

	/** All floating instances. Mapping task managers' addresses to floating instances. */
	private final Map<InstanceConnectionInfo, FloatingInstance> floatingInstances = new HashMap<InstanceConnectionInfo, FloatingInstance>();

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
	public InstanceType getSuitableInstanceType(final int minNumComputeUnits, final int minNumCPUCores,
			final int minMemorySize, final int minDiskCapacity, final int maxPricePerHour) {

		// instances are sorted by price, so the first instance that meets the demands is the suitable instance
		for (final InstanceType i : availableInstanceTypes) {
			if ((i.getNumberOfComputeUnits() >= minNumComputeUnits) && (i.getNumberOfCores() >= minNumCPUCores)
				&& (i.getMemorySize() >= minMemorySize) && (i.getDiskCapacity() >= minDiskCapacity)
				&& (i.getPricePerHour() <= maxPricePerHour)) {
				return i;
			}
		}

		return null;
	}

	/**
	 * A cloud instance is released and changed into a floating instance, waiting for a new job.
	 * 
	 * @param jobID
	 *        the ID of the finished job
	 * @param conf
	 *        the configuration of the finished job
	 * @param instance
	 *        the cloud instance which is to be released
	 */
	@Override
	public synchronized void releaseAllocatedResource(final JobID jobID, final Configuration conf,
			final AllocatedResource allocatedResource) {

		final AbstractInstance instance = allocatedResource.getInstance();

		JobToInstancesMapping jobToInstanceMapping = null;

		synchronized (this.jobToInstancesAssignmentMap) {
			jobToInstanceMapping = this.jobToInstancesAssignmentMap.get(jobID);

			if (jobToInstanceMapping == null) {
				LOG.error("No mapping for job " + jobID);
				return;
			}
		}

		if (jobToInstanceMapping.getAssignedInstances().contains(instance)) {

			// Unassigned Instance
			jobToInstanceMapping.unassignInstanceFromJob((CloudInstance) instance);

			// Make it a floating Instance
			this.floatingInstances.put(instance.getInstanceConnectionInfo(),
				((CloudInstance) instance).asFloatingInstance());

			LOG.info("Convert " + ((CloudInstance) instance).getInstanceID()
				+ " from allocated instance to floating instance");

		} else {
			LOG.error("Job " + jobID + " contains no such instance");
			return;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void reportHeartBeat(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) {

		// Check if this TM is orphaned
		if (this.orphanedTMs.contains(instanceConnectionInfo)) {
			LOG.debug("Received HeartBeat from orphaned TM " + instanceConnectionInfo);
			return;
		}

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
			jobID = this.reservedInstancesToJobMapping.get(instance.getInstanceID());
			if (jobID == null) {
				LOG.error("Cannot find job ID to instance ID " + instance.getInstanceID());
				return;
			}

			this.reservedInstancesToJobMapping.remove(instance.getInstanceID());
			instance.updateLastReceivedHeartBeat();

			final JobToInstancesMapping mapping = this.jobToInstancesAssignmentMap.get(jobID);
			if (mapping == null) {
				LOG.error("Cannot find mapping for job ID " + jobID);
				return;
			}
			mapping.assignInstanceToJob(instance);

			// Trigger notification that instance is available (outside synchronized section)
			final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>(1);
			allocatedResources.add(instance.asAllocatedResource());
			this.instanceListener.resourcesAllocated(jobID, allocatedResources);

			return;
		}

		// This TM seems to be unknown to the JobManager.. blacklist
		LOG.info("Received HeartBeat from unknown TM. Put into orphaned TM set. Address is: " + instanceConnectionInfo);
		this.orphanedTMs.add(instanceConnectionInfo);

	}

	/**
	 * Returns a cloud instance if this instance is assigned to a job.
	 * 
	 * @param instanceConnectionInfo
	 *        the connection information object identifying the instance
	 * @return a cloud instance
	 */
	private CloudInstance isAssignedInstance(final InstanceConnectionInfo instanceConnectionInfo) {

		synchronized (this.jobToInstancesAssignmentMap) {

			final Iterator<JobID> it = this.jobToInstancesAssignmentMap.keySet().iterator();

			while (it.hasNext()) {

				final JobID jobID = it.next();
				final JobToInstancesMapping m = this.jobToInstancesAssignmentMap.get(jobID);
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
	private CloudInstance isReservedInstance(final InstanceConnectionInfo instanceConnectionInfo)
			throws InstanceException {

		if (instanceConnectionInfo == null) {
			LOG.warn("Supplied instance connection info is null");
			return null;
		}

		synchronized (this.reservedInstancesToJobMapping) {

			if (this.reservedInstancesToJobMapping.size() == 0) {
				return null;
			}

			// Collect Jobs that have reserved instances
			final HashSet<JobID> jobsWithReservedInstances = new HashSet<JobID>();

			for (JobID id : this.reservedInstancesToJobMapping.values()) {
				jobsWithReservedInstances.add(id);
			}

			// Now we call the webservice for each job..

			for (JobID id : jobsWithReservedInstances) {

				JobToInstancesMapping mapping = null;

				synchronized (this.jobToInstancesAssignmentMap) {
					mapping = this.jobToInstancesAssignmentMap.get(id);
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
							return convertIntoCloudInstance(t, instanceConnectionInfo, mapping.getAwsAccessId(),
								mapping.getAwsSecretKey());
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
	 * @return a cloud instance
	 */
	private CloudInstance convertIntoCloudInstance(final com.amazonaws.services.ec2.model.Instance instance,
			final InstanceConnectionInfo instanceConnectionInfo, final String awsAccessKey, final String awsSecretKey) {

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

		final CloudInstance cloudInstance = new CloudInstance(instance.getInstanceId(), type, instanceConnectionInfo,
			instance.getLaunchTime().getTime(), this.networkTopology.getRootNode(), this.networkTopology, null,
			awsAccessKey, awsSecretKey);

		// TODO: Define hardware descriptions for cloud instance types

		return cloudInstance;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestInstance(final JobID jobID, Configuration conf, final InstanceRequestMap instanceRequestMap,
			final List<String> splitAffinityList) throws InstanceException {

		if (conf == null) {
			throw new IllegalArgumentException("No job configuration provided, unable to acquire credentials");
		}

		// First check, if all required configuration entries are available

		final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		LOG.info("found AWS access ID from Job Conf: " + awsAccessId);
		if (awsAccessId == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS access ID");
		}
		final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);
		if (awsSecretKey == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS secret key");
		}

		// First we check, if there are any orphaned TMs that are accessible with the provided configuration
		checkAndConvertOrphanedInstances(conf);

		// Check if there already exist a mapping for this job
		JobToInstancesMapping jobToInstanceMapping = null;
		synchronized (this.jobToInstancesAssignmentMap) {
			jobToInstanceMapping = this.jobToInstancesAssignmentMap.get(jobID);

			// Create new mapping if it does not yet exist
			if (jobToInstanceMapping == null) {
				LOG.debug("Creating new mapping for job " + jobID);
				jobToInstanceMapping = new JobToInstancesMapping(awsAccessId, awsSecretKey);
				this.jobToInstancesAssignmentMap.put(jobID, jobToInstanceMapping);
			}
		}

		// Our bill with all instances that we will provide...
		final LinkedList<FloatingInstance> floatingInstances = new LinkedList<FloatingInstance>();
		final LinkedList<String> requestedInstances = new LinkedList<String>();

		// We iterate over the maximum of requested Instances...
		final Iterator<Map.Entry<InstanceType, Integer>> it = instanceRequestMap.getMaximumIterator();

		while (it.hasNext()) {

			final Map.Entry<InstanceType, Integer> e = it.next();

			// This is our actual type...
			final InstanceType actualtype = e.getKey();
			final int maxcount = e.getValue();
			final int mincount = instanceRequestMap.getMinimumNumberOfInstances(actualtype);

			// And this is the list of instances we will have...
			LinkedList<FloatingInstance> actualFloatingInstances = new LinkedList<FloatingInstance>();
			LinkedList<String> actualRequestedInstances = new LinkedList<String>();

			// Check if floating instances available...
			actualFloatingInstances = anyFloatingInstancesAvailable(awsAccessId, awsSecretKey, actualtype, maxcount);

			// Do we need more instances?
			if (actualFloatingInstances.size() < maxcount) {
				int minimumrequestcount = Math.max(mincount - actualFloatingInstances.size(), 1);
				int maximumrequestcount = maxcount - actualFloatingInstances.size();

				actualRequestedInstances = allocateCloudInstance(conf, actualtype, minimumrequestcount,
					maximumrequestcount);
			}

			// Add provided Instances to overall bill...
			floatingInstances.addAll(actualFloatingInstances);
			requestedInstances.addAll(actualRequestedInstances);

			// Are we outer limits?
			if (actualRequestedInstances.size() + actualFloatingInstances.size() < mincount) {
				LOG.error("Requested: " + mincount + " to " + maxcount + " instanes of type "
					+ actualtype.getIdentifier() + ". Could only provide "
					+ (actualRequestedInstances.size() + actualFloatingInstances.size()) + ".");

				// something went wrong.. give the floating instances back!
				synchronized (this.floatingInstances) {
					for (FloatingInstance i : floatingInstances) {
						this.floatingInstances.put(i.getInstanceConnectionInfo(), i);
					}
				}
				throw new InstanceException("Could not allocate enough cloud instances");
			} // End outer limits

		} // End iterating over instance types..

		// If we reached this point, everything went well
		final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();

		// Convert and allocate Floating Instances...
		for (final FloatingInstance fi : floatingInstances) {
			final CloudInstance ci = fi.asCloudInstance();
			jobToInstanceMapping.assignInstanceToJob(ci);
			allocatedResources.add(ci.asAllocatedResource());
		}

		// Finally, inform the scheduler about the instances which have been floating before
		final CloudInstanceNotifier notifier = new CloudInstanceNotifier(this.instanceListener, jobID,
			allocatedResources);
		notifier.start();

		// Add reserved Instances to Job Mapping...
		for (final String i : requestedInstances) {
			this.reservedInstancesToJobMapping.put(i, jobID);
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
	 *         List containing the instance IDs of the allocated instances.
	 */
	private LinkedList<String> allocateCloudInstance(final Configuration conf, final InstanceType type,
			final int mincount, final int maxcount) {

		final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);

		String imageID = conf.getString("job.ec2.image.id", null);
		LOG.info("EC2 Image ID from job conf: " + imageID);
		if (imageID == null) {

			imageID = GlobalConfiguration.getString("ec2.image.id", null);
			if (imageID == null) {
				LOG.error("Unable to allocate instance: Image ID is unknown");
				return null;
			}
		}

		final String jobManagerIPAddress = GlobalConfiguration.getString("jobmanager.rpc.address", null);
		if (jobManagerIPAddress == null) {
			LOG.error("JobManager IP address is not set (jobmanager.rpc.address)");
			return null;
		}
		final String sshKeyPair = conf.getString("job.cloud.sshkeypair", null);

		final AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);
		final LinkedList<String> instanceIDs = new LinkedList<String>();

		// Iterate over instance types..

		final RunInstancesRequest request = new RunInstancesRequest(imageID, mincount, maxcount);
		request.setInstanceType(type.getIdentifier());

		// TODO: Make this configurable!
		final BlockDeviceMapping bdm = new BlockDeviceMapping();
		bdm.setVirtualName("ephemeral0");
		bdm.setDeviceName("/dev/sdb1");

		if (sshKeyPair != null) {
			request.setKeyName(sshKeyPair);
		}

		final LinkedList<BlockDeviceMapping> bdmlist = new LinkedList<BlockDeviceMapping>();
		bdmlist.add(bdm);
		request.setBlockDeviceMappings(bdmlist);

		// Setting User-Data parameters
		request.setUserData(EC2Utilities.createTaskManagerUserData(jobManagerIPAddress));

		// Request instances!
		final RunInstancesResult result = ec2client.runInstances(request);

		for (Instance i : result.getReservation().getInstances()) {
			instanceIDs.add(i.getInstanceId());
		}

		return instanceIDs;
	}

	/**
	 * Checks, if there are any orphaned Instances listed that are accessible via the provided configuration.
	 * If so, orphaned Instances will be converted to floating instances related to the given configuration.
	 * 
	 * @param conf
	 *        The configuration provided upon instances request
	 */
	private void checkAndConvertOrphanedInstances(final Configuration conf) {
		if (this.orphanedTMs.size() == 0) {
			return;
		}

		final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);

		LOG.debug("Checking orphaned Instances... " + this.orphanedTMs.size() + " orphaned instances listed.");
		final AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);

		final DescribeInstancesRequest request = new DescribeInstancesRequest();
		final DescribeInstancesResult result = ec2client.describeInstances(request);

		// Iterate over all Instances
		for (final Reservation r : result.getReservations()) {
			for (final Instance t : r.getInstances()) {

				InstanceType type = null;

				// Find out type of the instance
				for (int i = 0; i < this.availableInstanceTypes.length; i++) {

					if (this.availableInstanceTypes[i].getIdentifier().equals(t.getInstanceType())) {
						type = this.availableInstanceTypes[i];
						break;
					}
				}

				InetAddress inetAddress = null;
				try {
					inetAddress = InetAddress.getByName(t.getPrivateIpAddress());
				} catch (UnknownHostException e) {
					LOG.error("Cannot resolve " + t.getPrivateDnsName() + " into an IP address: "
						+ StringUtils.stringifyException(e));
					continue;
				}

				final Iterator<InstanceConnectionInfo> it = this.orphanedTMs.iterator();

				while (it.hasNext()) {
					final InstanceConnectionInfo oi = it.next();
					if (oi.getAddress().equals(inetAddress) && type != null) {
						LOG.info("Orphaned Instance " + oi + " converted into floating instance.");

						// We have found the corresponding orphaned TM.. take the poor lamb back to its nest.
						FloatingInstance floatinginstance = new FloatingInstance(t.getInstanceId(), oi, t
							.getLaunchTime().getTime(), type, awsAccessId, awsSecretKey);

						this.floatingInstances.put(oi, floatinginstance);
						it.remove();
						break;
					}
				}

			}
		}
	}

	/**
	 * Checks whether there is a floating instance with the specific type. If there are instances available,
	 * they will be removed from the list and returned...
	 * 
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 * @param type
	 *        the type of the floating instance, which is checked
	 * @return a list of suitable floating instances.
	 * @throws InstanceException
	 *         something wrong happens to the global configuration
	 */
	private LinkedList<FloatingInstance> anyFloatingInstancesAvailable(final String awsAccessId,
			final String awsSecretKey, final InstanceType type, final int count) throws InstanceException {

		LOG.info("Check for floating instance of type" + type.getIdentifier() + " requested count: " + count + ".");

		final LinkedList<FloatingInstance> foundfloatinginstances = new LinkedList<FloatingInstance>();

		synchronized (this.floatingInstances) {

			final Iterator<Map.Entry<InstanceConnectionInfo, FloatingInstance>> it = this.floatingInstances.entrySet()
				.iterator();
			while (it.hasNext()) {
				final FloatingInstance i = it.next().getValue();
				// Check if we own this instance
				if (i.isFromThisOwner(awsAccessId, awsSecretKey)) {
					// Yes it is.. now check if it is of the desired type..
					if (i.getType().equals(type)) {
						// Found..
						it.remove();
						foundfloatinginstances.add(i);
					}
				}
			}

		}

		LOG.info("Found " + foundfloatinginstances.size() + " suitable floating instances.");

		return foundfloatinginstances;

	}

	/**
	 * The cloud manager checks whether there are floating instances which expire and terminates them.
	 */
	@Override
	public void run() {

		synchronized (this.floatingInstances) {
			final Iterator<Map.Entry<InstanceConnectionInfo, FloatingInstance>> it = this.floatingInstances.entrySet()
				.iterator();

			while (it.hasNext()) {
				final Map.Entry<InstanceConnectionInfo, FloatingInstance> entry = it.next();

				// Call lifecycle method for each floating instance. If true, remove from floatinginstances list.
				if (entry.getValue().checkIfLifeCycleEnded()) {
					it.remove();
					LOG.info("Floating Instance " + entry.getValue().getInstanceID()
						+ " ended its lifecycle. Terminated");
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
		Map<InstanceType, InstanceTypeDescription> availableinstances = new HashMap<InstanceType, InstanceTypeDescription>();

		for (InstanceType t : this.availableInstanceTypes) {
			availableinstances.put(t, InstanceTypeDescriptionFactory.construct(t, null, -1));
		}

		return availableinstances;
	}

}
