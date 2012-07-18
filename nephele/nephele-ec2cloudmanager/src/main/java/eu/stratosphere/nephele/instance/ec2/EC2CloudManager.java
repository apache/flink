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

package eu.stratosphere.nephele.instance.ec2;

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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.BlockDeviceMapping;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Placement;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
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
import eu.stratosphere.nephele.util.StringUtils;

/**
 * The task of cloud manager is managing instances (virtual machines) in the cloud. The cloud manager can allocate and
 * release instances. There are three types of instances: reserved instance, cloud instance and floating instance. The
 * reserved instance is reserved for a specific job. The cloud instance is an instance in the running state. The
 * floating instance is idle and not reserved for any job. The type of an instance in the cloud may vary among these
 * three states. One instance belongs to only one user, i.e. the user can use only his own instances in the cloud. The
 * user pays fee for allocated instances hourly. The cloud manager checks the floating instances every 60 seconds in
 * order to terminate the floating instances which expire.
 */
public final class EC2CloudManager extends TimerTask implements InstanceManager {

	/**
	 * The log for the EC2 cloud manager.
	 **/
	private static final Log LOG = LogFactory.getLog(EC2CloudManager.class);

	/**
	 * The key to access the lease period from the configuration.
	 */
	private static String LEASE_PERIOD_KEY = "instancemanager.ec2.leaseperiod";

	/**
	 * The default lease period in milliseconds for instances on Amazon EC2.
	 */
	static final long DEFAULT_LEASE_PERIOD = 60 * 60 * 1000; // 1 hour in ms.

	/**
	 * The global (default) AMI to be used for TM instances
	 */
	static final String AWS_AMI_KEY_GLOBAL = "instancemanager.ec2.defaultami";

	/**
	 * The configuration key to access the AWS access ID of a job.
	 */
	static final String AWS_ACCESS_ID_KEY_GLOBAL = "instancemanager.ec2.defaultawsaccessid";

	/**
	 * The configuration key to access the AWS secret key of a job.
	 */
	static final String AWS_SECRET_KEY_KEY_GLOBAL = "instancemanager.ec2.defaultawssecretkey";

	/**
	 * The configuration key to access the AWS access ID of a job.
	 */
	static final String AWS_ACCESS_ID_KEY = "job.ec2.awsaccessid";

	/**
	 * The configuration key to access the AWS secret key of a job.
	 */
	static final String AWS_SECRET_KEY_KEY = "job.ec2.awssecretkey";

	/**
	 * The configuration key to access the AMI to run the task managers on.
	 */
	static final String AWS_AMI_KEY = "job.ec2.ami";

	/**
	 * The SSH Keypair to be installed on TMs (optional)
	 */
	static final String AWS_SSH_KEYPAIR = "job.ec2.sshkeypair";

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

	/**
	 * Instances that send heart beats but do not belong to any job are kept in this map.
	 **/
	private final Map<InstanceConnectionInfo, HardwareDescription> orphanedInstances = new HashMap<InstanceConnectionInfo, HardwareDescription>();

	/**
	 * The array of all available instance types in the cloud.
	 **/
	private final InstanceType[] availableInstanceTypes;

	/**
	 * The lease period for instances on Amazon EC2 in milliseconds, potentially configured by the user.
	 */
	private final long leasePeriod;

	/**
	 * The default instance type.
	 */
	private InstanceType defaultInstanceType = null;

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
	 * The network topology for each job.
	 */
	private final Map<JobID, NetworkTopology> networkTopologies = new HashMap<JobID, NetworkTopology>();

	/**
	 * The preferred availability for instances on EC2.
	 */
	private final String availabilityZone;

	/**
	 * Creates the cloud manager.
	 */
	public EC2CloudManager() {

		// Load the instance type this cloud can offer
		this.availableInstanceTypes = populateInstanceTypeArray();

		// Calculate lease period
		long lp = GlobalConfiguration.getInteger(LEASE_PERIOD_KEY, -1);
		if (lp > 0) {
			LOG.info("Found user-defined lease period of " + lp + " minutes for instances");
			lp = lp * 1000L * 60L; // Convert to milliseconds
		} else {
			lp = DEFAULT_LEASE_PERIOD;
		}
		this.leasePeriod = lp;

		this.cleanUpInterval = (long) GlobalConfiguration.getInteger("instancemanager.ec2.cleanupinterval",
			DEFAULTCLEANUPINTERVAL);

		if ((this.cleanUpInterval % BASEINTERVAL) != 0) {
			LOG.warn("Invalid clean up interval. Reverting to " + DEFAULTCLEANUPINTERVAL);
			this.cleanUpInterval = DEFAULTCLEANUPINTERVAL;
		}

		this.availabilityZone = GlobalConfiguration.getString("instancemanager.ec2.availabilityzone", null);
		if (this.availabilityZone == null) {
			LOG.info("No preferred availability zone configured");
		} else {
			LOG.info("Found " + this.availabilityZone + " as preferred availability zone in configuration");
		}

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

		int count = 1;
		while (true) {

			final String key = "instancemanager.ec2.type." + count;
			final String type = GlobalConfiguration.getString(key, null);
			if (type == null) {
				break;
			}

			final InstanceType instanceType = InstanceTypeFactory.constructFromDescription(type);
			LOG.info("Found instance type " + count + ": " + instanceType);

			instanceTypes.add(instanceType);
			++count;
		}

		if (instanceTypes.isEmpty()) {
			LOG.error("No instance types found in configuration");
			return new InstanceType[0];
		}

		int defaultIndex = GlobalConfiguration.getInteger("instancemanager.ec2.defaulttype", -1);
		if (defaultIndex < 1 || defaultIndex >= (instanceTypes.size() + 1)) {
			LOG.warn("Invalid index to default instance " + defaultIndex);
			defaultIndex = 1;
		}

		this.defaultInstanceType = instanceTypes.get(defaultIndex - 1);
		LOG.info("Default instance type is " + this.defaultInstanceType);

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

		// Destroy proxies of that instance
		instance.destroyProxies();

		if (jobToInstanceMapping.unassignInstanceFromJob(instance)) {

			// Make it a floating instance
			this.floatingInstances.put(instance.getInstanceConnectionInfo(),
				((EC2CloudInstance) instance).asFloatingInstance());

			// TODO: Clean up job to instance mapping and network topology

			LOG.info("Converting " + ((EC2CloudInstance) instance).getInstanceID()
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
		if (this.orphanedInstances.containsKey(instanceConnectionInfo)) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Received heart beat from orphaned instance " + instanceConnectionInfo);
			}
			return;
		}

		// Check if heart beat belongs to a floating instance
		if (this.floatingInstances.containsKey(instanceConnectionInfo)) {
			final FloatingInstance floatingInstance = this.floatingInstances.get(instanceConnectionInfo);
			floatingInstance.updateLastReceivedHeartBeat();
			return;
		}

		// Check if heart beat belongs to an assigned instance
		EC2CloudInstance instance = isAssignedInstance(instanceConnectionInfo);
		if (instance != null) {
			// If it's an assigned instance, just update the heart beat time stamp and leave
			instance.updateLastReceivedHeartBeat();
			return;
		}

		// Check if heart beat belongs to a reserved instance
		try {
			instance = isReservedInstance(instanceConnectionInfo, hardwareDescription);
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
			final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();
			allocatedResources.add(instance.asAllocatedResource());
			this.instanceListener.resourcesAllocated(jobID, allocatedResources);

			return;
		}

		// This TM seems to be unknown to the JobManager.. blacklist
		LOG.info("Received heart beat from unknown instance " + instanceConnectionInfo
			+ ", converting it into orphaned instance");
		this.orphanedInstances.put(instanceConnectionInfo, hardwareDescription);

	}

	/**
	 * Returns a cloud instance if this instance is assigned to a job.
	 * 
	 * @param instanceConnectionInfo
	 *        the connection information object identifying the instance
	 * @return a cloud instance
	 */
	private EC2CloudInstance isAssignedInstance(final InstanceConnectionInfo instanceConnectionInfo) {

		synchronized (this.jobToInstancesAssignmentMap) {

			final Iterator<JobID> it = this.jobToInstancesAssignmentMap.keySet().iterator();

			while (it.hasNext()) {

				final JobID jobID = it.next();
				final JobToInstancesMapping m = this.jobToInstancesAssignmentMap.get(jobID);
				final EC2CloudInstance instance = m.getInstanceByConnectionInfo(instanceConnectionInfo);
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
	 * @param hardwareDescription
	 *        the actual hardware description of the instance
	 * @return a cloud instance
	 * @throws InstanceException
	 *         something wrong happens to the global configuration
	 */
	private EC2CloudInstance isReservedInstance(final InstanceConnectionInfo instanceConnectionInfo,
			final HardwareDescription hardwareDescription) throws InstanceException {

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

			// Now we call the web service for each job..

			for (final JobID jobID : jobsWithReservedInstances) {

				JobToInstancesMapping mapping = null;

				synchronized (this.jobToInstancesAssignmentMap) {
					mapping = this.jobToInstancesAssignmentMap.get(jobID);
				}

				if (mapping == null) {
					LOG.error("Unknown mapping for job ID " + jobID);
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

							NetworkTopology networkTopology;
							synchronized (this.networkTopologies) {
								networkTopology = this.networkTopologies.get(jobID);
							}

							if (networkTopology == null) {
								throw new InstanceException("Cannot find network topology for job " + jobID);
							}

							return convertIntoCloudInstance(t, instanceConnectionInfo, mapping.getAwsAccessId(),
								mapping.getAwsSecretKey(), networkTopology.getRootNode(), hardwareDescription);
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
	private EC2CloudInstance convertIntoCloudInstance(final Instance instance,
			final InstanceConnectionInfo instanceConnectionInfo, final String awsAccessKey, final String awsSecretKey,
			final NetworkNode parentNode, final HardwareDescription hardwareDescription) {

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

		final EC2CloudInstance cloudInstance = new EC2CloudInstance(instance.getInstanceId(), type,
			instanceConnectionInfo, instance.getLaunchTime().getTime(), this.leasePeriod, parentNode,
			parentNode.getNetworkTopology(), hardwareDescription, awsAccessKey, awsSecretKey);

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

		String awsAccessId = conf.getString(AWS_ACCESS_ID_KEY, null);
		if (awsAccessId == null) {
			LOG.info("No Job-specific AWS access ID found. Trying to read from global configuration.");
			awsAccessId = GlobalConfiguration.getString(AWS_ACCESS_ID_KEY_GLOBAL, null);
			if (awsAccessId == null) {
				throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS access ID");
			}
		}

		String awsSecretKey = conf.getString(AWS_SECRET_KEY_KEY, null);
		if (awsSecretKey == null) {
			LOG.info("No Job-specific AWS secret key found. Trying to read from global configuration.");
			awsSecretKey = GlobalConfiguration.getString(AWS_SECRET_KEY_KEY_GLOBAL, null);
			if (awsSecretKey == null) {
				throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS secret key");
			}
		}

		if (conf.getString(AWS_AMI_KEY, null) == null) {
			LOG.info("No Job-specific AMI found. Trying to use default AMI from global configuration.");
			if (GlobalConfiguration.getString(AWS_AMI_KEY_GLOBAL, null) == null) {
				throw new InstanceException(
					"Unable to allocate cloud instance: no AWS AMI key found in global or job configuration.");
			}
		}

		// First we check, if there are any orphaned instances that are accessible with the provided configuration
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

		// Check if there already exists a network topology for this job
		NetworkTopology networkTopology = null;
		synchronized (this.networkTopologies) {
			networkTopology = this.networkTopologies.get(jobID);
			if (networkTopology == null) {
				networkTopology = NetworkTopology.createEmptyTopology();
				this.networkTopologies.put(jobID, networkTopology);
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
			final int mincount = maxcount;
			LOG.info("Requesting " + maxcount + " instances of type " + actualtype + " for job " + jobID);

			// And this is the list of instances we will have...
			LinkedList<FloatingInstance> actualFloatingInstances = null;
			LinkedList<String> actualRequestedInstances = null;

			// Check if floating instances available...
			actualFloatingInstances = anyFloatingInstancesAvailable(awsAccessId, awsSecretKey, actualtype, maxcount);

			// Do we need more instances?
			if (actualFloatingInstances.size() < maxcount) {
				int minimumrequestcount = Math.max(mincount - actualFloatingInstances.size(), 1);
				int maximumrequestcount = maxcount - actualFloatingInstances.size();

				actualRequestedInstances = allocateCloudInstance(conf, actualtype, minimumrequestcount,
					maximumrequestcount);
			} else {
				actualRequestedInstances = new LinkedList<String>();
			}

			// Add provided Instances to overall bill...
			floatingInstances.addAll(actualFloatingInstances);
			requestedInstances.addAll(actualRequestedInstances);

			// Are we outer limits?
			if (actualRequestedInstances.size() + actualFloatingInstances.size() < mincount) {
				LOG.error("Requested: " + mincount + " to " + maxcount + " instances of type "
					+ actualtype.getIdentifier() + ", but could only provide "
					+ (actualRequestedInstances.size() + actualFloatingInstances.size()) + ".");

				// something went wrong.. give the floating instances back!
				synchronized (this.floatingInstances) {
					for (FloatingInstance i : floatingInstances) {
						this.floatingInstances.put(i.getInstanceConnectionInfo(), i);
					}
				}
				throw new InstanceException("Could not allocate enough cloud instances. See logs for details.");
			} // End outer limits

		} // End iterating over instance types..

		// Convert and allocate Floating Instances...
		final List<AllocatedResource> allocatedResources = new ArrayList<AllocatedResource>();

		for (final FloatingInstance fi : floatingInstances) {
			final EC2CloudInstance ci = fi.asCloudInstance(networkTopology.getRootNode());
			jobToInstanceMapping.assignInstanceToJob(ci);
			allocatedResources.add(ci.asAllocatedResource());
		}

		// Finally, inform the scheduler about the instances which have been floating before
		if (!allocatedResources.isEmpty()) {
			final EC2CloudInstanceNotifier notifier = new EC2CloudInstanceNotifier(this.instanceListener, jobID,
				allocatedResources);
			notifier.start();
		}

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

		String awsAccessId = conf.getString(AWS_ACCESS_ID_KEY, null);
		if(awsAccessId == null){
			awsAccessId = GlobalConfiguration.getString(AWS_ACCESS_ID_KEY_GLOBAL, null);
		}
		
		String awsSecretKey = conf.getString(AWS_SECRET_KEY_KEY, null);
		if(awsSecretKey == null){
			awsSecretKey = GlobalConfiguration.getString(AWS_SECRET_KEY_KEY_GLOBAL, null);
		}
		
		String imageID = conf.getString(AWS_AMI_KEY, null);

		if (imageID == null) {
			imageID = GlobalConfiguration.getString(AWS_AMI_KEY_GLOBAL, null);
			LOG.info("Read Amazon Machine Image from global configuration: " + imageID);
		} else {
			LOG.info("Read Amazon Machine Image from job configuration: " + imageID);
		}

		final String jobManagerIPAddress = GlobalConfiguration.getString("jobmanager.rpc.address", null);
		if (jobManagerIPAddress == null) {
			LOG.error("JobManager IP address is not set (jobmanager.rpc.address)");
			return null;
		}
		final String sshKeyPair = conf.getString(AWS_SSH_KEYPAIR, null);

		final AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);
		final LinkedList<String> instanceIDs = new LinkedList<String>();

		// Iterate over instance types..

		final RunInstancesRequest request = new RunInstancesRequest(imageID, mincount, maxcount);
		request.setInstanceType(type.getIdentifier());

		// Set availability zone if configured
		String av = null;
		if (this.availabilityZone != null) {
			av = this.availabilityZone;
		}
		final String jobAV = conf.getString("job.ec2.availabilityzone", null);
		if (jobAV != null) {
			LOG.info("Found " + jobAV + " as job-specific preference for availability zone");
			av = jobAV;
		}

		if (av != null) {
			request.setPlacement(new Placement(av));
		}

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
		try {
			final RunInstancesResult result = ec2client.runInstances(request);

			for (Instance i : result.getReservation().getInstances()) {
				instanceIDs.add(i.getInstanceId());
			}
		} catch (AmazonClientException e) {
			// Only log the error here
			LOG.error(StringUtils.stringifyException(e));
		}

		return instanceIDs;
	}

	/**
	 * Checks, if there are any orphaned Instances listed that are accessible via the provided configuration.
	 * If so, orphaned Instances will be converted to floating instances related to the given configuration.
	 * 
	 * @param conf
	 *        The configuration provided upon instances request
	 * @throws InstanceException
	 *         thrown if an error occurs while communicating with Amazon EC2
	 */
	private void checkAndConvertOrphanedInstances(final Configuration conf) throws InstanceException {

		if (this.orphanedInstances.size() == 0) {
			return;
		}

		String awsAccessId = conf.getString(AWS_ACCESS_ID_KEY, null);
		if(awsAccessId == null){
			awsAccessId = GlobalConfiguration.getString(AWS_ACCESS_ID_KEY_GLOBAL, null);
		}
		String awsSecretKey = conf.getString(AWS_SECRET_KEY_KEY, null);
		if(awsSecretKey == null){
			awsSecretKey = GlobalConfiguration.getString(AWS_SECRET_KEY_KEY_GLOBAL, null);
		}
		
		if (LOG.isDebugEnabled()) {
			LOG.debug("Checking orphaned instances, " + this.orphanedInstances.size() + " orphaned instances listed.");
		}
		final AmazonEC2Client ec2client = EC2ClientFactory.getEC2Client(awsAccessId, awsSecretKey);

		DescribeInstancesResult result = null;

		try {
			final DescribeInstancesRequest request = new DescribeInstancesRequest();
			result = ec2client.describeInstances(request);
		} catch (AmazonClientException e) {
			throw new InstanceException(StringUtils.stringifyException(e));
		}

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

				final Iterator<Map.Entry<InstanceConnectionInfo, HardwareDescription>> it = this.orphanedInstances
					.entrySet().iterator();

				while (it.hasNext()) {
					final Map.Entry<InstanceConnectionInfo, HardwareDescription> entry = it.next();

					final InstanceConnectionInfo oi = entry.getKey();
					final HardwareDescription hd = entry.getValue();

					if (oi.getAddress().equals(inetAddress) && type != null) {
						LOG.info("Orphaned instance " + oi + " converted into floating instance.");

						// We have found the corresponding orphaned TM.. convert it back to a floating instance.
						final FloatingInstance floatinginstance = new FloatingInstance(t.getInstanceId(), oi, t
							.getLaunchTime().getTime(), this.leasePeriod, type, hd, awsAccessId, awsSecretKey);

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

		LOG.info("Checking for up to " + count + " floating instance of type " + type.getIdentifier());

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
						if (foundfloatinginstances.size() >= count) {
							// We have enough floating instances!
							break;
						}
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

				// Call life cycle method for each floating instance. If true, remove from floating instances list.
				if (entry.getValue().hasLifeCycleEnded()) {
					it.remove();
					LOG.info("Lifecycle of floating instance " + entry.getValue().getInstanceID()
						+ " has ended, terminating instance...");
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

		return this.defaultInstanceType;
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

		synchronized (this.jobToInstancesAssignmentMap) {

			final Iterator<Map.Entry<JobID, JobToInstancesMapping>> it = this.jobToInstancesAssignmentMap.entrySet()
				.iterator();

			while (it.hasNext()) {

				final Map.Entry<JobID, JobToInstancesMapping> entry = it.next();
				final List<EC2CloudInstance> unassignedInstances = entry.getValue().unassignAllInstancesFromJob();
				final Iterator<EC2CloudInstance> it2 = unassignedInstances.iterator();
				while (it2.hasNext()) {
					it2.next().destroyProxies();
				}
			}
		}

		// Stop the timer task
		if (LOG.isDebugEnabled()) {
			LOG.debug("Stopping timer task");
		}
		this.cancel();
	}

	@Override
	public NetworkTopology getNetworkTopology(final JobID jobID) {

		synchronized (this.networkTopologies) {

			return this.networkTopologies.get(jobID);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setInstanceListener(final InstanceListener instanceListener) {

		this.instanceListener = instanceListener;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		final Map<InstanceType, InstanceTypeDescription> availableinstances = new SerializableHashMap<InstanceType, InstanceTypeDescription>();

		for (final InstanceType t : this.availableInstanceTypes) {
			// TODO: Number of available instances is set to 1000 to improve interaction with PACT layer, must be -1
			// actually according to API
			availableinstances
				.put(t, InstanceTypeDescriptionFactory.construct(t, estimateHardwareDescription(t), 1000));
		}

		return availableinstances;
	}

	private HardwareDescription estimateHardwareDescription(final InstanceType instanceType) {

		final int numberOfCPUCores = instanceType.getNumberOfCores();
		final long sizeOfPhysicalMemory = (long) instanceType.getMemorySize() * 1024L * 1024L; // getMemorySize is in MB
		final long sizeOfFreeMemory = (long) Math.floor((double) sizeOfPhysicalMemory * 0.6f); // 0.6 is just a rough
																								// estimation

		return HardwareDescriptionFactory.construct(numberOfCPUCores, sizeOfPhysicalMemory, sizeOfFreeMemory);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public AbstractInstance getInstanceByName(final String name) {

		if (name == null) {
			throw new IllegalArgumentException("Argument name must not be null");
		}

		// TODO: Implement this method
		throw new UnsupportedOperationException("This method is not yet implemented");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void cancelPendingRequests(final JobID jobID) {

		// TODO: Implement this method
		LOG.error("This method is not yet implemented");
	}
}
