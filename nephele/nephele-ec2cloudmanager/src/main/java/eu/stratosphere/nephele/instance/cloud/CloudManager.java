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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

		final Boolean isSecure = GlobalConfiguration.getBoolean(EC2WSSECUREKEY, false);

		final String server = GlobalConfiguration.getString(EC2WSSERVERKEY, null);
		if (server == null) {
			LOG.error("Unable to contact cloud: web service server unknown");
			return null;
		}

		final int port = GlobalConfiguration.getInteger(EC2WSPORTKEY, -1);
		if (port < 0) {
			LOG.error("cloud.ec2ws.port not defined in config file");
			return null;
		}

		com.xerox.amazonws.ec2.Jec2 ec2Client = null;

		try {
			ec2Client = new com.xerox.amazonws.ec2.Jec2(awsAccessId, awsSecretKey, isSecure, server, port);
			ec2Client.setResourcePrefix("/services/Eucalyptus");
			ec2Client.setSignatureVersion(1);
		} catch (Exception e) {
			LOG.error("Unable to contact cloud: " + StringUtils.stringifyException(e));
			return null;
		}

		List<com.xerox.amazonws.ec2.TerminatingInstanceDescription> terminatedInstances = null;
		final String[] instanceIDs = { instanceID };

		try {
			LOG.info("Trying to terminate instance " + instanceID);
			terminatedInstances = ec2Client.terminateInstances(instanceIDs);
		} catch (Exception e) {
			LOG.error("Unable to destroy instance: " + StringUtils.stringifyException(e));
			return null;
		}

		if (terminatedInstances == null) {
			LOG.error("Unable to destroy instance: terminated instance is null");
			return null;
		}

		if (terminatedInstances.size() != 1) {
			LOG.error("More or less than one instance terminated at a time, this is unexpected");
			return null;
		}

		return terminatedInstances.get(0).getInstanceId();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription) {

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
	 * Returns all allocated instances by a specific user with an AWS access ID and an AWS secret key.
	 * 
	 * @param owner
	 *        the owner of the instances
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 * @return a list of allocated instances by a specific user with an AWS access ID and an AWS secret key
	 * @throws InstanceException
	 *         something wrong happens to the global configuration
	 */
	private List<com.xerox.amazonws.ec2.ReservationDescription.Instance> describeInstances(String owner,
			String awsAccessId, String awsSecretKey) throws InstanceException {

		final Boolean isSecure = GlobalConfiguration.getBoolean(EC2WSSECUREKEY, false);

		final String server = GlobalConfiguration.getString(EC2WSSERVERKEY, null);
		if (server == null) {
			LOG.error("Unable to contact cloud: web service server unknown");
			return null;
		}

		final int port = GlobalConfiguration.getInteger(EC2WSPORTKEY, -1);
		if (port < 0) {
			LOG.error("cloud.ec2ws.port not defined in config file");
			return null;
		}

		final com.xerox.amazonws.ec2.Jec2 ec2Client = new com.xerox.amazonws.ec2.Jec2(awsAccessId, awsSecretKey,
			isSecure, server, port);
		ec2Client.setResourcePrefix("/services/Eucalyptus");
		ec2Client.setSignatureVersion(1);

		List<com.xerox.amazonws.ec2.ReservationDescription> reservation = null;

		try {
			reservation = ec2Client.describeInstances(new String[0]);
		} catch (Exception e) {
			LOG.error("Error while communicating with cloud: " + StringUtils.stringifyException(e));
			return null;
		}

		if (reservation == null) {
			LOG.debug("EC2 describeInstances returned null");
			return null;
		}

		final List<com.xerox.amazonws.ec2.ReservationDescription.Instance> returnInstances = new ArrayList<com.xerox.amazonws.ec2.ReservationDescription.Instance>();

		final Iterator<com.xerox.amazonws.ec2.ReservationDescription> it = reservation.iterator();

		while (it.hasNext()) {

			final com.xerox.amazonws.ec2.ReservationDescription r = it.next();

			// Check if owner matches
			if (!owner.equals(r.getOwner())) {
				continue;
			}

			final List<com.xerox.amazonws.ec2.ReservationDescription.Instance> instances = r.getInstances();

			if (instances == null) {
				LOG.debug("EC2 describesInstances includes no instances for owner " + owner);
				continue;
			}

			final Iterator<com.xerox.amazonws.ec2.ReservationDescription.Instance> it2 = instances.iterator();

			while (it2.hasNext()) {
				returnInstances.add(it2.next());
			}
			// End while it2
		}
		// End while it

		return returnInstances;
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

			final Iterator<String> it = this.reservedInstances.keySet().iterator();

			while (it.hasNext()) {

				final String instanceID = it.next();
				final JobID jobID = this.reservedInstances.get(instanceID);
				JobToInstancesMapping mapping = null;

				synchronized (this.jobToInstancesMap) {
					mapping = this.jobToInstancesMap.get(jobID);
				}

				if (mapping == null) {
					LOG.error("Unknown mapping for job ID " + jobID);
					continue;
				}

				final List<com.xerox.amazonws.ec2.ReservationDescription.Instance> instances = describeInstances(
					mapping.getOwner(), mapping.getAwsAccessId(), mapping.getAwsSecretKey());

				if (instances == null) {
					continue;
				}

				final Iterator<com.xerox.amazonws.ec2.ReservationDescription.Instance> it2 = instances.iterator();

				while (it2.hasNext()) {
					final com.xerox.amazonws.ec2.ReservationDescription.Instance i = it2.next();

					InetAddress candidateAddress = null;

					try {
						candidateAddress = InetAddress.getByName(i.getDnsName());
					} catch (UnknownHostException e) {
						LOG.warn("Cannot convert " + i.getDnsName() + " into an IP address");
						continue;
					}

					if (instanceConnectionInfo.getAddress().equals(candidateAddress)) {
						return convertIntoCloudInstance(i, instanceConnectionInfo, mapping.getOwner());
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
	private CloudInstance convertIntoCloudInstance(com.xerox.amazonws.ec2.ReservationDescription.Instance instance,
			InstanceConnectionInfo instanceConnectionInfo, String owner) {

		InstanceType type = null;

		// Find out type of the instance
		for (int i = 0; i < this.availableInstanceTypes.length; i++) {

			if (this.availableInstanceTypes[i].getIdentifier().equals(instance.getInstanceType().getTypeId())) {
				type = this.availableInstanceTypes[i];
				break;
			}
		}

		if (type == null) {
			LOG.error("Cannot translate " + instance.getInstanceType() + " into a valid instance type");
			return null;
		}

		final CloudInstance cloudInstance = new CloudInstance(instance.getInstanceId(), type, owner,
			instanceConnectionInfo, instance.getLaunchTime().getTimeInMillis(), this.networkTopology.getRootNode(),
			this.networkTopology, null); //TODO: Define hardware descriptions for cloud instance types
		this.cloudInstances.add(cloudInstance);
		return cloudInstance;
	}

	/**
	 * Requests an instance for a new job. If there is a floating instance, this instance will be assigned to this job.
	 * If not, a new instance
	 * will be allocated in the cloud and directly reserved for this job.
	 * 
	 * @param jobID
	 *        the ID of the job, which needs to request an instance.
	 * @param conf
	 *        the configuration of the job
	 * @param instanceType
	 *        the type of the requesting instance
	 * @throws InstanceException
	 *         something wrong happens to the job configuration
	 */
	@Override
	public synchronized void requestInstance(JobID jobID, Configuration conf, InstanceType instanceType)
			throws InstanceException {

		if (conf == null) {
			throw new InstanceException("No job configuration provided, unable to acquire credentials");
		}

		// First check, if all required configuration entries are available
		final String owner = conf.getString("job.cloud.username", null);
		if (owner == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find username");
		}

		final String awsAccessId = conf.getString("job.cloud.awsaccessid", null);
		if (awsAccessId == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS access ID");
		}

		final String awsSecretKey = conf.getString("job.cloud.awssecretkey", null);
		if (awsSecretKey == null) {
			throw new InstanceException("Unable to allocate cloud instance: Cannot find AWS secret key");
		}

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

		// Check if there is any floating instance with matching owner and type
		final CloudInstance instance = anyFloatingInstanceAvailable(owner, awsAccessId, awsSecretKey, instanceType);
		if (instance != null) {
			jobToInstanceMapping.assignInstanceToJob(instance);
			this.instanceListener.resourceAllocated(jobID, instance.asAllocatedResource());
		} else {

			// If there is no floating instance available we have to allocate a new one from the cloud
			final String instanceID = allocateCloudInstance(awsAccessId, awsSecretKey, instanceType);

			// Add the instance ID to reversed instances, so the instance is not treated as floating when it arrives
			this.reservedInstances.put(instanceID, jobID);
		}
	}

	/**
	 * Allocates an instance (virtual machine) in the cloud.
	 * 
	 * @param awsAccessId
	 *        the access ID into AWS
	 * @param awsSecretKey
	 *        the secret key used to generate signatures for authentication
	 * @param instanceType
	 *        the type of the allocating instance
	 * @return the ID of the allocated instance
	 */
	private String allocateCloudInstance(String awsAccessId, String awsSecretKey, InstanceType instanceType) {

		final Boolean isSecure = GlobalConfiguration.getBoolean(EC2WSSECUREKEY, false);

		final String server = GlobalConfiguration.getString(EC2WSSERVERKEY, null);
		if (server == null) {
			LOG.error("Unable to contact cloud: web service server unknown");
			return null;
		}

		final int port = GlobalConfiguration.getInteger(EC2WSPORTKEY, -1);
		if (port < 0) {
			LOG.error("cloud.ec2ws.port not defined in config file");
			return null;
		}

		final String imageID = GlobalConfiguration.getString("ec2.image.id", null);
		if (imageID == null) {
			LOG.error("Unable to allocate instance: Image ID is unknown");
			return null;
		}

		com.xerox.amazonws.ec2.Jec2 ec2Client = null;

		try {
			ec2Client = new com.xerox.amazonws.ec2.Jec2(awsAccessId, awsSecretKey, isSecure, server, port);
			ec2Client.setResourcePrefix("/services/Eucalyptus");
			ec2Client.setSignatureVersion(1);
		} catch (Exception e) {
			LOG.error("Unable to contact cloud: " + StringUtils.stringifyException(e));
			return null;
		}

		com.xerox.amazonws.ec2.ReservationDescription reservation = null;

		try {
			// TODO: Make key name configurable
			reservation = ec2Client.runInstances(imageID, 1, 1, new ArrayList<String>(), new String(), "mykey", true,
				com.xerox.amazonws.ec2.InstanceType.getTypeFromString(instanceType.getIdentifier()), new String(),
				new String(), new String(), new ArrayList<com.xerox.amazonws.ec2.BlockDeviceMapping>());
		} catch (Exception e) {
			LOG.error("Unable to allocate instance: " + StringUtils.stringifyException(e));
			return null;
		}

		if (reservation == null) {
			LOG.error("Unable to allocate instance: reservation is null");
			return null;
		}

		if (reservation.getInstances().size() != 1) {
			LOG.error("More or less than one instance reserved at a time, this is unexpected");
			return null;
		}

		// Return the instance ID
		System.out.println(reservation.getInstances().get(0).getLaunchTime());
		return reservation.getInstances().get(0).getInstanceId();
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
	private CloudInstance anyFloatingInstanceAvailable(String owner, String awsAccessId, String awsSecretKey,
			InstanceType type) throws InstanceException {

		synchronized (this.floatingInstances) {

			if (this.floatingInstances.size() == 0) {
				// There is no floating instance known to the system
				return null;
			}

			final List<com.xerox.amazonws.ec2.ReservationDescription.Instance> instances = describeInstances(owner,
				awsAccessId, awsSecretKey);

			if (instances == null) {
				LOG.debug("EC2 describesInstances includes no instances for owner " + owner);
				return null;
			}

			final Iterator<com.xerox.amazonws.ec2.ReservationDescription.Instance> it = instances.iterator();

			while (it.hasNext()) {

				final com.xerox.amazonws.ec2.ReservationDescription.Instance instance = it.next();

				// Check for the correct instance type
				if (!type.getIdentifier().equals(instance.getInstanceType().getTypeId())) {

					continue;
				}

				InetAddress inetAddress = null;
				try {
					inetAddress = InetAddress.getByName(instance.getDnsName());
				} catch (UnknownHostException e) {
					LOG.error("Cannot resolve " + instance.getDnsName() + " into an IP address: "
						+ StringUtils.stringifyException(e));
					continue;
				}

				// Check if instance entry is a floating instance
				if (this.floatingInstances.containsKey(inetAddress)) {

					final FloatingInstance floatingInstance = this.floatingInstances.remove(inetAddress);
					this.floatingInstanceIDs.remove(floatingInstance.getInstanceID());
					return convertIntoCloudInstance(instance, floatingInstance.getInstanceConnectionInfo(), owner);
				}
			}
		}

		return null;
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
	public List<InstanceTypeDescription> getListOfAvailableInstanceTypes() {
		// TODO Auto-generated method stub
		return null;
	}
}
