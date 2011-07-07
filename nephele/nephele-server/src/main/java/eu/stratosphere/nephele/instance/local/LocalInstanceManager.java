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

package eu.stratosphere.nephele.instance.local;

import java.io.File;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.HardwareDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.instance.InstanceTypeDescriptionFactory;
import eu.stratosphere.nephele.instance.InstanceTypeFactory;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;
import eu.stratosphere.nephele.util.SerializableHashMap;

/**
 * The local instance manager is designed to manage instance allocation/deallocation for a single-node setup. It spans a
 * task manager which is executed within the same process as the job manager. Moreover, it determines the hardware
 * characteristics of the machine it runs on and generates a default instance type with the identifier "default". If
 * desired this default instance type can also be overwritten.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class LocalInstanceManager implements InstanceManager {

	/**
	 * The log object used to report events and errors.
	 */
	private static final Log LOG = LogFactory.getLog(LocalInstanceManager.class);

	/**
	 * The key for the configuration parameter defining the instance type to be used by the local instance manager. If
	 * the parameter is not set, a default instance type with the identifier "default" is generated from the machine's
	 * hardware characteristics.
	 */

	private static final String LOCALINSTANCE_TYPE_KEY = "instancemanager.local.type";

	/**
	 * The instance listener registered with this instance manager.
	 */
	private InstanceListener instanceListener;

	/**
	 * The default instance type which is either generated from the hardware characteristics of the machine the local
	 * instance manager runs on or read from the configuration.
	 */
	private final InstanceType defaultInstanceType;

	/**
	 * A synchronization object to protect critical sections.
	 */
	private final Object synchronizationObject = new Object();

	/**
	 * Stores if the local task manager is currently by a job.
	 */
	private AllocatedResource allocatedResource = null;

	/**
	 * The local instance encapsulating the task manager
	 */
	private LocalInstance localInstance = null;

	/**
	 * The thread running the local task manager.
	 */
	private final LocalTaskManagerThread localTaskManagerThread;

	/**
	 * The network topology the local instance is part of.
	 */
	private final NetworkTopology networkTopology;

	/**
	 * The map of instance type descriptions.
	 */
	private final Map<InstanceType, InstanceTypeDescription> instanceTypeDescriptionMap;

	/**
	 * Constructs a new local instance manager.
	 * 
	 * @param configDir
	 *        the path to the configuration directory
	 */
	public LocalInstanceManager(String configDir) {

		final Configuration config = GlobalConfiguration.getConfiguration();

		// get the default instance type
		InstanceType type = null;
		final String descr = config.getString(LOCALINSTANCE_TYPE_KEY, null);
		if (descr != null) {
			LOG.info("Attempting to parse default instance type from string " + descr);
			type = InstanceTypeFactory.constructFromDescription(descr);
			if (type == null) {
				LOG.warn("Unable to parse default instance type from configuration, using hardware profile instead");
			}
		}

		this.defaultInstanceType = (type != null) ? type : createDefaultInstanceType();

		LOG.info("Default instance type is " + this.defaultInstanceType.getIdentifier());

		this.networkTopology = NetworkTopology.createEmptyTopology();

		this.instanceTypeDescriptionMap = new SerializableHashMap<InstanceType, InstanceTypeDescription>();

		this.localTaskManagerThread = new LocalTaskManagerThread(configDir);
		this.localTaskManagerThread.start();
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

		if (this.defaultInstanceType.getIdentifier().equals(instanceTypeName)) {
			return this.defaultInstanceType;
		}

		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores, int minMemorySize,
			int minDiskCapacity, int maxPricePerHour) {

		if (minNumComputeUnits > this.defaultInstanceType.getNumberOfComputeUnits()) {
			return null;
		}

		if (minNumCPUCores > this.defaultInstanceType.getNumberOfCores()) {
			return null;
		}

		if (minMemorySize > this.defaultInstanceType.getMemorySize()) {
			return null;
		}

		if (minDiskCapacity > this.defaultInstanceType.getDiskCapacity()) {
			return null;
		}

		if (maxPricePerHour > this.defaultInstanceType.getPricePerHour()) {
			return null;
		}

		return this.defaultInstanceType;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void releaseAllocatedResource(JobID jobID, Configuration conf, AllocatedResource allocatedResource)
			throws InstanceException {

		synchronized (this.synchronizationObject) {

			if (this.allocatedResource != null) {

				if (this.allocatedResource.equals(allocatedResource)) {
					this.allocatedResource = null;
					return;
				}
			}

			throw new InstanceException("Resource with allocation ID " + allocatedResource.getAllocationID()
				+ " has not been allocated to job with ID " + jobID
				+ " according to the local instance manager's internal bookkeeping");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestInstance(JobID jobID, Configuration conf, InstanceType instanceType, int count) throws InstanceException {
		//TODO: Implement count - fail if count > 1
		
		boolean assignmentSuccessful = false;
		AllocatedResource allocatedResource = null;
		synchronized (this.synchronizationObject) {

			if (this.localInstance != null) { // Instance is available
				if (this.allocatedResource == null) { // Instance is not used by another job
					allocatedResource = new AllocatedResource(this.localInstance, instanceType, new AllocationID());
					this.allocatedResource = allocatedResource;
					assignmentSuccessful = true;
				}
			}
		}

		if (assignmentSuccessful) {
			// Spawn a new thread to send the notification
			new LocalInstanceNotifier(this.instanceListener, jobID, allocatedResource).start();
		} else {
			throw new InstanceException("No instance of type " + instanceType + " available");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription) {

		synchronized (this.synchronizationObject) {
			if (this.localInstance == null) {
				this.localInstance = new LocalInstance(this.defaultInstanceType,
					instanceConnectionInfo, this.networkTopology.getRootNode(), this.networkTopology,
					hardwareDescription);

				this.instanceTypeDescriptionMap.put(this.defaultInstanceType,
					InstanceTypeDescriptionFactory.construct(this.defaultInstanceType, hardwareDescription, 1));
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {

		// Stop the internal instance of the task manager
		if (this.localTaskManagerThread != null) {
			// Interrupt the thread running the task manager
			this.localTaskManagerThread.interrupt();

			while (!this.localTaskManagerThread.isTaskManagerShutDown()) {
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					break;
				}
			}

			// Clear the instance type description list
			this.instanceTypeDescriptionMap.clear();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) {

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
	 * Creates a default instance type based on the hardware characteristics of the machine that calls this method. The
	 * default instance type contains the machine's number of CPU cores and size of physical memory. The disc capacity
	 * is calculated from the free space in the directory for temporary files.
	 * 
	 * @return the default instance type used for the local machine
	 */
	public static final InstanceType createDefaultInstanceType() {

		final HardwareDescription hardwareDescription = HardwareDescriptionFactory.extractFromSystem();

		int diskCapacityInGB = 0;
		final String tempDir = GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH);
		if (tempDir != null) {
			File f = new File(tempDir);
			diskCapacityInGB = (int) (f.getFreeSpace() / (1024L * 1024L * 1024L));
		}

		final int physicalMemory = (int) (hardwareDescription.getSizeOfPhysicalMemory() / (1024L * 1024L));

		return InstanceTypeFactory.construct("default", hardwareDescription.getNumberOfCPUCores(),
			hardwareDescription.getNumberOfCPUCores(), physicalMemory, diskCapacityInGB, 0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Map<InstanceType, InstanceTypeDescription> getMapOfAvailableInstanceTypes() {

		return this.instanceTypeDescriptionMap;
	}
}
