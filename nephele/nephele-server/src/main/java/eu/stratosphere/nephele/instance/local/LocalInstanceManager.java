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
import java.util.List;

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
import eu.stratosphere.nephele.util.SerializableArrayList;

/**
 *
 */
public class LocalInstanceManager implements InstanceManager {
	private InstanceListener instanceListener;

	private final InstanceType defaultInstanceType;

	private final Object synchronizationObject = new Object();

	private AllocatedResource allocatedResource = null;

	private LocalTaskManagerThread localTaskManagerThread;

	private final NetworkTopology networkTopology;

	private final List<InstanceTypeDescription> instanceTypeDescriptionList;

	public LocalInstanceManager(String configDir) {
		Configuration config = GlobalConfiguration.getConfiguration();

		// get the default instance type
		InstanceType type = null;
		String descr = config.getString(ConfigConstants.JOBMANAGER_LOCALINSTANCE_TYPE_KEY, null);
		try {
			if (descr != null) {
				type = InstanceTypeFactory.constructFromDescription(descr);
			}
		} catch (IllegalArgumentException iaex) {
			LogFactory.getLog(LocalInstanceManager.class).error(
				"Invalid description of default instance: " + descr + ". Using default instance type.", iaex);
		}

		this.defaultInstanceType = type != null ? type : createDefaultInstanceType();
		this.networkTopology = NetworkTopology.createEmptyTopology();

		this.instanceTypeDescriptionList = new SerializableArrayList<InstanceTypeDescription>();

		this.localTaskManagerThread = new LocalTaskManagerThread(configDir);
		this.localTaskManagerThread.start();
	}

	@Override
	public InstanceType getDefaultInstanceType() {

		return this.defaultInstanceType;
	}

	@Override
	public InstanceType getInstanceTypeByName(String instanceTypeName) {
		// Ignore instanceTypeName, just return the default instance type
		return this.defaultInstanceType;
	}

	@Override
	public InstanceType getSuitableInstanceType(int minNumComputeUnits, int minNumCPUCores, int minMemorySize,
			int minDiskCapacity, int maxPricePerHour) {
		// Ignore requirements, always return the default instance type
		return this.defaultInstanceType;
	}

	@Override
	public void releaseAllocatedResource(JobID jobID, Configuration conf, AllocatedResource allocatedResource)
			throws InstanceException {

		// Nothing to do here
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void requestInstance(JobID jobID, Configuration conf, InstanceType instanceType) throws InstanceException {

		AllocatedResource allocatedResource = null;
		synchronized (this.synchronizationObject) {
			if (this.allocatedResource == null) {
				throw new InstanceException("No instance of type " + instanceType + " available");
			} else {
				allocatedResource = this.allocatedResource;
			}
		}

		// Spawn a new thread to send the notification
		new LocalInstanceNotifier(this.instanceListener, jobID, allocatedResource).start();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription) {

		synchronized (this.synchronizationObject) {
			if (this.allocatedResource == null) {
				this.allocatedResource = new AllocatedResource(new LocalInstance(this.defaultInstanceType,
					instanceConnectionInfo, this.networkTopology.getRootNode(), this.networkTopology,
					hardwareDescription),
					new AllocationID());

				this.instanceTypeDescriptionList.add(InstanceTypeDescriptionFactory.construct(this.defaultInstanceType,
					hardwareDescription, 1));
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
			this.instanceTypeDescriptionList.clear();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NetworkTopology getNetworkTopology(JobID jobID) {
		// TODO: Make configuration job specific
		return this.networkTopology;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void setInstanceListener(InstanceListener instanceListener) {

		this.instanceListener = instanceListener;
	}

	// ------------------------------------------------------------------------

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
	public List<InstanceTypeDescription> getListOfAvailableInstanceTypes() {
		// TODO Auto-generated method stub
		return null;
	}
}
