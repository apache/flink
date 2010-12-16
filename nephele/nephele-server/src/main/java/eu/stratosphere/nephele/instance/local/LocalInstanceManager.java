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

import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.configuration.ConfigConstants;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.configuration.GlobalConfiguration;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.AllocationID;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceException;
import eu.stratosphere.nephele.instance.InstanceListener;
import eu.stratosphere.nephele.instance.InstanceManager;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.topology.NetworkTopology;

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

	public LocalInstanceManager(String configDir) {
		Configuration config = GlobalConfiguration.getConfiguration();

		// get the default instance type
		InstanceType type = null;
		String descr = config.getString(ConfigConstants.JOBMANAGER_LOCALINSTANCE_TYPE_KEY, null);
		try {
			if (descr != null) {
				type = InstanceType.getTypeFromString(descr);
			}
		} catch (IllegalArgumentException iaex) {
			LogFactory.getLog(LocalInstanceManager.class).error(
				"Invalid description of default instance: " + descr + ". Using default instance type.", iaex);
		}

		this.defaultInstanceType = type != null ? type : createDefaultInstanceType();
		this.networkTopology = NetworkTopology.createEmptyTopology();

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

	@Override
	public void reportHeartBeat(InstanceConnectionInfo instanceConnectionInfo) {

		synchronized (this.synchronizationObject) {
			if (this.allocatedResource == null) {
				this.allocatedResource = new AllocatedResource(new LocalInstance(this.defaultInstanceType,
					instanceConnectionInfo, this.networkTopology.getRootNode(), this.networkTopology),
					new AllocationID());
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
	 * Creates an instance type for the local machine that calls this method. The local instance is
	 * given the system's number of CPU cores, the amount of memory currently available to the system
	 * (actually 80% of it) and the amount of disc space in the temp directory.
	 * 
	 * @return An instance type for the local machine.
	 */
	public static final InstanceType createDefaultInstanceType() {
		final Runtime runtime = Runtime.getRuntime();

		final int numberOfCPUCores = runtime.availableProcessors();
		final int memorySizeInMB = (int) ((runtime.freeMemory() + (runtime.maxMemory() - runtime.totalMemory())) * 0.8f / (1024 * 1024));

		int diskCapacityInGB = 0;
		final String tempDir = System.getProperty("java.io.tmpdir");
		if (tempDir != null) {
			File f = new File(tempDir);
			diskCapacityInGB = (int) (f.getFreeSpace() * 0.8f / (1024 * 1024 * 1024));
		}

		return new InstanceType("default", numberOfCPUCores, numberOfCPUCores, memorySizeInMB, diskCapacityInGB, 0);
	}
}
