/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MiniClusterConfiguration {

	private final Configuration config;

	private boolean singleRpcService = true;

	private int numJobManagers = 1;

	private int numTaskManagers = 1;

	private int numResourceManagers = 1;

	private String commonBindAddress;

	private long managedMemoryPerTaskManager = -1;

	// ------------------------------------------------------------------------
	//  Construction
	// ------------------------------------------------------------------------

	public MiniClusterConfiguration() {
		this.config = new Configuration();
	}

	public MiniClusterConfiguration(Configuration config) {
		checkNotNull(config);
		this.config = new Configuration(config);
	}

	// ------------------------------------------------------------------------
	//  setters
	// ------------------------------------------------------------------------

	public void addConfiguration(Configuration config) {
		checkNotNull(config, "configuration must not be null");
		this.config.addAll(config);
	}

	public void setUseSingleRpcService() {
		this.singleRpcService = true;
	}

	public void setUseRpcServicePerComponent() {
		this.singleRpcService = false;
	}

	public void setNumJobManagers(int numJobManagers) {
		checkArgument(numJobManagers >= 1, "must have at least one JobManager");
		this.numJobManagers = numJobManagers;
	}

	public void setNumTaskManagers(int numTaskManagers) {
		checkArgument(numTaskManagers >= 1, "must have at least one TaskManager");
		this.numTaskManagers = numTaskManagers;
	}

	public void setNumResourceManagers(int numResourceManagers) {
		checkArgument(numResourceManagers >= 1, "must have at least one ResourceManager");
		this.numResourceManagers = numResourceManagers;
	}

	public void setNumTaskManagerSlots(int numTaskSlots) {
		checkArgument(numTaskSlots >= 1, "must have at least one task slot per TaskManager");
		this.config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numTaskSlots);
	}

	public void setCommonRpcBindAddress(String bindAddress) {
		checkNotNull(bindAddress, "bind address must not be null");
		this.commonBindAddress = bindAddress;
	}

	public void setManagedMemoryPerTaskManager(long managedMemoryPerTaskManager) {
		checkArgument(managedMemoryPerTaskManager > 0, "must have more than 0 MB of memory for the TaskManager.");
		this.managedMemoryPerTaskManager = managedMemoryPerTaskManager;
	}

	// ------------------------------------------------------------------------
	//  getters
	// ------------------------------------------------------------------------

	public boolean getUseSingleRpcSystem() {
		return singleRpcService;
	}

	public int getNumJobManagers() {
		return numJobManagers;
	}

	public int getNumTaskManagers() {
		return numTaskManagers;
	}

	public int getNumResourceManagers() {
		return numResourceManagers;
	}

	public int getNumSlotsPerTaskManager() {
		return config.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);
	}

	public String getJobManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
	}

	public String getTaskManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				config.getString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, "localhost");
	}

	public String getResourceManagerBindAddress() {
		return commonBindAddress != null ?
			commonBindAddress :
			config.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost"); // TODO: Introduce proper configuration constant for the resource manager hostname
	}

	public Time getRpcTimeout() {
		FiniteDuration duration = AkkaUtils.getTimeout(config);
		return Time.of(duration.length(), duration.unit());
	}

	public long getManagedMemoryPerTaskManager() {
		return getOrCalculateManagedMemoryPerTaskManager();
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	public Configuration generateConfiguration() {
		Configuration newConfiguration = new Configuration(config);
		// set the memory
		long memory = getOrCalculateManagedMemoryPerTaskManager();
		newConfiguration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, memory);

		return newConfiguration;
	}

	@Override
	public String toString() {
		return "MiniClusterConfiguration {" +
				"singleRpcService=" + singleRpcService +
				", numJobManagers=" + numJobManagers +
				", numTaskManagers=" + numTaskManagers +
				", numResourceManagers=" + numResourceManagers +
				", commonBindAddress='" + commonBindAddress + '\'' +
				", config=" + config +
				'}';
	}

	/**
	 * Get or calculate the managed memory per task manager. The memory is calculated in the
	 * following order:
	 *
	 * 1. Return {@link #managedMemoryPerTaskManager} if set
	 * 2. Return config.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY) if set
	 * 3. Distribute the available free memory equally among all components (JMs, RMs and TMs) and
	 * calculate the managed memory from the share of memory for a single task manager.
	 *
	 * @return
	 */
	private long getOrCalculateManagedMemoryPerTaskManager() {
		if (managedMemoryPerTaskManager == -1) {
			// no memory set in the mini cluster configuration

			long memorySize = config.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);

			// we could probably use config.contains() but the previous implementation compared to
			// the default (-1) thus allowing the user to explicitly specify this as well
			// -> don't change this behaviour now
			if (memorySize == TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()) {
				// no memory set in the flink configuration
				// share the available memory among all running components

				float memoryFraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
				long networkBuffersMemory =
					(long) config.getInteger(TaskManagerOptions.NETWORK_NUM_BUFFERS) *
						(long) config.getInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE);

				long freeMemory = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag();

				// we assign each component the same amount of free memory
				// (might be a bit of overkill for the JMs and RMs)
				long memoryPerComponent = freeMemory / (numTaskManagers + numResourceManagers + numJobManagers);

				// subtract the network buffer memory
				long memoryMinusNetworkBuffers = memoryPerComponent - networkBuffersMemory;

				// calculate the managed memory size
				long managedMemoryBytes = (long) (memoryMinusNetworkBuffers * memoryFraction);

				return managedMemoryBytes >>> 20;
			} else {
				return memorySize;
			}
		} else {
			return managedMemoryPerTaskManager;
		}
	}
}
