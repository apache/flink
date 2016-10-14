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
import org.apache.flink.runtime.akka.AkkaUtils;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class MiniClusterConfiguration {

	private final Configuration config;

	private boolean singleRpcService = true;

	private int numJobManagers = 1;

	private int numTaskManagers = 1;

	private String commonBindAddress;

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

	public void setNumTaskManagerSlots(int numTaskSlots) {
		checkArgument(numTaskSlots >= 1, "must have at least one task slot per TaskManager");
		this.config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numTaskSlots);
	}

	public void setCommonRpcBindAddress(String bindAddress) {
		checkNotNull(bindAddress, "bind address must not be null");
		this.commonBindAddress = bindAddress;
	}

	// ------------------------------------------------------------------------
	//  getters
	// ------------------------------------------------------------------------

	public Configuration getConfiguration() {
		return config;
	}

	public boolean getUseSingleRpcSystem() {
		return singleRpcService;
	}

	public int getNumJobManagers() {
		return numJobManagers;
	}

	public int getNumTaskManagers() {
		return numTaskManagers;
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

	public Time getRpcTimeout() {
		FiniteDuration duration = AkkaUtils.getTimeout(config);
		return Time.of(duration.length(), duration.unit());
	}

	// ------------------------------------------------------------------------
	//  utils
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "MiniClusterConfiguration{" +
				"singleRpcService=" + singleRpcService +
				", numJobManagers=" + numJobManagers +
				", numTaskManagers=" + numTaskManagers +
				", commonBindAddress='" + commonBindAddress + '\'' +
				", config=" + config +
				'}';
	}
}
