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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.runtime.minicluster.RpcServiceSharing.SHARED;

/**
 * Configuration object for the {@link MiniCluster}.
 */
public class MiniClusterConfiguration {

	private final UnmodifiableConfiguration configuration;

	private final int numTaskManagers;

	private final RpcServiceSharing rpcServiceSharing;

	@Nullable
	private final String commonBindAddress;

	private final MiniCluster.HaServices haServices;

	// ------------------------------------------------------------------------
	//  Construction
	// ------------------------------------------------------------------------

	public MiniClusterConfiguration(
			Configuration configuration,
			int numTaskManagers,
			RpcServiceSharing rpcServiceSharing,
			@Nullable String commonBindAddress,
			MiniCluster.HaServices haServices) {
		this.numTaskManagers = numTaskManagers;
		this.configuration = generateConfiguration(Preconditions.checkNotNull(configuration));
		this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
		this.commonBindAddress = commonBindAddress;
		this.haServices = haServices;
	}

	private UnmodifiableConfiguration generateConfiguration(final Configuration configuration) {
		final Configuration modifiedConfig = new Configuration(configuration);

		TaskExecutorResourceUtils.adjustForLocalExecution(modifiedConfig);

		return new UnmodifiableConfiguration(modifiedConfig);
	}

	// ------------------------------------------------------------------------
	//  getters
	// ------------------------------------------------------------------------

	public RpcServiceSharing getRpcServiceSharing() {
		return rpcServiceSharing;
	}

	public int getNumTaskManagers() {
		return numTaskManagers;
	}

	public String getJobManagerExternalAddress() {
		return commonBindAddress != null ?
			commonBindAddress :
			configuration.getString(JobManagerOptions.ADDRESS, "localhost");
	}

	public String getTaskManagerExternalAddress() {
		return commonBindAddress != null ?
			commonBindAddress :
			configuration.getString(TaskManagerOptions.HOST, "localhost");
	}

	public String getJobManagerExternalPortRange() {
		return String.valueOf(configuration.getInteger(JobManagerOptions.PORT, 0));
	}

	public String getTaskManagerExternalPortRange() {
		return configuration.getString(TaskManagerOptions.RPC_PORT);
	}

	public String getJobManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(JobManagerOptions.BIND_HOST, "localhost");
	}

	public String getTaskManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(TaskManagerOptions.BIND_HOST, "localhost");
	}

	public Time getRpcTimeout() {
		return AkkaUtils.getTimeoutAsTime(configuration);
	}

	public UnmodifiableConfiguration getConfiguration() {
		return configuration;
	}

	public MiniCluster.HaServices getHaServices() {
		return haServices;
	}

	@Override
	public String toString() {
		return "MiniClusterConfiguration {" +
				"singleRpcService=" + rpcServiceSharing +
				", numTaskManagers=" + numTaskManagers +
				", commonBindAddress='" + commonBindAddress + '\'' +
				", config=" + configuration +
				'}';
	}

	// ----------------------------------------------------------------------------------
	// Enums
	// ----------------------------------------------------------------------------------

	// ----------------------------------------------------------------------------------
	// Builder
	// ----------------------------------------------------------------------------------

	/**
	 * Builder for the MiniClusterConfiguration.
	 */
	public static class Builder {
		private Configuration configuration = new Configuration();
		private int numTaskManagers = 1;
		private int numSlotsPerTaskManager = 1;
		private RpcServiceSharing rpcServiceSharing = SHARED;
		@Nullable
		private String commonBindAddress = null;
		private MiniCluster.HaServices haServices = MiniCluster.HaServices.CONFIGURED;

		public Builder setConfiguration(Configuration configuration1) {
			this.configuration = Preconditions.checkNotNull(configuration1);
			return this;
		}

		public Builder setNumTaskManagers(int numTaskManagers) {
			this.numTaskManagers = numTaskManagers;
			return this;
		}

		public Builder setNumSlotsPerTaskManager(int numSlotsPerTaskManager) {
			this.numSlotsPerTaskManager = numSlotsPerTaskManager;
			return this;
		}

		public Builder setRpcServiceSharing(RpcServiceSharing rpcServiceSharing) {
			this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
			return this;
		}

		public Builder setCommonBindAddress(String commonBindAddress) {
			this.commonBindAddress = commonBindAddress;
			return this;
		}

		public Builder setHaServices(MiniCluster.HaServices haServices) {
			this.haServices = haServices;
			return this;
		}

		public MiniClusterConfiguration build() {
			final Configuration modifiedConfiguration = new Configuration(configuration);
			modifiedConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, numSlotsPerTaskManager);
			modifiedConfiguration.setString(
				RestOptions.ADDRESS,
				modifiedConfiguration.getString(RestOptions.ADDRESS, "localhost"));

			return new MiniClusterConfiguration(
				modifiedConfiguration,
				numTaskManagers,
				rpcServiceSharing,
				commonBindAddress,
				haServices);
		}
	}
}
