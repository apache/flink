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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import static org.apache.flink.runtime.minicluster.RpcServiceSharing.SHARED;

/**
 * Configuration object for the {@link MiniCluster}.
 */
public class MiniClusterConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(MiniClusterConfiguration.class);

	static final String SCHEDULER_TYPE_KEY = JobManagerOptions.SCHEDULER.key();
	static final MemorySize DEFAULT_SHUFFLE_MEMORY_SIZE = MemorySize.parse("64m");
	static final MemorySize DEFAULT_MANAGED_MEMORY_SIZE = MemorySize.parse("16m");

	private final UnmodifiableConfiguration configuration;

	private final int numTaskManagers;

	private final RpcServiceSharing rpcServiceSharing;

	@Nullable
	private final String commonBindAddress;

	// ------------------------------------------------------------------------
	//  Construction
	// ------------------------------------------------------------------------

	public MiniClusterConfiguration(
			Configuration configuration,
			int numTaskManagers,
			RpcServiceSharing rpcServiceSharing,
			@Nullable String commonBindAddress) {

		this.numTaskManagers = numTaskManagers;
		this.configuration = generateConfiguration(Preconditions.checkNotNull(configuration));
		this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
		this.commonBindAddress = commonBindAddress;
	}

	private UnmodifiableConfiguration generateConfiguration(final Configuration configuration) {
		String schedulerType = System.getProperty(SCHEDULER_TYPE_KEY);
		if (StringUtils.isNullOrWhitespaceOnly(schedulerType)) {
			schedulerType = JobManagerOptions.SCHEDULER.defaultValue();
		}

		final Configuration modifiedConfig = new Configuration(configuration);

		if (!modifiedConfig.contains(JobManagerOptions.SCHEDULER)) {
			modifiedConfig.setString(JobManagerOptions.SCHEDULER, schedulerType);
		}

		adjustTaskManagerMemoryConfigurations(modifiedConfig);

		return new UnmodifiableConfiguration(modifiedConfig);
	}

	@VisibleForTesting
	static Configuration adjustTaskManagerMemoryConfigurations(final Configuration toBeModifiedConfiguration) {
		if (!TaskExecutorResourceUtils.isTaskExecutorResourceExplicitlyConfigured(toBeModifiedConfiguration)) {
			// This does not affect the JVM heap size for local execution,
			// we simply set it to pass the sanity checks in memory calculations
			toBeModifiedConfiguration.set(TaskManagerOptions.TASK_HEAP_MEMORY, MemorySize.parse("100m"));
		}

		if (!TaskExecutorResourceUtils.isShuffleMemoryExplicitlyConfigured(toBeModifiedConfiguration)) {
			toBeModifiedConfiguration.set(TaskManagerOptions.SHUFFLE_MEMORY_MIN, DEFAULT_SHUFFLE_MEMORY_SIZE);
			toBeModifiedConfiguration.set(TaskManagerOptions.SHUFFLE_MEMORY_MAX, DEFAULT_SHUFFLE_MEMORY_SIZE);
			LOG.info("Shuffle memory is not explicitly configured, use {} for local execution.", DEFAULT_SHUFFLE_MEMORY_SIZE);
		}

		if (!TaskExecutorResourceUtils.isManagedMemorySizeExplicitlyConfigured(toBeModifiedConfiguration)) {
			toBeModifiedConfiguration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, DEFAULT_MANAGED_MEMORY_SIZE);
			LOG.info("Managed memory is not explicitly configured, use {} for local execution.", DEFAULT_MANAGED_MEMORY_SIZE);
		}

		return toBeModifiedConfiguration;
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

	public String getJobManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(JobManagerOptions.ADDRESS, "localhost");
	}

	public String getTaskManagerBindAddress() {
		return commonBindAddress != null ?
				commonBindAddress :
				configuration.getString(TaskManagerOptions.HOST, "localhost");
	}

	public Time getRpcTimeout() {
		return AkkaUtils.getTimeoutAsTime(configuration);
	}

	public UnmodifiableConfiguration getConfiguration() {
		return configuration;
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
				commonBindAddress);
		}
	}
}
