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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration for the task manager services such as the memory manager,
 * the io manager and the metric registry.
 */
public class TaskManagerServicesConfiguration {

	private final InetAddress taskManagerAddress;

	private final String[] tmpDirPaths;

	private final String[] localRecoveryStateRootDirectories;

	private final int numberOfSlots;

	@Nullable
	private final QueryableStateConfiguration queryableStateConfig;

	/**
	 * Managed memory (in megabytes).
	 *
	 * @see TaskManagerOptions#MANAGED_MEMORY_SIZE
	 */
	private final long configuredMemory;

	private final MemoryType memoryType;

	private final boolean preAllocateMemory;

	private final float memoryFraction;

	private final int pageSize;

	private final long timerServiceShutdownTimeout;

	private final boolean localRecoveryEnabled;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private Optional<Time> systemResourceMetricsProbingInterval;

	public TaskManagerServicesConfiguration(
			InetAddress taskManagerAddress,
			String[] tmpDirPaths,
			String[] localRecoveryStateRootDirectories,
			boolean localRecoveryEnabled,
			@Nullable QueryableStateConfiguration queryableStateConfig,
			int numberOfSlots,
			long configuredMemory,
			MemoryType memoryType,
			boolean preAllocateMemory,
			float memoryFraction,
			int pageSize,
			long timerServiceShutdownTimeout,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			Optional<Time> systemResourceMetricsProbingInterval) {

		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.localRecoveryStateRootDirectories = checkNotNull(localRecoveryStateRootDirectories);
		this.localRecoveryEnabled = checkNotNull(localRecoveryEnabled);
		this.queryableStateConfig = queryableStateConfig;
		this.numberOfSlots = checkNotNull(numberOfSlots);

		this.configuredMemory = configuredMemory;
		this.memoryType = checkNotNull(memoryType);
		this.preAllocateMemory = preAllocateMemory;
		this.memoryFraction = memoryFraction;
		this.pageSize = pageSize;

		checkArgument(timerServiceShutdownTimeout >= 0L, "The timer " +
			"service shutdown timeout must be greater or equal to 0.");
		this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

		this.systemResourceMetricsProbingInterval = checkNotNull(systemResourceMetricsProbingInterval);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	InetAddress getTaskManagerAddress() {
		return taskManagerAddress;
	}

	public String[] getTmpDirPaths() {
		return tmpDirPaths;
	}

	String[] getLocalRecoveryStateRootDirectories() {
		return localRecoveryStateRootDirectories;
	}

	boolean isLocalRecoveryEnabled() {
		return localRecoveryEnabled;
	}

	@Nullable
	QueryableStateConfiguration getQueryableStateConfig() {
		return queryableStateConfig;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public float getMemoryFraction() {
		return memoryFraction;
	}

	/**
	 * Returns the memory type to use.
	 *
	 * @return on-heap or off-heap memory
	 */
	MemoryType getMemoryType() {
		return memoryType;
	}

	/**
	 * Returns the size of the managed memory (in megabytes), if configured.
	 *
	 * @return managed memory or a default value (currently <tt>-1</tt>) if not configured
	 *
	 * @see TaskManagerOptions#MANAGED_MEMORY_SIZE
	 */
	long getConfiguredMemory() {
		return configuredMemory;
	}

	boolean isPreAllocateMemory() {
		return preAllocateMemory;
	}

	public int getPageSize() {
		return pageSize;
	}

	long getTimerServiceShutdownTimeout() {
		return timerServiceShutdownTimeout;
	}

	public Optional<Time> getSystemResourceMetricsProbingInterval() {
		return systemResourceMetricsProbingInterval;
	}

	RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
		return retryingRegistrationConfiguration;
	}

	// --------------------------------------------------------------------------------------------
	//  Parsing of Flink configuration
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method to extract TaskManager config parameters from the configuration and to
	 * sanity check them.
	 *
	 * @param configuration The configuration.
	 * @param remoteAddress identifying the IP address under which the TaskManager will be accessible
	 *
	 * @return TaskExecutorConfiguration that wrappers InstanceConnectionInfo, etc.
	 */
	public static TaskManagerServicesConfiguration fromConfiguration(
			Configuration configuration,
			InetAddress remoteAddress) {
		final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);
		String[] localStateRootDir = ConfigurationUtils.parseLocalStateDirectories(configuration);
		if (localStateRootDir.length == 0) {
			// default to temp dirs.
			localStateRootDir = tmpDirs;
		}

		boolean localRecoveryMode = configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY);

		final QueryableStateConfiguration queryableStateConfig = QueryableStateConfiguration.fromConfiguration(configuration);

		boolean preAllocateMemory = configuration.getBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE);

		long timerServiceShutdownTimeout = AkkaUtils.getTimeout(configuration).toMillis();

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		return new TaskManagerServicesConfiguration(
			remoteAddress,
			tmpDirs,
			localStateRootDir,
			localRecoveryMode,
			queryableStateConfig,
			ConfigurationParserUtils.getSlot(configuration),
			ConfigurationParserUtils.getManagedMemorySize(configuration),
			ConfigurationParserUtils.getMemoryType(configuration),
			preAllocateMemory,
			ConfigurationParserUtils.getManagedMemoryFraction(configuration),
			ConfigurationParserUtils.getPageSize(configuration),
			timerServiceShutdownTimeout,
			retryingRegistrationConfiguration,
			ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));
	}
}
