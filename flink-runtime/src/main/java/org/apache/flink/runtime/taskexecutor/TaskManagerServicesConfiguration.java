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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorResourceUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
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

	private final Configuration configuration;

	private final ResourceID resourceID;

	private final InetAddress taskManagerAddress;

	private final boolean localCommunicationOnly;

	private final String[] tmpDirPaths;

	private final String[] localRecoveryStateRootDirectories;

	private final int numberOfSlots;

	@Nullable
	private final QueryableStateConfiguration queryableStateConfig;

	private final int pageSize;

	private final long timerServiceShutdownTimeout;

	private final boolean localRecoveryEnabled;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	private Optional<Time> systemResourceMetricsProbingInterval;

	private final TaskExecutorResourceSpec taskExecutorResourceSpec;

	public TaskManagerServicesConfiguration(
			Configuration configuration,
			ResourceID resourceID,
			InetAddress taskManagerAddress,
			boolean localCommunicationOnly,
			String[] tmpDirPaths,
			String[] localRecoveryStateRootDirectories,
			boolean localRecoveryEnabled,
			@Nullable QueryableStateConfiguration queryableStateConfig,
			int numberOfSlots,
			int pageSize,
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			long timerServiceShutdownTimeout,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration,
			Optional<Time> systemResourceMetricsProbingInterval) {
		this.configuration = checkNotNull(configuration);
		this.resourceID = checkNotNull(resourceID);

		this.taskManagerAddress = checkNotNull(taskManagerAddress);
		this.localCommunicationOnly = localCommunicationOnly;
		this.tmpDirPaths = checkNotNull(tmpDirPaths);
		this.localRecoveryStateRootDirectories = checkNotNull(localRecoveryStateRootDirectories);
		this.localRecoveryEnabled = checkNotNull(localRecoveryEnabled);
		this.queryableStateConfig = queryableStateConfig;
		this.numberOfSlots = checkNotNull(numberOfSlots);

		this.pageSize = pageSize;

		this.taskExecutorResourceSpec = taskExecutorResourceSpec;

		checkArgument(timerServiceShutdownTimeout >= 0L, "The timer " +
			"service shutdown timeout must be greater or equal to 0.");
		this.timerServiceShutdownTimeout = timerServiceShutdownTimeout;
		this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);

		this.systemResourceMetricsProbingInterval = checkNotNull(systemResourceMetricsProbingInterval);
	}

	// --------------------------------------------------------------------------------------------
	//  Getter/Setter
	// --------------------------------------------------------------------------------------------

	public Configuration getConfiguration() {
		return configuration;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	InetAddress getTaskManagerAddress() {
		return taskManagerAddress;
	}

	boolean isLocalCommunicationOnly() {
		return localCommunicationOnly;
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

	public int getPageSize() {
		return pageSize;
	}

	public MemorySize getShuffleMemorySize() {
		return taskExecutorResourceSpec.getShuffleMemSize();
	}

	public MemorySize getManagedMemorySize() {
		return taskExecutorResourceSpec.getManagedMemorySize();
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
	 * @param resourceID resource ID of the task manager
	 * @param remoteAddress identifying the IP address under which the TaskManager will be accessible
	 * @param localCommunicationOnly True if only local communication is possible.
	 *                               Use only in cases where only one task manager runs.
	 *
	 * @return configuration of task manager services used to create them
	 */
	public static TaskManagerServicesConfiguration fromConfiguration(
			Configuration configuration,
			ResourceID resourceID,
			InetAddress remoteAddress,
			boolean localCommunicationOnly) {
		final String[] tmpDirs = ConfigurationUtils.parseTempDirectories(configuration);
		String[] localStateRootDir = ConfigurationUtils.parseLocalStateDirectories(configuration);
		if (localStateRootDir.length == 0) {
			// default to temp dirs.
			localStateRootDir = tmpDirs;
		}

		boolean localRecoveryMode = configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY);

		final QueryableStateConfiguration queryableStateConfig = QueryableStateConfiguration.fromConfiguration(configuration);

		long timerServiceShutdownTimeout = AkkaUtils.getTimeout(configuration).toMillis();

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		final TaskExecutorResourceSpec taskExecutorResourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
		return new TaskManagerServicesConfiguration(
			configuration,
			resourceID,
			remoteAddress,
			localCommunicationOnly,
			tmpDirs,
			localStateRootDir,
			localRecoveryMode,
			queryableStateConfig,
			ConfigurationParserUtils.getSlot(configuration),
			ConfigurationParserUtils.getPageSize(configuration),
			taskExecutorResourceSpec,
			timerServiceShutdownTimeout,
			retryingRegistrationConfiguration,
			ConfigurationUtils.getSystemResourceMetricsProbingInterval(configuration));
	}
}
