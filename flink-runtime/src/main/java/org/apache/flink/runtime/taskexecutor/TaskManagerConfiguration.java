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
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.time.Duration;

/**
 * Configuration object for {@link TaskExecutor}.
 */
public class TaskManagerConfiguration implements TaskManagerRuntimeInfo {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerConfiguration.class);

	private final int numberSlots;

	private final ResourceProfile defaultSlotResourceProfile;

	private final ResourceProfile totalResourceProfile;

	private final String[] tmpDirectories;

	private final Time timeout;

	// null indicates an infinite duration
	@Nullable
	private final Time maxRegistrationDuration;

	private final UnmodifiableConfiguration configuration;

	private final boolean exitJvmOnOutOfMemory;

	@Nullable
	private final String taskManagerLogPath;

	@Nullable
	private final String taskManagerStdoutPath;

	@Nullable
	private final String taskManagerLogDir;

	private final String taskManagerExternalAddress;

	private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

	public TaskManagerConfiguration(
			int numberSlots,
			ResourceProfile defaultSlotResourceProfile,
			ResourceProfile totalResourceProfile,
			String[] tmpDirectories,
			Time timeout,
			@Nullable Time maxRegistrationDuration,
			Configuration configuration,
			boolean exitJvmOnOutOfMemory,
			@Nullable String taskManagerLogPath,
			@Nullable String taskManagerStdoutPath,
			@Nullable String taskManagerLogDir,
			String taskManagerExternalAddress,
			RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

		this.numberSlots = numberSlots;
		this.defaultSlotResourceProfile = defaultSlotResourceProfile;
		this.totalResourceProfile = totalResourceProfile;
		this.tmpDirectories = Preconditions.checkNotNull(tmpDirectories);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maxRegistrationDuration = maxRegistrationDuration;
		this.configuration = new UnmodifiableConfiguration(Preconditions.checkNotNull(configuration));
		this.exitJvmOnOutOfMemory = exitJvmOnOutOfMemory;
		this.taskManagerLogPath = taskManagerLogPath;
		this.taskManagerStdoutPath = taskManagerStdoutPath;
		this.taskManagerLogDir = taskManagerLogDir;
		this.taskManagerExternalAddress = taskManagerExternalAddress;
		this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public ResourceProfile getDefaultSlotResourceProfile() {
		return defaultSlotResourceProfile;
	}

	public ResourceProfile getTotalResourceProfile() {
		return totalResourceProfile;
	}

	public Time getTimeout() {
		return timeout;
	}

	@Nullable
	public Time getMaxRegistrationDuration() {
		return maxRegistrationDuration;
	}

	@Override
	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public String[] getTmpDirectories() {
		return tmpDirectories;
	}

	@Override
	public boolean shouldExitJvmOnOutOfMemoryError() {
		return exitJvmOnOutOfMemory;
	}

	@Nullable
	public String getTaskManagerLogPath() {
		return taskManagerLogPath;
	}

	@Nullable
	public String getTaskManagerStdoutPath() {
		return taskManagerStdoutPath;
	}

	@Nullable
	public String getTaskManagerLogDir() {
		return taskManagerLogDir;
	}

	@Override
	public String getTaskManagerExternalAddress() {
		return taskManagerExternalAddress;
	}

	public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
		return retryingRegistrationConfiguration;
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods
	// --------------------------------------------------------------------------------------------

	public static TaskManagerConfiguration fromConfiguration(
			Configuration configuration,
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			String externalAddress) {
		int numberSlots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

		if (numberSlots == -1) {
			numberSlots = 1;
		}

		final String[] tmpDirPaths = ConfigurationUtils.parseTempDirectories(configuration);

		final Time timeout;
		try {
			timeout = AkkaUtils.getTimeoutAsTime(configuration);
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"Invalid format for '" + AkkaOptions.ASK_TIMEOUT.key() +
					"'.Use formats like '50 s' or '1 min' to specify the timeout.");
		}

		LOG.debug("Messages have a max timeout of " + timeout);

		Time finiteRegistrationDuration;
		try {
			Duration maxRegistrationDuration = configuration.get(TaskManagerOptions.REGISTRATION_TIMEOUT);
			finiteRegistrationDuration = Time.milliseconds(maxRegistrationDuration.toMillis());
		} catch (IllegalArgumentException e) {
			LOG.warn("Invalid format for parameter {}. Set the timeout to be infinite.",
				TaskManagerOptions.REGISTRATION_TIMEOUT.key());
			finiteRegistrationDuration = null;
		}

		final boolean exitOnOom = configuration.getBoolean(TaskManagerOptions.KILL_ON_OUT_OF_MEMORY);

		final String taskManagerLogPath = configuration.getString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, System.getProperty("log.file"));
		final String taskManagerStdoutPath;
		final String taskManagerLogDir;

		if (taskManagerLogPath != null) {
			final int extension = taskManagerLogPath.lastIndexOf('.');
			taskManagerLogDir = new File(taskManagerLogPath).getParent();

			if (extension > 0) {
				taskManagerStdoutPath = taskManagerLogPath.substring(0, extension) + ".out";
			} else {
				taskManagerStdoutPath = null;
			}
		} else {
			taskManagerStdoutPath = null;
			taskManagerLogDir = null;
		}

		final RetryingRegistrationConfiguration retryingRegistrationConfiguration = RetryingRegistrationConfiguration.fromConfiguration(configuration);

		return new TaskManagerConfiguration(
			numberSlots,
			TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(taskExecutorResourceSpec, numberSlots),
			TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(taskExecutorResourceSpec),
			tmpDirPaths,
			timeout,
			finiteRegistrationDuration,
			configuration,
			exitOnOom,
			taskManagerLogPath,
			taskManagerStdoutPath,
			taskManagerLogDir,
			externalAddress,
			retryingRegistrationConfiguration);
	}
}
