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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import scala.concurrent.duration.Duration;

/**
 * Configuration object for {@link TaskExecutor}.
 */
public class TaskManagerConfiguration implements TaskManagerRuntimeInfo {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerConfiguration.class);

	private final int numberSlots;

	private final String[] tmpDirectories;

	private final Time timeout;
	// null indicates an infinite duration
	private final Time maxRegistrationDuration;
	private final Time initialRegistrationPause;
	private final Time maxRegistrationPause;
	private final Time refusedRegistrationPause;

	private final UnmodifiableConfiguration configuration;

	private final boolean exitJvmOnOutOfMemory;

	private final FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder;

	private final String[] alwaysParentFirstLoaderPatterns;

	@Nullable
	private final String taskManagerLogPath;

	@Nullable
	private final String taskManagerStdoutPath;

	public TaskManagerConfiguration(
		int numberSlots,
		String[] tmpDirectories,
		Time timeout,
		Time maxRegistrationDuration,
		Time initialRegistrationPause,
		Time maxRegistrationPause,
		Time refusedRegistrationPause,
		Configuration configuration,
		boolean exitJvmOnOutOfMemory,
		FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder,
		String[] alwaysParentFirstLoaderPatterns,
		@Nullable String taskManagerLogPath,
		@Nullable String taskManagerStdoutPath) {

		this.numberSlots = numberSlots;
		this.tmpDirectories = Preconditions.checkNotNull(tmpDirectories);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maxRegistrationDuration = maxRegistrationDuration;
		this.initialRegistrationPause = Preconditions.checkNotNull(initialRegistrationPause);
		this.maxRegistrationPause = Preconditions.checkNotNull(maxRegistrationPause);
		this.refusedRegistrationPause = Preconditions.checkNotNull(refusedRegistrationPause);
		this.configuration = new UnmodifiableConfiguration(Preconditions.checkNotNull(configuration));
		this.exitJvmOnOutOfMemory = exitJvmOnOutOfMemory;
		this.classLoaderResolveOrder = classLoaderResolveOrder;
		this.alwaysParentFirstLoaderPatterns = alwaysParentFirstLoaderPatterns;
		this.taskManagerLogPath = taskManagerLogPath;
		this.taskManagerStdoutPath = taskManagerStdoutPath;
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public Time getTimeout() {
		return timeout;
	}

	public Time getMaxRegistrationDuration() {
		return maxRegistrationDuration;
	}

	public Time getInitialRegistrationPause() {
		return initialRegistrationPause;
	}

	public Time getMaxRegistrationPause() {
		return maxRegistrationPause;
	}

	public Time getRefusedRegistrationPause() {
		return refusedRegistrationPause;
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

	public FlinkUserCodeClassLoaders.ResolveOrder getClassLoaderResolveOrder() {
		return classLoaderResolveOrder;
	}

	public String[] getAlwaysParentFirstLoaderPatterns() {
		return alwaysParentFirstLoaderPatterns;
	}

	@Nullable
	public String getTaskManagerLogPath() {
		return taskManagerLogPath;
	}

	@Nullable
	public String getTaskManagerStdoutPath() {
		return taskManagerStdoutPath;
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods
	// --------------------------------------------------------------------------------------------

	public static TaskManagerConfiguration fromConfiguration(Configuration configuration) {
		int numberSlots = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, 1);

		if (numberSlots == -1) {
			numberSlots = 1;
		}

		final String[] tmpDirPaths = ConfigurationUtils.parseTempDirectories(configuration);

		final Time timeout;

		try {
			timeout = Time.milliseconds(AkkaUtils.getTimeout(configuration).toMillis());
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"Invalid format for '" + AkkaOptions.ASK_TIMEOUT.key() +
					"'.Use formats like '50 s' or '1 min' to specify the timeout.");
		}

		LOG.info("Messages have a max timeout of " + timeout);

		final Time finiteRegistrationDuration;

		try {
			Duration maxRegistrationDuration = Duration.create(configuration.getString(TaskManagerOptions.REGISTRATION_TIMEOUT));
			if (maxRegistrationDuration.isFinite()) {
				finiteRegistrationDuration = Time.milliseconds(maxRegistrationDuration.toMillis());
			} else {
				finiteRegistrationDuration = null;
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.REGISTRATION_TIMEOUT.key(), e);
		}

		final Time initialRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF));
			if (pause.isFinite()) {
				initialRegistrationPause = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("The initial registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final Time maxRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				TaskManagerOptions.REGISTRATION_MAX_BACKOFF));
			if (pause.isFinite()) {
				maxRegistrationPause = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("The maximum registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final Time refusedRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(TaskManagerOptions.REFUSED_REGISTRATION_BACKOFF));
			if (pause.isFinite()) {
				refusedRegistrationPause = Time.milliseconds(pause.toMillis());
			} else {
				throw new IllegalArgumentException("The refused registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF.key(), e);
		}

		final boolean exitOnOom = configuration.getBoolean(TaskManagerOptions.KILL_ON_OUT_OF_MEMORY);

		final String classLoaderResolveOrder =
			configuration.getString(CoreOptions.CLASSLOADER_RESOLVE_ORDER);

		final String[] alwaysParentFirstLoaderPatterns = CoreOptions.getParentFirstLoaderPatterns(configuration);

		final String taskManagerLogPath = configuration.getString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, System.getProperty("log.file"));
		final String taskManagerStdoutPath;

		if (taskManagerLogPath != null) {
			final int extension = taskManagerLogPath.lastIndexOf('.');

			if (extension > 0) {
				taskManagerStdoutPath = taskManagerLogPath.substring(0, extension) + ".out";
			} else {
				taskManagerStdoutPath = null;
			}
		} else {
			taskManagerStdoutPath = null;
		}

		return new TaskManagerConfiguration(
			numberSlots,
			tmpDirPaths,
			timeout,
			finiteRegistrationDuration,
			initialRegistrationPause,
			maxRegistrationPause,
			refusedRegistrationPause,
			configuration,
			exitOnOom,
			FlinkUserCodeClassLoaders.ResolveOrder.fromString(classLoaderResolveOrder),
			alwaysParentFirstLoaderPatterns,
			taskManagerLogPath,
			taskManagerStdoutPath);
	}
}
