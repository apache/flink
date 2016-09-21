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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.io.File;

/**
 * Configuration object for {@link TaskExecutor}.
 */
public class TaskManagerConfiguration {

	private static final Logger LOG = LoggerFactory.getLogger(TaskManagerConfiguration.class);

	private final int numberSlots;

	private final String[] tmpDirPaths;

	private final Time timeout;
	private final Time maxRegistrationDuration;
	private final Time initialRegistrationPause;
	private final Time maxRegistrationPause;
	private final Time refusedRegistrationPause;

	private final long cleanupInterval;

	public TaskManagerConfiguration(
		int numberSlots,
		String[] tmpDirPaths,
		Time timeout,
		Time maxRegistrationDuration,
		Time initialRegistrationPause,
		Time maxRegistrationPause,
		Time refusedRegistrationPause,
		long cleanupInterval) {

		this.numberSlots = numberSlots;
		this.tmpDirPaths = Preconditions.checkNotNull(tmpDirPaths);
		this.timeout = Preconditions.checkNotNull(timeout);
		this.maxRegistrationDuration = Preconditions.checkNotNull(maxRegistrationDuration);
		this.initialRegistrationPause = Preconditions.checkNotNull(initialRegistrationPause);
		this.maxRegistrationPause = Preconditions.checkNotNull(maxRegistrationPause);
		this.refusedRegistrationPause = Preconditions.checkNotNull(refusedRegistrationPause);
		this.cleanupInterval = Preconditions.checkNotNull(cleanupInterval);
	}

	public int getNumberSlots() {
		return numberSlots;
	}

	public String[] getTmpDirPaths() {
		return tmpDirPaths;
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

	public long getCleanupInterval() {
		return cleanupInterval;
	}

	// --------------------------------------------------------------------------------------------
	//  Static factory methods
	// --------------------------------------------------------------------------------------------

	public static TaskManagerConfiguration fromConfiguration(Configuration configuration) {
		int numberSlots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);

		if (numberSlots == -1) {
			numberSlots = 1;
		}

		final String[] tmpDirPaths = configuration.getString(
			ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator);

		final Time timeout;

		try {
			timeout = Time.milliseconds(AkkaUtils.getTimeout(configuration).toMillis());
		} catch (Exception e) {
			throw new IllegalArgumentException(
				"Invalid format for '" + ConfigConstants.AKKA_ASK_TIMEOUT +
					"'.Use formats like '50 s' or '1 min' to specify the timeout.");
		}

		LOG.info("Messages have a max timeout of " + timeout);

		final long cleanupInterval = configuration.getLong(
			ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
			ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000;

		final Time finiteRegistrationDuration;

		try {
			Duration maxRegistrationDuration = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
				ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION));
			if (maxRegistrationDuration.isFinite()) {
				finiteRegistrationDuration = Time.seconds(maxRegistrationDuration.toSeconds());
			} else {
				finiteRegistrationDuration = null;
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION, e);
		}

		final Time initialRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_INITIAL_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				initialRegistrationPause = Time.seconds(pause.toSeconds());
			} else {
				throw new IllegalArgumentException("The initial registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		final Time maxRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_MAX_REGISTARTION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				maxRegistrationPause = Time.seconds(pause.toSeconds());
			} else {
				throw new IllegalArgumentException("The maximum registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		final Time refusedRegistrationPause;
		try {
			Duration pause = Duration.create(configuration.getString(
				ConfigConstants.TASK_MANAGER_REFUSED_REGISTRATION_PAUSE,
				ConfigConstants.DEFAULT_TASK_MANAGER_REFUSED_REGISTRATION_PAUSE));
			if (pause.isFinite()) {
				refusedRegistrationPause = Time.seconds(pause.toSeconds());
			} else {
				throw new IllegalArgumentException("The refused registration pause must be finite: " + pause);
			}
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid format for parameter " +
				ConfigConstants.TASK_MANAGER_INITIAL_REGISTRATION_PAUSE, e);
		}

		return new TaskManagerConfiguration(
			numberSlots,
			tmpDirPaths,
			timeout,
			finiteRegistrationDuration,
			initialRegistrationPause,
			maxRegistrationPause,
			refusedRegistrationPause,
			cleanupInterval);
	}
}
