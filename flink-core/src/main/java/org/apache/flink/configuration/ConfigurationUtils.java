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

package org.apache.flink.configuration;

import org.apache.flink.api.common.time.Time;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL;

/**
 * Utility class for {@link Configuration} related helper functions.
 */
public class ConfigurationUtils {

	private static final String[] EMPTY = new String[0];

	/**
	 * Get job manager's heap memory. This method will check the new key
	 * {@link JobManagerOptions#JOB_MANAGER_HEAP_MEMORY} and
	 * the old key {@link JobManagerOptions#JOB_MANAGER_HEAP_MEMORY_MB} for backwards compatibility.
	 *
	 * @param configuration the configuration object
	 * @return the memory size of job manager's heap memory.
	 */
	public static MemorySize getJobManagerHeapMemory(Configuration configuration) {
		if (configuration.containsKey(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key())) {
			return MemorySize.parse(configuration.getString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY));
		} else if (configuration.containsKey(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB.key())) {
			return MemorySize.parse(configuration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB) + "m");
		} else {
			//use default value
			return MemorySize.parse(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.defaultValue());
		}
	}

	/**
	 * Get task manager's heap memory. This method will check the new key
	 * {@link TaskManagerOptions#TASK_MANAGER_HEAP_MEMORY} and
	 * the old key {@link TaskManagerOptions#TASK_MANAGER_HEAP_MEMORY_MB} for backwards compatibility.
	 *
	 * @param configuration the configuration object
	 * @return the memory size of task manager's heap memory.
	 */
	public static MemorySize getTaskManagerHeapMemory(Configuration configuration) {
		if (configuration.containsKey(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY.key())) {
			return MemorySize.parse(configuration.getString(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY));
		} else if (configuration.containsKey(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB.key())) {
			return MemorySize.parse(configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB) + "m");
		} else {
			//use default value
			return MemorySize.parse(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY.defaultValue());
		}
	}

	/**
	 * @return extracted {@link MetricOptions#SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL} or {@code Optional.empty()} if
	 * {@link MetricOptions#SYSTEM_RESOURCE_METRICS} are disabled.
	 */
	public static Optional<Time> getSystemResourceMetricsProbingInterval(Configuration configuration) {
		if (!configuration.getBoolean(SYSTEM_RESOURCE_METRICS)) {
			return Optional.empty();
		} else {
			return Optional.of(Time.milliseconds(
				configuration.getLong(SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL)));
		}
	}

	/**
	 * Extracts the task manager directories for temporary files as defined by
	 * {@link org.apache.flink.configuration.CoreOptions#TMP_DIRS}.
	 *
	 * @param configuration configuration object
	 * @return array of configured directories (in order)
	 */
	@Nonnull
	public static String[] parseTempDirectories(Configuration configuration) {
		return splitPaths(configuration.getString(CoreOptions.TMP_DIRS));
	}

	/**
	 * Extracts the local state directories  as defined by
	 * {@link CheckpointingOptions#LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS}.
	 *
	 * @param configuration configuration object
	 * @return array of configured directories (in order)
	 */
	@Nonnull
	public static String[] parseLocalStateDirectories(Configuration configuration) {
		String configValue = configuration.getString(CheckpointingOptions.LOCAL_RECOVERY_TASK_MANAGER_STATE_ROOT_DIRS, "");
		return splitPaths(configValue);
	}

	public static Time getStandaloneClusterStartupPeriodTime(Configuration configuration) {
		final Time timeout;
		long standaloneClusterStartupPeriodTime = configuration.getLong(ResourceManagerOptions.STANDALONE_CLUSTER_STARTUP_PERIOD_TIME);
		if (standaloneClusterStartupPeriodTime >= 0) {
			timeout = Time.milliseconds(standaloneClusterStartupPeriodTime);
		} else {
			timeout = Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));
		}
		return timeout;
	}

	/**
	 * Creates a new {@link Configuration} from the given {@link Properties}.
	 *
	 * @param properties to convert into a {@link Configuration}
	 * @return {@link Configuration} which has been populated by the values of the given {@link Properties}
	 */
	@Nonnull
	public static Configuration createConfiguration(Properties properties) {
		final Configuration configuration = new Configuration();

		final Set<String> propertyNames = properties.stringPropertyNames();

		for (String propertyName : propertyNames) {
			configuration.setString(propertyName, properties.getProperty(propertyName));
		}

		return configuration;
	}

	@Nonnull
	private static String[] splitPaths(@Nonnull String separatedPaths) {
		return separatedPaths.length() > 0 ? separatedPaths.split(",|" + File.pathSeparator) : EMPTY;
	}

	// Make sure that we cannot instantiate this class
	private ConfigurationUtils() {
	}
}
