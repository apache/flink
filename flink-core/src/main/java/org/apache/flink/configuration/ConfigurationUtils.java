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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS;
import static org.apache.flink.configuration.MetricOptions.SYSTEM_RESOURCE_METRICS_PROBING_INTERVAL;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for {@link Configuration} related helper functions.
 */
public class ConfigurationUtils {

	private static final String[] EMPTY = new String[0];

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

	/**
	 * Replaces values whose keys are sensitive according to {@link GlobalConfiguration#isSensitive(String)}
	 * with {@link GlobalConfiguration#HIDDEN_CONTENT}.
	 *
	 * <p>This can be useful when displaying configuration values.
	 *
	 * @param keyValuePairs for which to hide sensitive values
	 * @return A map where all sensitive value are hidden
	 */
	@Nonnull
	public static Map<String, String> hideSensitiveValues(Map<String, String> keyValuePairs) {
		final HashMap<String, String> result = new HashMap<>();

		for (Map.Entry<String, String> keyValuePair : keyValuePairs.entrySet()) {
			if (GlobalConfiguration.isSensitive(keyValuePair.getKey())) {
				result.put(keyValuePair.getKey(), GlobalConfiguration.HIDDEN_CONTENT);
			} else {
				result.put(keyValuePair.getKey(), keyValuePair.getValue());
			}
		}

		return result;
	}

	@Nonnull
	public static String[] splitPaths(@Nonnull String separatedPaths) {
		return separatedPaths.length() > 0 ? separatedPaths.split(",|" + File.pathSeparator) : EMPTY;
	}

	@VisibleForTesting
	public static Map<String, String> parseTmResourceDynamicConfigs(String dynamicConfigsStr) {
		Map<String, String> configs = new HashMap<>();
		String[] configStrs = dynamicConfigsStr.split(" ");

		checkArgument(
			configStrs.length % 2 == 0,
			"Dynamic option string contained odd number of arguments: #arguments=%s, (%s)", configStrs.length, dynamicConfigsStr);
		for (int i = 0; i < configStrs.length; ++i) {
			String configStr = configStrs[i];
			if (i % 2 == 0) {
				checkArgument(configStr.equals("-D"));
			} else {
				String[] configKV = configStr.split("=");
				checkArgument(configKV.length == 2);
				configs.put(configKV[0], configKV[1]);
			}
		}

		checkConfigContains(configs, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key());
		checkConfigContains(configs, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key());
		checkConfigContains(configs, TaskManagerOptions.TASK_HEAP_MEMORY.key());
		checkConfigContains(configs, TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key());
		checkConfigContains(configs, TaskManagerOptions.NETWORK_MEMORY_MAX.key());
		checkConfigContains(configs, TaskManagerOptions.NETWORK_MEMORY_MIN.key());
		checkConfigContains(configs, TaskManagerOptions.MANAGED_MEMORY_SIZE.key());

		return configs;
	}

	private static void checkConfigContains(Map<String, String> configs, String key) {
		checkArgument(configs.containsKey(key), "Key %s is missing present from dynamic configs.", key);
	}

	@VisibleForTesting
	public static Map<String, String> parseJvmArgString(String jvmParamsStr) {
		final String xmx = "-Xmx";
		final String xms = "-Xms";
		final String maxDirect = "-XX:MaxDirectMemorySize=";
		final String maxMetadata = "-XX:MaxMetaspaceSize=";

		Map<String, String> configs = new HashMap<>();
		for (String paramStr : jvmParamsStr.split(" ")) {
			if (paramStr.startsWith(xmx)) {
				configs.put(xmx, paramStr.substring(xmx.length()));
			} else if (paramStr.startsWith(xms)) {
				configs.put(xms, paramStr.substring(xms.length()));
			} else if (paramStr.startsWith(maxDirect)) {
				configs.put(maxDirect, paramStr.substring(maxDirect.length()));
			} else if (paramStr.startsWith(maxMetadata)) {
				configs.put(maxMetadata, paramStr.substring(maxMetadata.length()));
			}
		}

		checkArgument(configs.containsKey(xmx));
		checkArgument(configs.containsKey(xms));
		checkArgument(configs.containsKey(maxMetadata));

		return configs;
	}

	/**
	 * Extract and parse Flink configuration properties with a given name prefix and
	 * return the result as a Map.
	 */
	public static Map<String, String> getPrefixedKeyValuePairs(String prefix, Configuration configuration) {
		Map<String, String> result  = new HashMap<>();
		for (Map.Entry<String, String> entry: configuration.toMap().entrySet()) {
			if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
				String key = entry.getKey().substring(prefix.length());
				result.put(key, entry.getValue());
			}
		}
		return result;
	}

	// Make sure that we cannot instantiate this class
	private ConfigurationUtils() {
	}
}
