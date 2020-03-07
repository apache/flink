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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.util.FlinkRuntimeException;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common utils for Kubernetes.
 */
public class KubernetesUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesUtils.class);

	/**
	 * Read file content to string.
	 *
	 * @param filePath file path
	 * @return content
	 */
	public static String getContentFromFile(String filePath) throws FileNotFoundException {
		File file = new File(filePath);
		if (file.exists()) {
			StringBuilder content = new StringBuilder();
			String line;
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))){
				while ((line = reader.readLine()) != null) {
					content.append(line).append(System.lineSeparator());
				}
			} catch (IOException e) {
				throw new RuntimeException("Error read file content.", e);
			}
			return content.toString();
		}
		throw new FileNotFoundException("File " + filePath + " not exists.");
	}

	/**
	 * Check whether the port config option is a fixed port. If not, the fallback port will be set to configuration.
	 * @param flinkConfig flink configuration
	 * @param port config option need to be checked
	 * @param fallbackPort the fallback port that will be set to the configuration
	 */
	public static void checkAndUpdatePortConfigOption(
			Configuration flinkConfig,
			ConfigOption<String> port,
			int fallbackPort) {
		if (KubernetesUtils.parsePort(flinkConfig, port) == 0) {
			flinkConfig.setString(port, String.valueOf(fallbackPort));
			LOG.info(
				"Kubernetes deployment requires a fixed port. Configuration {} will be set to {}",
				port.key(),
				fallbackPort);
		}
	}

	/**
	 * Parse a valid port for the config option. A fixed port is expected, and do not support a range of ports.
	 *
	 * @param flinkConfig flink config
	 * @param port port config option
	 * @return valid port
	 */
	public static Integer parsePort(Configuration flinkConfig, ConfigOption<String> port) {
		checkNotNull(flinkConfig.get(port), port.key() + " should not be null.");

		try {
			return Integer.parseInt(flinkConfig.get(port));
		} catch (NumberFormatException ex) {
			throw new FlinkRuntimeException(
				port.key() + " should be specified to a fixed port. Do not support a range of ports.",
				ex);
		}
	}

	/**
	 * Generate name of the internal Service.
	 */
	public static String getInternalServiceName(String clusterId) {
		return clusterId;
	}

	/**
	 * Generate name of the external Service.
	 */
	public static String getRestServiceName(String clusterId) {
		return clusterId + Constants.FLINK_REST_SERVICE_SUFFIX;
	}

	/**
	 * Generate name of the Deployment.
	 */
	public static String getDeploymentName(String clusterId) {
		return clusterId;
	}

	/**
	 * Get task manager labels for the current Flink cluster. They could be used to watch the pods status.
	 *
	 * @return Task manager labels.
	 */
	public static Map<String, String> getTaskManagerLabels(String clusterId) {
		final Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, clusterId);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		return labels;
	}

	/**
	 * Get resource requirements from memory and cpu.
	 *
	 * @param mem Memory in mb.
	 * @param cpu cpu.
	 * @return KubernetesResource requirements.
	 */
	public static ResourceRequirements getResourceRequirements(int mem, double cpu) {
		final Quantity cpuQuantity = new Quantity(String.valueOf(cpu));
		final Quantity memQuantity = new Quantity(mem + Constants.RESOURCE_UNIT_MB);

		return new ResourceRequirementsBuilder()
			.addToRequests(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToRequests(Constants.RESOURCE_NAME_CPU, cpuQuantity)
			.addToLimits(Constants.RESOURCE_NAME_MEMORY, memQuantity)
			.addToLimits(Constants.RESOURCE_NAME_CPU, cpuQuantity)
			.build();
	}

	public static String getCommonStartCommand(
			Configuration flinkConfig,
			ClusterComponent mode,
			String jvmMemOpts,
			String configDirectory,
			String logDirectory,
			boolean hasLogback,
			boolean hasLog4j,
			String mainClass,
			@Nullable String mainArgs) {
		final Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("classpath", "-classpath " + "$" + Constants.ENV_FLINK_CLASSPATH);

		startCommandValues.put("jvmmem", jvmMemOpts);

		final String opts;
		final String logFileName;
		if (mode == ClusterComponent.JOB_MANAGER) {
			opts = getJavaOpts(flinkConfig, CoreOptions.FLINK_JM_JVM_OPTIONS);
			logFileName = "jobmanager";
		} else {
			opts = getJavaOpts(flinkConfig, CoreOptions.FLINK_TM_JVM_OPTIONS);
			logFileName = "taskmanager";
		}
		startCommandValues.put("jvmopts", opts);

		startCommandValues.put("logging",
			getLogging(logDirectory + "/" + logFileName + ".log", configDirectory, hasLogback, hasLog4j));

		startCommandValues.put("class", mainClass);

		startCommandValues.put("args", mainArgs != null ? mainArgs : "");

		startCommandValues.put("redirects",
			"1> " + logDirectory + "/" + logFileName + ".out " +
			"2> " + logDirectory + "/" + logFileName + ".err");

		final String commandTemplate = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_START_COMMAND_TEMPLATE);
		return BootstrapTools.getStartCommand(commandTemplate, startCommandValues);
	}

	private static String getJavaOpts(Configuration flinkConfig, ConfigOption<String> configOption) {
		String baseJavaOpts = flinkConfig.getString(CoreOptions.FLINK_JVM_OPTIONS);

		if (flinkConfig.getString(configOption).length() > 0) {
			return baseJavaOpts + " " + flinkConfig.getString(configOption);
		} else {
			return baseJavaOpts;
		}
	}

	private static String getLogging(String logFile, String confDir, boolean hasLogback, boolean hasLog4j) {
		StringBuilder logging = new StringBuilder();
		if (hasLogback || hasLog4j) {
			logging.append("-Dlog.file=").append(logFile);
			if (hasLogback) {
				logging.append(" -Dlogback.configurationFile=file:").append(confDir).append("/logback.xml");
			}
			if (hasLog4j) {
				logging.append(" -Dlog4j.configurationFile=file:").append(confDir).append("/log4j.properties");
			}
		}
		return logging.toString();
	}

	/**
	 * Cluster components.
	 */
	public enum ClusterComponent {
		JOB_MANAGER,
		TASK_MANAGER
	}

	private KubernetesUtils() {}
}
