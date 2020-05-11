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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.externalresource.ExternalResourceDriver;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Driver takes the responsibility to discover GPU resources and provide the GPU resource information.
 * It retrieves the GPU information by executing a user-defined discovery script.
 */
class GPUDriver implements ExternalResourceDriver {

	private static final Logger LOG = LoggerFactory.getLogger(GPUDriver.class);

	private static final long DISCOVERY_SCRIPT_TIMEOUT_MS = 10000;

	@VisibleForTesting
	static final ConfigOption<String> DISCOVERY_SCRIPT_PATH =
		key("discovery-script.path")
			.stringType()
			.defaultValue(String.format("%s/external-resource-gpu/nvidia-gpu-discovery.sh", ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS));

	@VisibleForTesting
	static final ConfigOption<String> DISCOVERY_SCRIPT_ARG =
		key("discovery-script.args")
			.stringType()
			.noDefaultValue();

	private final File discoveryScriptFile;
	private final String args;

	GPUDriver(Configuration config) throws Exception {
		final String discoveryScriptPathStr = config.getString(DISCOVERY_SCRIPT_PATH);
		if (StringUtils.isNullOrWhitespaceOnly(discoveryScriptPathStr)) {
			throw new IllegalConfigurationException(
				String.format("GPU discovery script ('%s') is not configured.", ExternalResourceOptions.genericKeyWithSuffix(DISCOVERY_SCRIPT_PATH.key())));
		}

		Path discoveryScriptPath = Paths.get(discoveryScriptPathStr);
		if (!discoveryScriptPath.isAbsolute()) {
			discoveryScriptPath = Paths.get(System.getenv().getOrDefault(ConfigConstants.ENV_FLINK_HOME_DIR, "."), discoveryScriptPathStr);
		}
		discoveryScriptFile = discoveryScriptPath.toFile();

		if (!discoveryScriptFile.exists()) {
			throw new FileNotFoundException(String.format("The gpu discovery script does not exist in path %s.", discoveryScriptFile.getAbsolutePath()));
		}
		if (!discoveryScriptFile.canExecute()) {
			throw new FlinkException(String.format("The discovery script %s is not executable.", discoveryScriptFile.getAbsolutePath()));
		}

		args = config.getString(DISCOVERY_SCRIPT_ARG);
	}

	@Override
	public Set<GPUInfo> retrieveResourceInfo(long gpuAmount) throws Exception {
		Preconditions.checkArgument(gpuAmount > 0, "The gpuAmount should be positive when retrieving the GPU resource information.");

		final Set<GPUInfo> gpuResources = new HashSet<>();
		String output = executeDiscoveryScript(discoveryScriptFile, gpuAmount, args);
		if (!output.isEmpty()) {
			String[] indexes = output.split(",");
			for (String index : indexes) {
				if (!StringUtils.isNullOrWhitespaceOnly(index)) {
					gpuResources.add(new GPUInfo(index.trim()));
				}
			}
		}
		LOG.info("Discover GPU resources: {}.", gpuResources);
		return Collections.unmodifiableSet(gpuResources);
	}

	private String executeDiscoveryScript(File discoveryScript, long gpuAmount, String args) throws Exception {
		final String cmd = discoveryScript.getAbsolutePath() + " " + gpuAmount + " " + args;
		final Process process = Runtime.getRuntime().exec(cmd);
		try (final BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			final BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
			final boolean hasProcessTerminated = process.waitFor(DISCOVERY_SCRIPT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
			if (!hasProcessTerminated) {
				throw new TimeoutException(String.format("The discovery script executed for over %d ms.", DISCOVERY_SCRIPT_TIMEOUT_MS));
			}

			final int exitVal = process.exitValue();
			if (exitVal != 0) {
				final String stdout = stdoutReader.lines().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
				final String stderr = stderrReader.lines().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
				LOG.warn("Discovery script exit with {}.\\nSTDOUT: {}\\nSTDERR: {}", exitVal, stdout, stderr);
				throw new FlinkException(String.format("Discovery script exit with non-zero return code: %s.", exitVal));
			}
			Object[] stdout = stdoutReader.lines().toArray();
			if (stdout.length > 1) {
				LOG.warn(
					"The output of the discovery script should only contain one single line. Finding {} lines with content: {}. Will only keep the first line.", stdout.length, Arrays.toString(stdout));
			}
			if (stdout.length == 0) {
				return "";
			}
			return (String) stdout[0];
		} finally {
			process.destroyForcibly();
		}
	}
}
