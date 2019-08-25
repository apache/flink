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

package org.apache.flink.runtime.clusterframework.overlays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_HOME_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_PLUGINS_DIR;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Overlays Flink into a container, based on supplied bin/conf/lib directories.
 *
 * The overlayed Flink is indistinguishable from (and interchangeable with)
 * a normal installation of Flink.  For a docker image-based container, it should be
 * possible to bypass this overlay and rely on the normal installation method.
 *
 * The following files are copied to the container:
 *  - flink/bin/
 *  - flink/conf/
 *  - flink/lib/
 */
public class FlinkDistributionOverlay extends AbstractContainerOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkDistributionOverlay.class);

	static final Path TARGET_ROOT = new Path("flink");

	final File flinkBinPath;
	final File flinkConfPath;
	final File flinkLibPath;
	final File flinkPluginsPath;

	public FlinkDistributionOverlay(File flinkBinPath, File flinkConfPath, File flinkLibPath, File flinkPluginsPath) {
		this.flinkBinPath = checkNotNull(flinkBinPath);
		this.flinkConfPath = checkNotNull(flinkConfPath);
		this.flinkLibPath = checkNotNull(flinkLibPath);
		this.flinkPluginsPath = checkNotNull(flinkPluginsPath);
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {

		container.getEnvironmentVariables().put(ENV_FLINK_HOME_DIR, TARGET_ROOT.toString());

		// add the paths to the container specification.
		addPathRecursively(flinkBinPath, TARGET_ROOT, container);
		addPathRecursively(flinkConfPath, TARGET_ROOT, container);
		addPathRecursively(flinkLibPath, TARGET_ROOT, container);
		if (flinkPluginsPath.isDirectory()) {
			addPathRecursively(flinkPluginsPath, TARGET_ROOT, container);
		}
		else {
			LOG.warn("The plugins directory '" + flinkPluginsPath + "' doesn't exist.");
		}
	}

	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * A builder for the {@link FlinkDistributionOverlay}.
	 */
	public static class Builder {
		File flinkBinPath;
		File flinkConfPath;
		File flinkLibPath;
		File flinkPluginsPath;

		/**
		 * Configures the overlay using the current environment.
		 *
		 * Locates Flink using FLINK_???_DIR environment variables as provided to all Flink processes by config.sh.
		 *
		 * @param globalConfiguration the current configuration.
		 */
		public Builder fromEnvironment(Configuration globalConfiguration) {
			flinkBinPath = getObligatoryFileFromEnvironment(ENV_FLINK_BIN_DIR);
			flinkConfPath = getObligatoryFileFromEnvironment(ENV_FLINK_CONF_DIR);
			flinkLibPath = getObligatoryFileFromEnvironment(ENV_FLINK_LIB_DIR);
			flinkPluginsPath = getObligatoryFileFromEnvironment(ENV_FLINK_PLUGINS_DIR);

			return this;
		}

		public FlinkDistributionOverlay build() {
			return new FlinkDistributionOverlay(flinkBinPath, flinkConfPath, flinkLibPath, flinkPluginsPath);
		}

		private static File getObligatoryFileFromEnvironment(String envVariableName) {
			String directory = System.getenv(envVariableName);
			checkState(directory != null, "the %s environment variable must be set", envVariableName);
			return new File(directory);
		}
	}
}
