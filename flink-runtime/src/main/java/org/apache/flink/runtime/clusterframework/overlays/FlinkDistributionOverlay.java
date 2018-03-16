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
import java.util.Map;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_BIN_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_HOME_DIR;
import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.util.Preconditions.checkNotNull;

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

	public FlinkDistributionOverlay(File flinkBinPath, File flinkConfPath, File flinkLibPath) {
		this.flinkBinPath = checkNotNull(flinkBinPath);
		this.flinkConfPath = checkNotNull(flinkConfPath);
		this.flinkLibPath = checkNotNull(flinkLibPath);
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {

		container.getEnvironmentVariables().put(ENV_FLINK_HOME_DIR, TARGET_ROOT.toString());

		// add the paths to the container specification.
		addPathRecursively(flinkBinPath, TARGET_ROOT, container);
		addPathRecursively(flinkConfPath, TARGET_ROOT, container);
		addPathRecursively(flinkLibPath, TARGET_ROOT, container);
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

		/**
		 * Configures the overlay using the current environment.
		 *
		 * Locates Flink using FLINK_???_DIR environment variables as provided to all Flink processes by config.sh.
		 *
		 * @param globalConfiguration the current configuration.
		 */
		public Builder fromEnvironment(Configuration globalConfiguration) {

			Map<String,String> env = System.getenv();
			if(env.containsKey(ENV_FLINK_BIN_DIR)) {
				flinkBinPath = new File(System.getenv(ENV_FLINK_BIN_DIR));
			}
			else {
				throw new IllegalStateException(String.format("the %s environment variable must be set", ENV_FLINK_BIN_DIR));
			}

			if(env.containsKey(ENV_FLINK_CONF_DIR)) {
				flinkConfPath = new File(System.getenv(ENV_FLINK_CONF_DIR));
			}
			else {
				throw new IllegalStateException(String.format("the %s environment variable must be set", ENV_FLINK_CONF_DIR));
			}

			if(env.containsKey(ENV_FLINK_LIB_DIR)) {
				flinkLibPath = new File(System.getenv(ENV_FLINK_LIB_DIR));
			}
			else {
				throw new IllegalStateException(String.format("the %s environment variable must be set", ENV_FLINK_LIB_DIR));
			}

			return this;
		}

		public FlinkDistributionOverlay build() {
			return new FlinkDistributionOverlay(flinkBinPath, flinkConfPath, flinkLibPath);
		}
	}
}
