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
import org.apache.flink.runtime.clusterframework.ContainerSpecification;
import org.apache.flink.runtime.entrypoint.component.AbstractUserClassPathJobGraphRetriever;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;


/**
 * Overlays Flink and Job into a container, based on supplied bin/conf/lib/plugins/job directories.
 * The following files are copied to the container:
 *  - bin/
 *  - conf/
 *  - lib/
 *  - plugins/
 *  - job/
 */
public class FlinkJobOverlay extends FlinkDistributionOverlay {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJobOverlay.class);

	private final File jobPath;

	FlinkJobOverlay(File flinkBinPath, File flinkConfPath,
					File flinkLibPath, File flinkPluginsPath, File jobPath) {
		super(flinkBinPath, flinkConfPath, flinkLibPath, flinkPluginsPath);
		this.jobPath = jobPath;
	}

	@Override
	public void configure(ContainerSpecification container) throws IOException {
		super.configure(container);
		if (jobPath.isDirectory()) {
			addPathRecursively(jobPath, TARGET_ROOT, container);
		} else {
			LOG.warn("The job directory '" + jobPath + "' doesn't exist.");
		}
	}

	public static FlinkJobOverlay.Builder newBuilder() {
		return new FlinkJobOverlay.Builder();
	}

	/**
	 * A builder for the {@link FlinkJobOverlay}.
	 */
	public static class Builder extends FlinkDistributionOverlay.Builder {
		File jobPath;

		/**
		 * Configures the overlay using the current environment.
		 *
		 * @param globalConfiguration the current configuration.
		 */
		public FlinkJobOverlay.Builder fromEnvironment(Configuration globalConfiguration) {
			super.fromEnvironment(globalConfiguration);
			jobPath = new File(
				Paths.get(AbstractUserClassPathJobGraphRetriever.DEFAULT_JOB_DIR).toAbsolutePath().toUri());
			return this;
		}

		public FlinkJobOverlay build() {
			return new FlinkJobOverlay(flinkBinPath, flinkConfPath, flinkLibPath, flinkPluginsPath, jobPath);
		}

	}
}
