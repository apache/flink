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

package org.apache.flink.yarn;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Yarn client which starts a {@link TestingApplicationMaster}. Additionally the client adds the
 * flink-yarn-tests-X-tests.jar and the flink-runtime-X-tests.jar to the set of files which
 * are shipped to the yarn cluster. This is necessary to load the testing classes.
 */
public class TestingYarnClusterDescriptor extends YarnClusterDescriptor {

	public TestingYarnClusterDescriptor(Configuration configuration, String configurationDirectory) {
		super(
			configuration,
			configurationDirectory,
			YarnClient.createYarnClient());
	}

	@Override
	protected String getYarnSessionClusterEntrypoint() {
		return TestingApplicationMaster.class.getName();
	}

	@Override
	protected String getYarnJobClusterEntrypoint() {
		throw new UnsupportedOperationException("Does not support Yarn per-job clusters.");
	}

	@Override
	public YarnClusterClient deployJobCluster(
			ClusterSpecification clusterSpecification,
			JobGraph jobGraph,
			boolean detached) {
		throw new UnsupportedOperationException("Cannot deploy a per-job cluster yet.");
	}

	static class TestJarFinder implements FilenameFilter {

		private final String jarName;

		TestJarFinder(final String jarName) {
			this.jarName = jarName;
		}

		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith(jarName) && name.endsWith("-tests.jar") &&
				dir.getAbsolutePath().contains(dir.separator + jarName + dir.separator);
		}
	}
}
