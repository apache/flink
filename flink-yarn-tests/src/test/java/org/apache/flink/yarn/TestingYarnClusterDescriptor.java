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
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * Yarn client which starts a {@link TestingApplicationMaster}. Additionally the client adds the
 * flink-yarn-tests-X-tests.jar and the flink-runtime-X-tests.jar to the set of files which
 * are shipped to the yarn cluster. This is necessary to load the testing classes.
 */
public class TestingYarnClusterDescriptor extends AbstractYarnClusterDescriptor {

	public TestingYarnClusterDescriptor(Configuration configuration, String configurationDirectory) {
		super(
			configuration,
			configurationDirectory,
			YarnClient.createYarnClient());
		List<File> filesToShip = new ArrayList<>();

		File testingJar = YarnTestBase.findFile("..", new TestJarFinder("flink-yarn-tests"));
		Preconditions.checkNotNull(testingJar, "Could not find the flink-yarn-tests tests jar. " +
			"Make sure to package the flink-yarn-tests module.");

		File testingRuntimeJar = YarnTestBase.findFile("..", new TestJarFinder("flink-runtime"));
		Preconditions.checkNotNull(testingRuntimeJar, "Could not find the flink-runtime tests " +
			"jar. Make sure to package the flink-runtime module.");

		File testingYarnJar = YarnTestBase.findFile("..", new TestJarFinder("flink-yarn"));
		Preconditions.checkNotNull(testingRuntimeJar, "Could not find the flink-yarn tests " +
			"jar. Make sure to package the flink-yarn module.");

		filesToShip.add(testingJar);
		filesToShip.add(testingRuntimeJar);
		filesToShip.add(testingYarnJar);

		addShipFiles(filesToShip);
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
	public YarnClusterClient deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph) {
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
