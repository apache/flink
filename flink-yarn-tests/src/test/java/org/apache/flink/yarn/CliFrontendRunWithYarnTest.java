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

import org.apache.flink.client.cli.CliFrontendTestBase;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.TestingClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.util.NonDeployingYarnClusterDescriptor;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.flink.client.cli.CliFrontendRunTest.verifyCliFrontend;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.yarn.util.YarnTestUtils.getTestJarPath;

/**
 * Tests for the RUN command using a {@link org.apache.flink.yarn.cli.FlinkYarnSessionCli} inside
 * the {@link org.apache.flink.client.cli.CliFrontend}.
 *
 * @see org.apache.flink.client.cli.CliFrontendRunTest
 */
public class CliFrontendRunWithYarnTest extends CliFrontendTestBase {

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Test
	public void testRun() throws Exception {
		String testJarPath = getTestJarPath("BatchWordCount.jar").getAbsolutePath();

		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setInteger(JobManagerOptions.PORT, 8081);
		configuration.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, "1g");

		final ClusterClientServiceLoader testServiceLoader =
			new TestingYarnClusterClientServiceLoader(new TestingClusterClient<>());

		final FlinkYarnSessionCli yarnCLI = new FlinkYarnSessionCli(
			configuration,
			testServiceLoader,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn",
			true);

		// test detached mode
		{
			String[] parameters = {"-m", "yarn-cluster", "-p", "2", "-d", testJarPath};
			verifyCliFrontend(testServiceLoader, yarnCLI, parameters, 2, true);
		}

		// test detached mode
		{
			String[] parameters = {"-m", "yarn-cluster", "-p", "2", "-yd", testJarPath};
			verifyCliFrontend(testServiceLoader, yarnCLI, parameters, 2, true);
		}
	}

	private static class TestingYarnClusterClientServiceLoader implements ClusterClientServiceLoader {

		private final ClusterClient<ApplicationId> clusterClient;

		TestingYarnClusterClientServiceLoader(ClusterClient<ApplicationId> clusterClient) {
			this.clusterClient = checkNotNull(clusterClient);
		}

		@Override
		public ClusterClientFactory<ApplicationId> getClusterClientFactory(Configuration configuration) {
			return new TestingYarnClusterClientFactory(clusterClient);
		}
	}

	private static class TestingYarnClusterClientFactory extends YarnClusterClientFactory {

		private final ClusterClient<ApplicationId> clusterClient;

		TestingYarnClusterClientFactory(ClusterClient<ApplicationId> clusterClient) {
			this.clusterClient = checkNotNull(clusterClient);
		}

		@Override
		public YarnClusterDescriptor createClusterDescriptor(Configuration configuration) {
			YarnClusterDescriptor parent = super.createClusterDescriptor(configuration);
			return new NonDeployingYarnClusterDescriptor(
					parent.getFlinkConfiguration(),
					(YarnConfiguration) parent.getYarnClient().getConfig(),
					parent.getYarnClient(),
					clusterClient);
		}
	}
}
