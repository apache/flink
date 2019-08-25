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
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.flink.yarn.util.FakeClusterClient;
import org.apache.flink.yarn.util.NonDeployingYarnClusterDescriptor;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.client.cli.CliFrontendRunTest.verifyCliFrontend;
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

		FlinkYarnSessionCli yarnCLI = new TestingFlinkYarnSessionCli(
			configuration,
			tmp.getRoot().getAbsolutePath(),
			"y",
			"yarn");

		// test detached mode
		{
			String[] parameters = {"-m", "yarn-cluster", "-yn", "1", "-p", "2", "-d", testJarPath};
			verifyCliFrontend(yarnCLI, parameters, 2, true, true);
		}

		// test detached mode
		{
			String[] parameters = {"-m", "yarn-cluster", "-yn", "1", "-p", "2", "-yd", testJarPath};
			verifyCliFrontend(yarnCLI, parameters, 2, true, true);
		}
	}

	private static class TestingFlinkYarnSessionCli extends FlinkYarnSessionCli {
		@SuppressWarnings("unchecked")
		private final ClusterClient<ApplicationId> clusterClient;
		private final String configurationDirectory;

		private TestingFlinkYarnSessionCli(
				Configuration configuration,
				String configurationDirectory,
				String shortPrefix,
				String longPrefix) throws Exception {
			super(configuration, configurationDirectory, shortPrefix, longPrefix);

			this.clusterClient = new FakeClusterClient(configuration);
			this.configurationDirectory = configurationDirectory;
		}

		@Override
		public AbstractYarnClusterDescriptor createClusterDescriptor(CommandLine commandLine)
			throws FlinkException {
			AbstractYarnClusterDescriptor parent = super.createClusterDescriptor(commandLine);
			return new NonDeployingDetachedYarnClusterDescriptor(
					parent.getFlinkConfiguration(),
					(YarnConfiguration) parent.getYarnClient().getConfig(),
					configurationDirectory,
					parent.getYarnClient(),
					clusterClient);
		}
	}

	private static class NonDeployingDetachedYarnClusterDescriptor extends NonDeployingYarnClusterDescriptor {

		NonDeployingDetachedYarnClusterDescriptor(
			Configuration flinkConfiguration,
			YarnConfiguration yarnConfiguration, String configurationDirectory,
			YarnClient yarnClient,
			ClusterClient<ApplicationId> clusterClient) {
			super(flinkConfiguration, yarnConfiguration, configurationDirectory, yarnClient,
				clusterClient);
		}

		@Override
		public ClusterClient<ApplicationId> deployJobCluster(
				ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
			assertTrue(detached);
			return super.deployJobCluster(clusterSpecification, jobGraph, true);
		}
	}

}
