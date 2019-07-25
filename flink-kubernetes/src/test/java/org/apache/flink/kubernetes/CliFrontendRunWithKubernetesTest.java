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

package org.apache.flink.kubernetes;

import org.apache.flink.client.cli.CliFrontendTestBase;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.cli.KubernetesCustomCli;
import org.apache.flink.kubernetes.cluster.KubernetesClusterDescriptor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;

import org.apache.commons.cli.CommandLine;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import static org.apache.flink.client.cli.CliFrontendRunTest.verifyCliFrontend;

/**
 * To test kubernetes related commands started from CliFrontend, such as 'bin/flink run -k8s..'.
 */
public class CliFrontendRunWithKubernetesTest extends CliFrontendTestBase {

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
		String testJarPath = "/Users/tianchen/Workspace/flink/flink-clients/target/maven-test-jar.jar";

		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.ADDRESS, "localhost");
		configuration.setInteger(JobManagerOptions.PORT, 8081);

		KubernetesCustomCli cli = new TestingKubernetesCustomCli(configuration);

		{
			String[] parameters = {"-m", "kubernetes-cluster", "-p", "2", "-d", testJarPath};
			verifyCliFrontend(cli, parameters, 2, true, true);
		}
	}

	private static class TestingKubernetesCustomCli extends KubernetesCustomCli {

		public TestingKubernetesCustomCli(Configuration configuration) {
			super(configuration, "");
		}

		@Override
		public ClusterDescriptor<String> createClusterDescriptor(CommandLine commandLine) throws
			FlinkException {
			try {
				FlinkKubernetesOptions flinkOptions = this.fromCommandLine(commandLine);
				Configuration tmpConfig = flinkOptions.getConfiguration();
				flinkOptions.getConfiguration().addAll(this.configuration);
				flinkOptions.getConfiguration().addAll(tmpConfig);

				return new TestingKubernetesClusterDescriptor(flinkOptions);
			} catch (Exception e) {
				throw new FlinkException("Could not create the KubernetesClusterDescriptor.", e);
			}
		}
	}

	private static class TestingKubernetesClusterDescriptor extends KubernetesClusterDescriptor {
		Configuration config;
		public TestingKubernetesClusterDescriptor(@Nonnull FlinkKubernetesOptions options) {
			super(options);
			this.config = options.getConfiguration();
		}

		@Override
		public ClusterClient deployJobCluster (
			ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached) {
			try {
				return new RestClusterClient<>(config, "test");
			} catch (Exception e) {
				e.printStackTrace();
			}
			return null;
		}
	}
}
