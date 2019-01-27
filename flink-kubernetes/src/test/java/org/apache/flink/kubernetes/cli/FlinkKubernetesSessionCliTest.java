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

package org.apache.flink.kubernetes.cli;

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.deploy.KubernetesClusterId;

import org.apache.commons.cli.CommandLine;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for FlinkKubernetesSessionCli.
 */
public class FlinkKubernetesSessionCliTest {

	private static final String TEST_CLUSTER_ID = "flink-session-1";

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testIsActive() throws Exception {
		final FlinkKubernetesSessionCli cli = new FlinkKubernetesSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"",
			"");

		final CommandLine commandLine = cli.parseCommandLineOptions(
			new String[] {"-m", "kubernetes-cluster", "-n", "2"}, true);
		Assert.assertNotNull(commandLine);
		assertTrue(cli.isActive(commandLine));
	}

	@Test
	public void testDynamicProperties() throws Exception {

		FlinkKubernetesSessionCli cli = new FlinkKubernetesSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"",
			"");

		String name = "jm1";
		final CommandLine commandLine = cli.parseCommandLineOptions(
			new String[] {"-n", "15", "-D",
				KubernetesConfigOptions.JOB_MANAGER_CONTAINER_NAME.key() + "=" + name}, true);

		Assert.assertNotNull(commandLine);
		Configuration configuration = cli.applyCommandLineOptionsToConfiguration(commandLine);
		Assert.assertNotNull(configuration);
		Assert.assertEquals(name, configuration.getString(KubernetesConfigOptions.JOB_MANAGER_CONTAINER_NAME));
	}

	@Test
	public void testResumeFromKubernetesCluster() throws Exception {
		final FlinkKubernetesSessionCli flinkKubernetesSessionCli = new FlinkKubernetesSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"k",
			"kubernetes");

		final CommandLine commandLine = flinkKubernetesSessionCli.parseCommandLineOptions(
			new String[] {"-knm", TEST_CLUSTER_ID, "-ksa", "localhost:8081"}, true);

		final KubernetesClusterId clusterId = flinkKubernetesSessionCli.getClusterId(commandLine);

		assertNotNull(clusterId);
		assertEquals(TEST_CLUSTER_ID, clusterId.toString());
	}

	@Test
	public void testGetClusterSpecification() throws CliArgsException {
		final FlinkKubernetesSessionCli flinkKubernetesSessionCli = new FlinkKubernetesSessionCli(
			new Configuration(),
			tmp.getRoot().getAbsolutePath(),
			"k",
			"kubernetes");

		final CommandLine commandLine = flinkKubernetesSessionCli.parseCommandLineOptions(
			new String[] {"-knm", TEST_CLUSTER_ID, "-kjm", "512", "-ktm", "256", "-kn", "3", "-ks", "5"}, true);
		ClusterSpecification specification = flinkKubernetesSessionCli.getClusterSpecification(commandLine);
		assertEquals(512, specification.getMasterMemoryMB());
		assertEquals(256, specification.getTaskManagerMemoryMB());
		assertEquals(3, specification.getNumberTaskManagers());
		assertEquals(5, specification.getSlotsPerTaskManager());
	}
}
