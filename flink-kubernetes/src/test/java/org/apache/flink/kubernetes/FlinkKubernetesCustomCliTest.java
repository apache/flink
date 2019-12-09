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

import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.cli.FlinkKubernetesCustomCli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link FlinkKubernetesCustomCli}.
 */
public class FlinkKubernetesCustomCliTest {

	@Test
	public void testDynamicProperties() throws Exception {

		final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(
			new Configuration(),
			"",
			"");
		Options options = new Options();
		cli.addGeneralOptions(options);
		cli.addRunOptions(options);

		final CommandLineParser parser = new DefaultParser();
		final CommandLine cmd = parser.parse(options, new String[]{
			"run",
			"-D", "akka.ask.timeout=5 min",
			"-D", "env.java.opts=-DappName=foobar"
		});

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(cmd);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);

		Assert.assertNotNull(clientFactory);

		final Map<String, String> executorConfigMap = executorConfig.toMap();
		assertEquals(3, executorConfigMap.size());
		assertEquals("5 min", executorConfigMap.get("akka.ask.timeout"));
		assertEquals("-DappName=foobar", executorConfigMap.get("env.java.opts"));
		assertTrue(executorConfigMap.containsKey(DeploymentOptions.TARGET.key()));
	}

	@Test
	public void testCorrectSettingOfMaxSlots() throws Exception {
		String[] params =
			new String[] {"-ks", "3"};

		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1234);

		final CommandLine commandLine = cli.parseCommandLineOptions(params, true);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		assertEquals(3, clusterSpecification.getSlotsPerTaskManager());
		assertEquals(1, clusterSpecification.getNumberTaskManagers());
	}

	@Test
	public void testResumeFromKubernetesID() throws Exception {
		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final String clusterId = "my-test-CLUSTER_ID";
		final CommandLine commandLine = cli.parseCommandLineOptions(new String[] {"-kid", clusterId}, true);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory clientFactory = getClusterClientFactory(executorConfig);

		assertEquals(clusterId, clientFactory.getClusterId(executorConfig));
	}

	/**
	 * Tests that the command line arguments override the configuration settings
	 * when the {@link ClusterSpecification} is created.
	 */
	@Test
	public void testCommandLineClusterSpecification() throws Exception {
		final Configuration configuration = new Configuration();
		final int jobManagerMemory = 1337;
		final int taskManagerMemory = 7331;
		final int slotsPerTaskManager = 30;

		configuration.setString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, jobManagerMemory + "m");
		configuration.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, taskManagerMemory + "m");
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {"-kjm", String.valueOf(jobManagerMemory) + "m", "-ktm", String.valueOf(taskManagerMemory) +
			"m", "-ks", String.valueOf(slotsPerTaskManager)};
		final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(
			configuration,
			"k",
			"kubernetes");

		CommandLine commandLine = cli.parseCommandLineOptions(args, false);

		Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(jobManagerMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(taskManagerMemory));
		assertThat(clusterSpecification.getSlotsPerTaskManager(), is(slotsPerTaskManager));
	}

	/**
	 * Tests that the configuration settings are used to create the
	 * {@link ClusterSpecification}.
	 */
	@Test
	public void testConfigurationClusterSpecification() throws Exception {
		final Configuration configuration = new Configuration();
		final int jobManagerMemory = 1337;
		configuration.setString(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY, jobManagerMemory + "m");
		final int taskManagerMemory = 7331;
		configuration.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, taskManagerMemory + "m");
		final int slotsPerTaskManager = 42;
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {};
		final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(
			configuration,
			"",
			"kubernetes");

		CommandLine commandLine = cli.parseCommandLineOptions(args, false);

		Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(jobManagerMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(taskManagerMemory));
		assertThat(clusterSpecification.getSlotsPerTaskManager(), is(slotsPerTaskManager));
	}

	/**
	 * Tests the specifying heap memory without unit for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithoutUnit() throws Exception {
		final String[] args = new String[] { "-kjm", "1024", "-ktm", "2048" };
		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final CommandLine commandLine = cli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying heap memory with unit (MB) for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithUnitMB() throws Exception {
		final String[] args = new String[] { "-kjm", "1024m", "-ktm", "2048m" };
		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);
		final CommandLine commandLine = cli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying heap memory with arbitrary unit for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithArbitraryUnit() throws Exception {
		final String[] args = new String[] { "-kjm", "1g", "-ktm", "2g" };
		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);
		final CommandLine commandLine = cli.parseCommandLineOptions(args, false);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(2048));
	}

	/**
	 * Tests the specifying heap memory with old config key for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithOldConfigKey() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB, 2048);
		configuration.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB, 4096);

		final FlinkKubernetesCustomCli cli = new FlinkKubernetesCustomCli(
			configuration,
			"k",
			"kubernetes");

		final CommandLine commandLine = cli.parseCommandLineOptions(new String[0], false);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(2048));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(4096));
	}

	/**
	 * Tests the specifying heap memory with config default value for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithConfigDefaultValue() throws Exception {
		final FlinkKubernetesCustomCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final CommandLine commandLine = cli.parseCommandLineOptions(new String[0], false);

		final Configuration executorConfig = cli.applyCommandLineOptionsToConfiguration(commandLine);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(1024));
	}

	private ClusterClientFactory<String> getClusterClientFactory(final Configuration executorConfig) {
		final ClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
		return clusterClientServiceLoader.getClusterClientFactory(executorConfig);
	}

	private FlinkKubernetesCustomCli createFlinkKubernetesCustomCliWithTmTotalMemory(int totalMemomory) {
		Configuration configuration = new Configuration();
		configuration.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY, totalMemomory + "m");
		return new FlinkKubernetesCustomCli(configuration, "k", "kubernetes");
	}
}
