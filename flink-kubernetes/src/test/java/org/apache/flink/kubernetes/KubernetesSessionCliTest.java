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

import org.apache.flink.client.cli.CliArgsException;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.DefaultClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.cli.KubernetesSessionCli;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.executors.KubernetesSessionClusterExecutor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link KubernetesSessionCli}.
 */
public class KubernetesSessionCliTest {

	@Test
	public void testKubernetesSessionCliSetsDeploymentTargetCorrectly() throws CliArgsException {
		final KubernetesSessionCli cli = new KubernetesSessionCli(new Configuration());

		final String[] args = {};
		final Configuration configuration = cli.getEffectiveConfiguration(args);

		assertEquals(KubernetesSessionClusterExecutor.NAME, configuration.get(DeploymentOptions.TARGET));
	}

	@Test
	public void testDynamicProperties() throws Exception {

		final KubernetesSessionCli cli = new KubernetesSessionCli(new Configuration());
		final String[] args = new String[] {
			"-e", KubernetesSessionClusterExecutor.NAME,
			"-Dakka.ask.timeout=5 min",
			"-Denv.java.opts=-DappName=foobar"
		};

		final Configuration executorConfig = cli.getEffectiveConfiguration(args);
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
		final String[] params = new String[] {
				"-e", KubernetesSessionClusterExecutor.NAME,
				"-D" + TaskManagerOptions.NUM_TASK_SLOTS.key() + "=3"};

		final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1234);

		final Configuration executorConfig = cli.getEffectiveConfiguration(params);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		// each task manager has 3 slots but the parallelism is 7. Thus the slots should be increased.
		assertEquals(3, clusterSpecification.getSlotsPerTaskManager());
		assertEquals(1, clusterSpecification.getNumberTaskManagers());
	}

	@Test
	public void testResumeFromKubernetesID() throws Exception {
		final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final String clusterId = "my-test-CLUSTER_ID";
		final String[] args = new String[] {
				"-e", KubernetesSessionClusterExecutor.NAME,
				"-D" + KubernetesConfigOptions.CLUSTER_ID.key() + "=" + clusterId};

		final Configuration executorConfig = cli.getEffectiveConfiguration(args);
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
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(taskManagerMemory + "m"));
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {
				"-e", KubernetesSessionClusterExecutor.NAME,
				"-D" + JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key() + "=" + jobManagerMemory + "m",
				"-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=" + taskManagerMemory + "m",
				"-D" + TaskManagerOptions.NUM_TASK_SLOTS.key() + "=" + slotsPerTaskManager
		};

		final KubernetesSessionCli cli = new KubernetesSessionCli(configuration);

		Configuration executorConfig = cli.getEffectiveConfiguration(args);
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
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(taskManagerMemory + "m"));
		final int slotsPerTaskManager = 42;
		configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);

		final String[] args = {"-e", KubernetesSessionClusterExecutor.NAME};
		final KubernetesSessionCli cli = new KubernetesSessionCli(configuration);

		Configuration executorConfig = cli.getEffectiveConfiguration(args);
		ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(jobManagerMemory));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(taskManagerMemory));
		assertThat(clusterSpecification.getSlotsPerTaskManager(), is(slotsPerTaskManager));
	}

	/**
	 * Tests the specifying heap memory with unit (MB) for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithUnitMB() throws Exception {
		final String[] args = new String[] {
				"-e", KubernetesSessionClusterExecutor.NAME,
				"-D" + JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key() + "=1024m",
				"-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=2048m"
		};

		final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final Configuration executorConfig = cli.getEffectiveConfiguration(args);
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
		final String[] args = new String[] {
				"-e", KubernetesSessionClusterExecutor.NAME,
				"-D" + JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.key() + "=1g",
				"-D" + TaskManagerOptions.TOTAL_PROCESS_MEMORY.key() + "=3g"
		};

		final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final Configuration executorConfig = cli.getEffectiveConfiguration(args);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(3072));
	}

	/**
	 * Tests the specifying heap memory with old config key for job manager and task manager.
	 */
	@Test
	public void testHeapMemoryPropertyWithOldConfigKey() throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(DeploymentOptions.TARGET, KubernetesSessionClusterExecutor.NAME);
		configuration.setInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY_MB, 2048);
		configuration.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB, 4096);

		final KubernetesSessionCli cli = new KubernetesSessionCli(configuration);

		final Configuration executorConfig = cli.getEffectiveConfiguration(new String[]{});
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
		final String[] args = new String[] {
				"-e", KubernetesSessionClusterExecutor.NAME
		};

		final KubernetesSessionCli cli = createFlinkKubernetesCustomCliWithTmTotalMemory(1024);

		final Configuration executorConfig = cli.getEffectiveConfiguration(args);
		final ClusterClientFactory<String> clientFactory = getClusterClientFactory(executorConfig);
		final ClusterSpecification clusterSpecification = clientFactory.getClusterSpecification(executorConfig);

		assertThat(clusterSpecification.getMasterMemoryMB(), is(1024));
		assertThat(clusterSpecification.getTaskManagerMemoryMB(), is(1024));
	}

	private ClusterClientFactory<String> getClusterClientFactory(final Configuration executorConfig) {
		final ClusterClientServiceLoader clusterClientServiceLoader = new DefaultClusterClientServiceLoader();
		return clusterClientServiceLoader.getClusterClientFactory(executorConfig);
	}

	private KubernetesSessionCli createFlinkKubernetesCustomCliWithTmTotalMemory(int totalMemory) {
		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(totalMemory + "m"));
		return new KubernetesSessionCli(configuration);
	}
}
