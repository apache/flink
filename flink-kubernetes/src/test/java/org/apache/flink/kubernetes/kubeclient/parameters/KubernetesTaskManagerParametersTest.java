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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link KubernetesTaskManagerParameters}.
 */
public class KubernetesTaskManagerParametersTest {

	private static final int TASK_MANAGER_MEMORY = 1024;
	private static final double TASK_MANAGER_CPU = 1.2;
	private static final int RPC_PORT = 13001;

	private static final String POD_NAME = "task-manager-pod-1";
	private static final String DYNAMIC_PROPERTIES = "-Dkey.b='b2'";

	private final Map<String, String> customizedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	private KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

	@Before
	public void setup() {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.set(TaskManagerOptions.CPU_CORES, TASK_MANAGER_CPU);
		flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(TASK_MANAGER_MEMORY + "m"));
		flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(RPC_PORT));

		customizedEnvs.forEach((k, v) ->
			flinkConfig.setString(ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k, v));

		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig);
		final ContaineredTaskManagerParameters containeredTaskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec,
				flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS));

		this.kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(flinkConfig,
			POD_NAME,
			TASK_MANAGER_MEMORY,
			DYNAMIC_PROPERTIES,
			containeredTaskManagerParameters);
	}

	@Test
	public void testGetEnvironments() {
		assertEquals(customizedEnvs, kubernetesTaskManagerParameters.getEnvironments());
	}

	@Test
	public void testGetPodName() {
		assertEquals(POD_NAME, kubernetesTaskManagerParameters.getPodName());
	}

	@Test
	public void testGetTaskManagerMemoryMB() {
		assertEquals(TASK_MANAGER_MEMORY, kubernetesTaskManagerParameters.getTaskManagerMemoryMB());
	}

	@Test
	public void testGetTaskManagerCPU() {
		assertEquals(TASK_MANAGER_CPU, kubernetesTaskManagerParameters.getTaskManagerCPU(), 0.000000000001);
	}

	@Test
	public void testGetRpcPort() {
		assertEquals(RPC_PORT, kubernetesTaskManagerParameters.getRPCPort());
	}

	@Test
	public void testGetDynamicProperties() {
		assertEquals(DYNAMIC_PROPERTIES, kubernetesTaskManagerParameters.getDynamicProperties());
	}
}
