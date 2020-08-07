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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Base test class for the TaskManager side.
 */
public class KubernetesTaskManagerTestBase extends KubernetesTestBase {

	protected static final int RPC_PORT = 12345;

	protected static final String POD_NAME = "taskmanager-pod-1";
	private static final String DYNAMIC_PROPERTIES = "";

	protected static final int TOTAL_PROCESS_MEMORY = 1184;
	protected static final double TASK_MANAGER_CPU = 2.0;

	protected final Map<String, String> customizedEnvs = new HashMap<String, String>() {
		{
			put("key1", "value1");
			put("key2", "value2");
		}
	};

	protected final Map<String, String> userLabels = new HashMap<String, String>() {
		{
			put("label1", "value1");
			put("label2", "value2");
		}
	};

	protected final Map<String, String> nodeSelector = new HashMap<String, String>() {
		{
			put("env", "production");
			put("disk", "ssd");
		}
	};

	protected TaskExecutorProcessSpec taskExecutorProcessSpec;

	protected ContaineredTaskManagerParameters containeredTaskManagerParameters;

	protected KubernetesTaskManagerParameters kubernetesTaskManagerParameters;

	protected FlinkPod baseFlinkPod = new FlinkPod.Builder().build();

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		flinkConfig.set(TaskManagerOptions.RPC_PORT, String.valueOf(RPC_PORT));
		flinkConfig.set(TaskManagerOptions.CPU_CORES, TASK_MANAGER_CPU);
		flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse(TOTAL_PROCESS_MEMORY + "m"));
		customizedEnvs.forEach((k, v) ->
			flinkConfig.setString(ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX + k, v));
		this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_LABELS, userLabels);
		this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR, nodeSelector);
	}

	@Override
	protected void onSetup() throws Exception {
		taskExecutorProcessSpec = TaskExecutorProcessUtils.processSpecFromConfig(flinkConfig);
		containeredTaskManagerParameters = ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);
		kubernetesTaskManagerParameters = new KubernetesTaskManagerParameters(
				flinkConfig,
				POD_NAME,
				DYNAMIC_PROPERTIES,
				containeredTaskManagerParameters,
				ExternalResourceUtils.getExternalResources(flinkConfig, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
	}
}
