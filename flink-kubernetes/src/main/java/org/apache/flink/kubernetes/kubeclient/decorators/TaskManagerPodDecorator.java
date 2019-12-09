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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Volume;

import java.io.File;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Task manager specific pod configuration.
 */
public class TaskManagerPodDecorator extends Decorator<Pod, KubernetesPod> {

	private static final String CONTAINER_NAME = "flink-task-manager";

	private final TaskManagerPodParameter parameter;

	public TaskManagerPodDecorator(TaskManagerPodParameter parameters) {
		checkNotNull(parameters);
		this.parameter = parameters;
	}

	@Override
	protected Pod decorateInternalResource(Pod pod, Configuration flinkConfig) {

		final String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		checkNotNull(clusterId, "ClusterId must be specified!");

		final int taskManagerRpcPort = KubernetesUtils.parsePort(flinkConfig, TaskManagerOptions.RPC_PORT);
		checkArgument(taskManagerRpcPort > 0, "%s should not be 0.", TaskManagerOptions.RPC_PORT.key());

		final String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		final boolean hasLogback = new File(confDir, Constants.CONFIG_FILE_LOGBACK_NAME).exists();
		final boolean hasLog4j = new File(confDir, Constants.CONFIG_FILE_LOG4J_NAME).exists();

		final Map<String, String> labels = new LabelBuilder()
			.withExist(pod.getMetadata().getLabels())
			.withTaskManagerComponent()
			.toLabels();

		pod.getMetadata().setLabels(labels);
		pod.getMetadata().setName(this.parameter.getPodName());

		final Volume configMapVolume = KubernetesUtils.getConfigMapVolume(clusterId, hasLogback, hasLog4j);

		pod.setSpec(new PodSpecBuilder()
			.withVolumes(configMapVolume)
			.withContainers(createTaskManagerContainer(flinkConfig, hasLogback, hasLog4j, taskManagerRpcPort))
			.build());
		return pod;
	}

	private Container createTaskManagerContainer(
			Configuration flinkConfig,
			boolean hasLogBack,
			boolean hasLog4j,
			int taskManagerRpcPort) {
		final String flinkConfDirInPod = flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		return new ContainerBuilder()
			.withName(CONTAINER_NAME)
			.withCommand(flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH))
			.withArgs(this.parameter.getArgs())
			.withImage(flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE))
			.withImagePullPolicy(flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY))
			.withResources(KubernetesUtils.getResourceRequirements(
				parameter.getTaskManagerMemoryInMB(),
				parameter.getTaskManagerCpus()))
			.withPorts(new ContainerPortBuilder().withContainerPort(taskManagerRpcPort).build())
			.withEnv(this.parameter.getEnvironmentVariables()
				.entrySet()
				.stream()
				.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
				.collect(Collectors.toList()))
			.withVolumeMounts(KubernetesUtils.getConfigMapVolumeMount(flinkConfDirInPod, hasLogBack, hasLog4j))
			.build();
	}
}
