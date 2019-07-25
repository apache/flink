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

package org.apache.flink.kubernetes.kubeclient.fabric8.decorators;

import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkPod;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Task manager specific pod configuration.
 * */
public class TaskManagerDecorator extends Decorator<Pod, FlinkPod> {

	private static final String CONTAINER_NAME = "flink-task-manager";

	TaskManagerPodParameter parameter;

	public TaskManagerDecorator(TaskManagerPodParameter parameters) {
		Preconditions.checkNotNull(parameters);
		this.parameter = parameters;
	}

	@Override
	protected Pod doDecorate(Pod resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		Preconditions.checkArgument(flinkKubernetesOptions != null && flinkKubernetesOptions.getClusterId() != null);
		Map<String, String> labels = org.apache.flink.kubernetes.kubeclient.fabric8.decorators.LabelBuilder
			.withExist(resource.getMetadata().getLabels())
			.withTaskManagerRole()
			.toLabels();

		resource.getMetadata().setLabels(labels);
		resource.getMetadata().setName(this.parameter.getPodName());

		Container container = new ContainerBuilder()
			.withName(CONTAINER_NAME)
			.withImage(flinkKubernetesOptions.getImageName())
			.withImagePullPolicy("IfNotPresent")
			.withArgs(this.parameter.getArgs())
			.withEnv(this.parameter.getEnvironmentVariables()
				.entrySet()
				.stream()
				.map(kv -> new EnvVar(kv.getKey(), kv.getValue(), null))
				.collect(Collectors.toList()))
			.build();

		resource.setSpec(new PodSpecBuilder().withContainers(Arrays.asList(container)).build());
		return resource;
	}
}
