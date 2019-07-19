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

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkPod;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Job manager specific pod configuration.
 * */
public class JobManagerPodDecorator extends Decorator<Pod, FlinkPod> {

	@Override
	protected Pod doDecorate(Pod resource, FlinkKubernetesOptions flinkKubernetesOptions) {

		Preconditions.checkArgument(flinkKubernetesOptions != null && flinkKubernetesOptions.getClusterId() != null);

		Map<String, String> labels = LabelBuilder
			.withExist(resource.getMetadata().getLabels())
			.withJobManagerRole()
			.toLabels();

		resource.getMetadata().setLabels(labels);

		List<String> args = Arrays.asList(
			"cluster",
			"-" + FlinkKubernetesOptions.IMAGE_OPTION.getLongOpt(),
			flinkKubernetesOptions.getImageName(),
			"-" + FlinkKubernetesOptions.CLUSTERID_OPTION.getLongOpt(),
			flinkKubernetesOptions.getClusterId()
		);

		Container container = new ContainerBuilder()
			.withName(flinkKubernetesOptions.getClusterId())
			.withArgs(args)
			.withImage(flinkKubernetesOptions.getImageName())
			.withImagePullPolicy("IfNotPresent")
			.withPorts(Arrays.asList(
				new ContainerPortBuilder().withContainerPort(flinkKubernetesOptions.getServicePort(RestOptions.PORT)).build(),
				new ContainerPortBuilder().withContainerPort(flinkKubernetesOptions.getServicePort(JobManagerOptions.PORT)).build(),
				new ContainerPortBuilder().withContainerPort(6124).build(),
				new ContainerPortBuilder().withContainerPort(6125).build()))
			.build();

		resource.setSpec(new PodSpecBuilder().withContainers(Arrays.asList(container)).build());
		return resource;
	}
}
