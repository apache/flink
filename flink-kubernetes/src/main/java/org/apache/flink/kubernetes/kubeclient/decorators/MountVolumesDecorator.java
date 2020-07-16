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

import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeSpecification;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesVolume;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Mount user specifed mount to the JobManager or TaskManager.
 */
public class MountVolumesDecorator extends AbstractKubernetesStepDecorator {

	private static final Logger LOG = LoggerFactory.getLogger(MountVolumesDecorator.class);

	private final AbstractKubernetesParameters kubernetesParameters;

	public MountVolumesDecorator(AbstractKubernetesParameters kubernetesParameters) {
		this.kubernetesParameters = kubernetesParameters;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
		List<KubernetesVolumeSpecification> volumeSpecs = kubernetesParameters.getKubernetesVolumeSpecifications();

		final Pod mountedPod = new PodBuilder(flinkPod.getPod())
			.editOrNewSpec()
			.addAllToVolumes(
				volumeSpecs.stream()
					.map(spec -> KubernetesVolume.fromVolumeSpecification(spec).getInternalResource())
					.collect(Collectors.toList()))
			.endSpec()
			.build();

		final Container mainContainer = new ContainerBuilder(flinkPod.getMainContainer())
			.addAllToVolumeMounts(parseVolumeMount(volumeSpecs))
			.build();

		return new FlinkPod.Builder(flinkPod)
			.withPod(mountedPod)
			.withMainContainer(mainContainer)
			.build();
	}

	private List<VolumeMount> parseVolumeMount(List<KubernetesVolumeSpecification> kubernetesVolumeSpecifications) {
		return kubernetesVolumeSpecifications.stream()
			.map(specification ->
				new VolumeMountBuilder()
					.withName(specification.getVolumeName())
					.withMountPath(specification.getMountPath())
					.withSubPath(specification.getMountSubPath())
					.withReadOnly(specification.getMountReadOnly())
					.build())
			.collect(Collectors.toList());
	}
}
