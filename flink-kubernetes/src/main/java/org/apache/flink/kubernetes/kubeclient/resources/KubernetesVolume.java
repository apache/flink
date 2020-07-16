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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.configuration.volume.EmptyDirVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.HostPathVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeSpecification;
import org.apache.flink.kubernetes.configuration.volume.NFSVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.PVCVolumeConfig;

import io.fabric8.kubernetes.api.model.EmptyDirVolumeSource;
import io.fabric8.kubernetes.api.model.HostPathVolumeSource;
import io.fabric8.kubernetes.api.model.NFSVolumeSource;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSource;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

/**
 * Represent Volume resource in kubernetes.
 */
public class KubernetesVolume extends KubernetesResource<Volume> {

	private KubernetesVolume(Volume internalResource) {
		super(internalResource);
	}

	public static KubernetesVolume fromVolumeSpecification(KubernetesVolumeSpecification volumeSpecification) {
		final VolumeBuilder volumeBuilder = new VolumeBuilder();
		final KubernetesVolumeConfig volumeConfig = volumeSpecification.getVolumeConfig();
		if (volumeConfig instanceof EmptyDirVolumeConfig) {
			final EmptyDirVolumeConfig emptyDirVolumeConfig = (EmptyDirVolumeConfig) volumeConfig;
			volumeBuilder.withEmptyDir(new EmptyDirVolumeSource(
				emptyDirVolumeConfig.getMedium(), new Quantity(emptyDirVolumeConfig.getSizeLimit())));
		} else if (volumeConfig instanceof HostPathVolumeConfig) {
			final HostPathVolumeConfig hostPathVolumeConfig = (HostPathVolumeConfig) volumeConfig;
			volumeBuilder.withHostPath(new HostPathVolumeSource(hostPathVolumeConfig.getHostPath(), ""));
		} else if (volumeConfig instanceof NFSVolumeConfig) {
			final NFSVolumeConfig nfsVolumeConfig = (NFSVolumeConfig) volumeConfig;
			volumeBuilder.withNfs(new NFSVolumeSource(
				nfsVolumeConfig.getPath(), null, nfsVolumeConfig.getServer()));
		} else if (volumeConfig instanceof PVCVolumeConfig) {
			final PVCVolumeConfig pvcVolumeConfig = (PVCVolumeConfig) volumeConfig;
			volumeBuilder.withPersistentVolumeClaim(new PersistentVolumeClaimVolumeSource(
				pvcVolumeConfig.getClaimName(), volumeSpecification.getMountReadOnly()));
		} else {
			throw new IllegalArgumentException(
				volumeConfig.getClass().getSimpleName() + " is not a supported volume config class");
		}

		volumeBuilder.withName(volumeSpecification.getVolumeName());
		return new KubernetesVolume(volumeBuilder.build());
	}
}
