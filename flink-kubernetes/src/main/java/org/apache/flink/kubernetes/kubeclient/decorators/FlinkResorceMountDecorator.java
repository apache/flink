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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Mounts resources on the JobManager or TaskManager pod. Resource can be PVC, Secret or ConfigMap.
 * All resources are assumed to exist.
 * Resource definition is contained in FlinkConfiguration as follows:
 *  for job manager:
 * 			pvckey = "kubernetes.jobmanager.pvcmount"
 * 			secretkey = "kubernetes.jobmanager.secretmount"
 * 			pvckey = "kubernetes.jobmanager.configmapmount"
 * 	for task manager:
 * 			pvckey = "kubernetes.taskmanager.pvcmount"
 * 			secretkey = "kubernetes.taskmanager.secretmount"
 * 			pvckey = "kubernetes.taskmanager.configmapmount"
 * Each key can contain multiple resources separated by ",". The resource itself is defined by three
 * strings separated by ":", as following :
 * 	mount name:resource name:mount path
 */
public class FlinkResorceMountDecorator extends AbstractKubernetesStepDecorator{

	/**
	 * Enumaration defining types of resources.
	 */
	private enum ResourceType {PVC, SECRET, CONFIGMAP}

	/**
	 * Internal representation of resource.
	 */
	private class ResourceDescriptor{
		private ResourceType type;			// Resource type
		private String mountname;			// Mount name
		private String resourcename;		// Resource name
		private String mountpath;			// Mount path

		/**
		 * Constructor.
		 */
		ResourceDescriptor(ResourceType type, String mountname, String resourcename, String mountpath){
			this.type = type;
			this.mountname = mountname;
			this.resourcename = resourcename;
			this.mountpath = mountpath;
		}
	}

	private final AbstractKubernetesParameters kubernetesComponentConf;
	private List<ResourceDescriptor> descriptors = new LinkedList<>();
	private ConfigOption<String> pvckey;
	private ConfigOption<String> secretkey;
	private ConfigOption<String> configkey;

	public FlinkResorceMountDecorator(AbstractKubernetesParameters kubernetesComponentConf, ConfigOption<String> pvckey, ConfigOption<String> secretkey, ConfigOption<String> configkey) {
		this.pvckey = pvckey;
		this.secretkey = secretkey;
		this.configkey = configkey;
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

		// buil the list of descriptors
		// PVCs
		getMountingInfo(ResourceType.PVC, kubernetesComponentConf.getFlinkConfiguration().getString(pvckey));

		// Secrets
		getMountingInfo(ResourceType.SECRET, kubernetesComponentConf.getFlinkConfiguration().getString(secretkey));

		// configMaps
		getMountingInfo(ResourceType.CONFIGMAP, kubernetesComponentConf.getFlinkConfiguration().getString(configkey));

		// Check if we need to add any resources
		if (descriptors.size() < 1) {
			return flinkPod;
		}

		// Add volumes to pod
		final Pod mountedPod = decoratePod(flinkPod.getPod());

		// Add mount info to the container
		final ContainerBuilder containerBuilder = new ContainerBuilder(flinkPod.getMainContainer());
		for (ResourceDescriptor descriptor : descriptors) {
			containerBuilder
					.addNewVolumeMount()
					.withName(descriptor.mountname)
					.withMountPath(descriptor.mountpath)
					.endVolumeMount();
		}
		final Container mountedMainContainer = containerBuilder.build();

		// Return pod
		return new FlinkPod.Builder(flinkPod)
				.withPod(mountedPod)
				.withMainContainer(mountedMainContainer)
				.build();

	}

	/**
	 * Decorate pod with mount information.
	 * @param pod - original pod
	 * @return pod with mounts added
	 */
	private Pod decoratePod(Pod pod){
		final PodBuilder podBuilder = new PodBuilder(pod);
		for (ResourceDescriptor descriptor : descriptors) {
			switch (descriptor.type) {
				case PVC :
					podBuilder.editSpec().addNewVolume().withName(descriptor.mountname)
							.withNewPersistentVolumeClaim()
							.withClaimName(descriptor.resourcename).withNewReadOnly(false)
							.endPersistentVolumeClaim()
							.endVolume().endSpec();
					break;
				case SECRET :
					podBuilder.editSpec().addNewVolume().withName(descriptor.mountname)
							.withNewSecret()
							.withSecretName(descriptor.resourcename)
							.endSecret()
							.endVolume().endSpec();
					break;
				case CONFIGMAP:
					podBuilder.editSpec().addNewVolume().withName(descriptor.mountname)
							.withNewConfigMap()
							.withName(descriptor.resourcename)
							.endConfigMap()
							.endVolume().endSpec();
					break;
			}
		}
		return podBuilder.build();
	}

	/**
 	 * Build descriptors from config string.
	 * @param type - resource type
	 * @param  mountInfoString - string representation of resource
	 */
	private void getMountingInfo(ResourceType type, String mountInfoString){
		if (mountInfoString == null){
			return;
		}
		String[] mounts = mountInfoString.split(",");
		for (String mount : mounts){
			String[] mountInfo = mount.split(":");
			if (mountInfo.length == 3) {
				descriptors.add(new ResourceDescriptor(type, mountInfo[0], mountInfo[1], mountInfo[2]));
			}
		}
	}
}
