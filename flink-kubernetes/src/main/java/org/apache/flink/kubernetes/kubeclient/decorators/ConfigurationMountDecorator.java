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
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Mounts configurations on the JobManager or TaskManager pod. Configurations can be ConfigMap or Secrets.
 * All resources are assumed to exist.
 * Resource definition is contained in FlinkConfiguration as follows:
 *  for job manager:
 * 			configurationkey = "kubernetes.jobmanager.configurationmount"
 * 	for task manager:
 * 			configurationkey = "kubernetes.taskmanager.configurationmount"
 * Each key can contain multiple configurations separated by ",". The configuration itself is defined by
 *   parameters separated by ":". Parameters include configuration type, mount name, confiration path
 *   configuration resource name and optional items list
 */
public class ConfigurationMountDecorator extends AbstractKubernetesStepDecorator{

	public static final String KUBERNETES_CONFIGURATION_SECRETS = "secret";
	public static final String KUBERNETES_CONFIGURATION_CONFIGMAP = "configmap";

	private static final Logger LOG = LoggerFactory.getLogger(ConfigurationMountDecorator.class);

	private final AbstractKubernetesParameters kubernetesComponentConf;
	private List<Volume> volumes = new LinkedList<>();
	private List<VolumeMount> volumemounts = new LinkedList<>();
	private ConfigOption<String> configurationkey;

	public ConfigurationMountDecorator(AbstractKubernetesParameters kubernetesComponentConf, ConfigOption<String> configurationkey) {
		this.configurationkey = configurationkey;
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

		// build the list of descriptors
		convertConfiguratioMountingInfo(kubernetesComponentConf.getFlinkConfiguration().getString(configurationkey));

		// Check if we need to add any resources
		if (volumes.size() < 1) {
			return flinkPod;
		}

		// Add volumes to pod
		final Pod podWithMount = new PodBuilder(flinkPod.getPod())
				.editOrNewSpec()
				.addToVolumes(volumes.stream().toArray(Volume[]::new))
				.endSpec()
				.build();

		final Container containerWithMount = new ContainerBuilder(flinkPod.getMainContainer())
				.addToVolumeMounts(volumemounts.stream().toArray(VolumeMount[]::new))
				.build();

		return new FlinkPod.Builder(flinkPod)
				.withPod(podWithMount)
				.withMainContainer(containerWithMount)
				.build();
	}

	/**
	 * Build volume and vloume mount for configurations from config string.
	 * @param  mountInfoString - string representation of configurations
	 */
	private void convertConfiguratioMountingInfo(String mountInfoString){
		if (mountInfoString == null){
			return;
		}
		String[] mounts = mountInfoString.split(",");
		for (String mount : mounts){
			String[] mountInfo = mount.split(":");
			if (mountInfo.length >= 4) {
				// Add volume mounts
				volumemounts.add(buildVolumeMount(mountInfo[1], mountInfo[2]));
				String itemsString = (mountInfo.length == 5) ? mountInfo[4] : null;
				switch (mountInfo[0]) {
					case KUBERNETES_CONFIGURATION_SECRETS:
						volumes.add(new VolumeBuilder()
								.withName(mountInfo[1])
								.withNewSecret()
									.withSecretName(mountInfo[3])
									.withItems(buildItems(itemsString))
								.endSecret()
								.build());
						break;
					case KUBERNETES_CONFIGURATION_CONFIGMAP:
						volumes.add(new VolumeBuilder()
								.withName(mountInfo[1])
								.withNewConfigMap()
									.withName(mountInfo[3])
									.withItems(buildItems(itemsString))
								.endConfigMap()
								.build());

						break;
					default:
						LOG.error("Unsupported configuration type {}", mountInfo[0]);
				}
			}
			else {
				LOG.error("Not enough parameters for configuration mount {}", mount);
			}
		}
	}

	/**
	 * Build volume.
	 * @param  name - volume name
	 * @param  path - volume path
	 */
	private VolumeMount buildVolumeMount(String name, String path) {
		return new VolumeMountBuilder()
				.withName(name)
				.withMountPath(path)
				.build();
	}

	/**
	 * Build items from config string.
	 * @param  itemsString - string representation items in the format
	 *   key -> path;....
	 */
	private List<KeyToPath> buildItems(String itemsString){
		List<KeyToPath> result = null;
		if (itemsString != null) {
			result = new LinkedList<>();
			String[] items = itemsString.split(";");
			for (String item : items) {
				String[] keypath = item.split("->");
				if (keypath.length == 2) {
					result.add(new KeyToPathBuilder().withKey(keypath[0]).withPath(keypath[1]).build());
				}
			}
		}
		return result;
	}
}

