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
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Mounts volumes on the JobManager or TaskManager pod. Volumes can be PVC, empty dir and host path
 * All resources are assumed to exist.
 * Resource definition is contained in FlinkConfiguration as follows:
 *  for job manager:
 * 			volumekey = "kubernetes.jobmanager.volumemount"
 * 	for task manager:
 * 			volumekey = "kubernetes.taskmanager.vlumemount"
  * Each key can contain multiple volumes separated by ",". The volume itself is defined by
 * parameters separated by ":". Parameters include volume type, mount name, volume path and volume specific parameters
 */

public class VolumeMountDecorator extends AbstractKubernetesStepDecorator{

	public static final String KUBERNETES_VOLUMES_PVC = "pvc";
	public static final String KUBERNETES_VOLUMES_EMPTYDIR = "emptydir";
	public static final String KUBERNETES_VOLUMES_HOSTPATH = "hostpath";
	public static final String EMPTYDIRDEFAULT = "default";

	private static final Logger LOG = LoggerFactory.getLogger(VolumeMountDecorator.class);

	private final AbstractKubernetesParameters kubernetesComponentConf;
	private List<Volume> volumes = new LinkedList<>();
	private List<VolumeMount> volumemounts = new LinkedList<>();
	private ConfigOption<String> volumekey;

	public VolumeMountDecorator(AbstractKubernetesParameters kubernetesComponentConf, ConfigOption<String> volumekey) {
		this.volumekey = volumekey;
		this.kubernetesComponentConf = kubernetesComponentConf;
	}

	@Override
	public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {

		// build the list of descriptors
		convertvolumeMountingInfo(kubernetesComponentConf.getFlinkConfiguration().getString(volumekey));
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
	 * Build volume and voloume mount for volumes definitions from config string.
	 * @param  mountInfoString - string representation of volumes
	 */
	private void convertvolumeMountingInfo(String mountInfoString){
		if (mountInfoString == null) {
			return;
		}
		String[] mounts = mountInfoString.split(",");
		for (String mount : mounts) {
			String[] mountInfo = mount.split(":");
			if (mountInfo.length > 0) {
				switch (mountInfo[0]){
					case KUBERNETES_VOLUMES_PVC:
						// pvc
						if (mountInfo.length >= 5){
							List<SubpathToPath> subpaths = (mountInfo.length == 6) ? buildsubpaths(mountInfo[5]) : Arrays.asList(new SubpathToPath(null, mountInfo[2]));
							if (subpaths.size() > 0) {
								// Volome mounts
								for (SubpathToPath subpath : subpaths){
									volumemounts.add(buildVolumeMount(mountInfo[1], subpath.path, subpath.subpath));
								}
								// volumes
								volumes.add(new VolumeBuilder()
										.withName(mountInfo[1])
										.withNewPersistentVolumeClaim()
										.withClaimName(mountInfo[3])
										.withNewReadOnly(mountInfo[4])
										.endPersistentVolumeClaim()
										.build());
							}
							else {
								LOG.error("Failed to process subpath list {}", mountInfo[5]);
							}
						}
						else {
							LOG.error("Not enough parameters for pvc volume mount {}", mount);
						}
						break;
					case KUBERNETES_VOLUMES_EMPTYDIR:
						if (mountInfo.length >= 4) {
							volumemounts.add(buildVolumeMount(mountInfo[1], mountInfo[2], null));
							Quantity sizeLimit = (mountInfo.length == 5) ? new Quantity(mountInfo[4]) : null;
							String medium = (mountInfo[3].equals(EMPTYDIRDEFAULT)) ? "" : mountInfo[3];
							volumes.add(new VolumeBuilder()
								.withName(mountInfo[1])
								.withNewEmptyDir()
									.withMedium(medium)
									.withSizeLimit(sizeLimit)
								.endEmptyDir()
								.build());
						}
						else {
							LOG.error("Not enough parameters for emptydir volume mount {}", mount);
						}
						break;
					case KUBERNETES_VOLUMES_HOSTPATH:
						if (mountInfo.length == 5) {
							volumemounts.add(buildVolumeMount(mountInfo[1], mountInfo[2], null));
							volumes.add(new VolumeBuilder()
									.withName(mountInfo[1])
									.withNewHostPath()
										.withNewPath(mountInfo[3])
										.withNewType(mountInfo[4])
									.endHostPath()
									.build());
						}
						else {
							LOG.error("Not enough parameters for host path volume mount {}", mount);
						}
						break;
					default:
						LOG.error("Unsupported volume type {}", mountInfo[0]);
				}
			}
			else {
				LOG.error("Not enough parameters for volume mount {}", mount);
			}
		}
	}

	/**
	 * Build volume.
	 * @param  name - volume name
	 * @param  path - volume path
	 */
	private VolumeMount buildVolumeMount(String name, String path, String subpath) {
		VolumeMountBuilder builder = new VolumeMountBuilder()
				.withName(name)
				.withMountPath(path);
		if (subpath != null) {
			builder.withSubPath(subpath);
		}
		return builder.build();
	}

	/**
	 * Build subpaths from config string.
	 * @param  subpathString - string representation items in the format
	 *   subpath -> path;....
	 */
	private List<SubpathToPath> buildsubpaths(String subpathString){
		List<SubpathToPath> result = new LinkedList<>();
		String[] items = subpathString.split(";");
		for (String item : items) {
			String[] subpath = item.split("->");
			if (subpath.length == 2) {
				result.add(new SubpathToPath(subpath[0], subpath[1]));
			}
		}
		return result;
	}

	private class SubpathToPath{
		String subpath;
		String path;

		public SubpathToPath(String subpath, String path){
			this.subpath = subpath;
			this.path = path;
		}
	}
}

