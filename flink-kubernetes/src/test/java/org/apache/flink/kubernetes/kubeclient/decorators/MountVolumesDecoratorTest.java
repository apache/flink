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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.volume.EmptyDirVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.HostPathVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.KubernetesVolumeSpecification;
import org.apache.flink.kubernetes.configuration.volume.NFSVolumeConfig;
import org.apache.flink.kubernetes.configuration.volume.PVCVolumeConfig;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;

import org.junit.Test;

import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_EMPTYDIR_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_HOSTPATH_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_PATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_READONLY_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_NFS_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_PATH_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_SERVER_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUMES_PVC_TYPE;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUME_NAME;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_VOLUME_TYPE;
import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link MountVolumesDecorator}.
 */
public class MountVolumesDecoratorTest extends KubernetesJobManagerTestBase {

	@Test
	public void testMountsHostPathVolume() {
		KubernetesVolumeConfig hostPathVolumeConfig = new HostPathVolumeConfig("/tmp/hostPath");
		KubernetesVolumeSpecification volumeSpec = new KubernetesVolumeSpecification(
			"testHostPathVolume",
			"/path/to/mount",
			"/tmp/subPath",
			false,
			hostPathVolumeConfig);

		AbstractKubernetesParameters kubernetesParameters = createKubernetesParameters(volumeSpec);
		MountVolumesDecorator mountVolumesDecorator = new MountVolumesDecorator(kubernetesParameters);
		FlinkPod mountedPod = mountVolumesDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(1, mountedPod.getPod().getSpec().getVolumes().size());
		assertEquals("/tmp/hostPath", mountedPod.getPod().getSpec().getVolumes().get(0).getHostPath().getPath());

		assertEquals(1, mountedPod.getMainContainer().getVolumeMounts().size());
		assertEquals("/path/to/mount", mountedPod.getMainContainer().getVolumeMounts().get(0).getMountPath());
		assertEquals("testHostPathVolume", mountedPod.getMainContainer().getVolumeMounts().get(0).getName());
		assertEquals(false, mountedPod.getMainContainer().getVolumeMounts().get(0).getReadOnly());
	}

	@Test
	public void testMountEmptyDirVolume() {
		KubernetesVolumeConfig emptyDirVolumeConfig = new EmptyDirVolumeConfig("memory", "5g");
		KubernetesVolumeSpecification volumeSpec = new KubernetesVolumeSpecification(
			"testEmptyDirVolume",
			"/path/to/mount",
			"/tmp/subPath",
			false,
			emptyDirVolumeConfig);

		AbstractKubernetesParameters kubernetesParameters = createKubernetesParameters(volumeSpec);
		MountVolumesDecorator mountVolumesDecorator = new MountVolumesDecorator(kubernetesParameters);
		FlinkPod mountedPod = mountVolumesDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(1, mountedPod.getPod().getSpec().getVolumes().size());
		assertEquals("memory", mountedPod.getPod().getSpec().getVolumes().get(0).getEmptyDir().getMedium());
		assertEquals("5g", mountedPod.getPod().getSpec().getVolumes().get(0).getEmptyDir().getSizeLimit().toString());

		assertEquals(1, mountedPod.getMainContainer().getVolumeMounts().size());
		assertEquals("/path/to/mount", mountedPod.getMainContainer().getVolumeMounts().get(0).getMountPath());
		assertEquals("/tmp/subPath", mountedPod.getMainContainer().getVolumeMounts().get(0).getSubPath());
		assertEquals("testEmptyDirVolume", mountedPod.getMainContainer().getVolumeMounts().get(0).getName());
		assertEquals(false, mountedPod.getMainContainer().getVolumeMounts().get(0).getReadOnly());
	}

	@Test
	public void testMountPVCVolume() {
		KubernetesVolumeConfig pvcVolumeConfig = new PVCVolumeConfig("testClaimName");
		KubernetesVolumeSpecification volumeSpec = new KubernetesVolumeSpecification(
			"testPVCVolume",
			"/path/to/mount",
			"/tmp/subPath",
			false,
			pvcVolumeConfig);

		AbstractKubernetesParameters kubernetesParameters = createKubernetesParameters(volumeSpec);
		MountVolumesDecorator mountVolumesDecorator = new MountVolumesDecorator(kubernetesParameters);
		FlinkPod mountedPod = mountVolumesDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(1, mountedPod.getPod().getSpec().getVolumes().size());
		assertEquals("testClaimName", mountedPod.getPod().getSpec().getVolumes().get(0).getPersistentVolumeClaim().getClaimName());

		assertEquals(1, mountedPod.getMainContainer().getVolumeMounts().size());
		assertEquals("/path/to/mount", mountedPod.getMainContainer().getVolumeMounts().get(0).getMountPath());
		assertEquals("/tmp/subPath", mountedPod.getMainContainer().getVolumeMounts().get(0).getSubPath());
		assertEquals("testPVCVolume", mountedPod.getMainContainer().getVolumeMounts().get(0).getName());
		assertEquals(false, mountedPod.getMainContainer().getVolumeMounts().get(0).getReadOnly());
	}

	@Test
	public void testMountNFSVolume() {
		KubernetesVolumeConfig nfsVolumeConfig = new NFSVolumeConfig("/tmp/nfs", "testServer");
		KubernetesVolumeSpecification volumeSpec = new KubernetesVolumeSpecification(
			"testNFSVolume",
			"/path/to/mount",
			"/tmp/subPath",
			false,
			nfsVolumeConfig);

		AbstractKubernetesParameters kubernetesParameters = createKubernetesParameters(volumeSpec);
		MountVolumesDecorator mountVolumesDecorator = new MountVolumesDecorator(kubernetesParameters);
		FlinkPod mountedPod = mountVolumesDecorator.decorateFlinkPod(baseFlinkPod);
		assertEquals(1, mountedPod.getPod().getSpec().getVolumes().size());
		assertEquals("/tmp/nfs", mountedPod.getPod().getSpec().getVolumes().get(0).getNfs().getPath());
		assertEquals("testServer", mountedPod.getPod().getSpec().getVolumes().get(0).getNfs().getServer());

		assertEquals(1, mountedPod.getMainContainer().getVolumeMounts().size());
		assertEquals("/path/to/mount", mountedPod.getMainContainer().getVolumeMounts().get(0).getMountPath());
		assertEquals("/tmp/subPath", mountedPod.getMainContainer().getVolumeMounts().get(0).getSubPath());
		assertEquals("testNFSVolume", mountedPod.getMainContainer().getVolumeMounts().get(0).getName());
		assertEquals(false, mountedPod.getMainContainer().getVolumeMounts().get(0).getReadOnly());
	}

	private AbstractKubernetesParameters createKubernetesParameters(KubernetesVolumeSpecification volumeSpec) {
		String volumeSpecPropertyString = "" +
			KUBERNETES_VOLUME_NAME + ":" + volumeSpec.getVolumeName() + "," +
			KUBERNETES_VOLUMES_MOUNT_PATH_KEY + ":" + volumeSpec.getMountPath() + "," +
			KUBERNETES_VOLUMES_MOUNT_SUBPATH_KEY + ":" + volumeSpec.getMountSubPath() + "," +
			KUBERNETES_VOLUMES_MOUNT_READONLY_KEY + ":" + volumeSpec.getMountReadOnly() + ",";

		final KubernetesVolumeConfig volumeConfig = volumeSpec.getVolumeConfig();
		if (volumeConfig instanceof HostPathVolumeConfig) {
			volumeSpecPropertyString += "" +
				KUBERNETES_VOLUME_TYPE + ":" + KUBERNETES_VOLUMES_HOSTPATH_TYPE + "," +
				KUBERNETES_VOLUMES_OPTIONS_PATH_KEY + ":" + ((HostPathVolumeConfig) volumeConfig).getHostPath() + ";";
		} else if (volumeConfig instanceof EmptyDirVolumeConfig) {
			volumeSpecPropertyString += "" +
				KUBERNETES_VOLUME_TYPE + ":" + KUBERNETES_VOLUMES_EMPTYDIR_TYPE + "," +
				KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY + ":" + ((EmptyDirVolumeConfig) volumeConfig).getMedium() + "," +
				KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY + ":" + ((EmptyDirVolumeConfig) volumeConfig).getSizeLimit() + ";";
		} else if (volumeConfig instanceof PVCVolumeConfig) {
			volumeSpecPropertyString += "" +
				KUBERNETES_VOLUME_TYPE + ":" + KUBERNETES_VOLUMES_PVC_TYPE + "," +
				KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY + ":" + ((PVCVolumeConfig) volumeConfig).getClaimName() + ";";
		} else if (volumeConfig instanceof NFSVolumeConfig) {
			volumeSpecPropertyString += "" +
				KUBERNETES_VOLUME_TYPE + ":" + KUBERNETES_VOLUMES_NFS_TYPE + "," +
				KUBERNETES_VOLUMES_OPTIONS_PATH_KEY + ":" + ((NFSVolumeConfig) volumeConfig).getPath() + "," +
				KUBERNETES_VOLUMES_OPTIONS_SERVER_KEY + ":" + ((NFSVolumeConfig) volumeConfig).getServer() + ";";
		}

		flinkConfig.setString(KubernetesConfigOptions.JOB_MANAGER_VOLUMES.key(), volumeSpecPropertyString);
		return new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);
	}
}
