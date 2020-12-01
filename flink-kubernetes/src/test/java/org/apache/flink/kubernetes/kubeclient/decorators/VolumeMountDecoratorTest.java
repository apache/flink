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

import org.apache.flink.kubernetes.VolumeTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link MountSecretsDecorator}.
 */
public class VolumeMountDecoratorTest extends KubernetesJobManagerTestBase {

	private VolumeMountDecorator volumeMountDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		this.flinkConfig.setString(KubernetesConfigOptions.JOBMANAGER_VOLUME_MOUNT.key(),
			VolumeMountDecorator.KUBERNETES_VOLUMES_PVC + ":pvc-mount1:/opt/pvcclaim/tes1/:testclaim1:false,"
				+ VolumeMountDecorator.KUBERNETES_VOLUMES_PVC + ":pvc-mount2::testclaim:false:path1->/opt/pvcclaim/test/path1;path2->/opt/pvcclaim/test/path2,"
				+ VolumeMountDecorator.KUBERNETES_VOLUMES_EMPTYDIR + ":edir-mount:/emptydirclaim:" + VolumeMountDecorator.EMPTYDIRDEFAULT + ","
				+ VolumeMountDecorator.KUBERNETES_VOLUMES_HOSTPATH + ":hp-mount:/var/local/hp:/var/local/hp:DirectoryOrCreate");
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		this.volumeMountDecorator = new VolumeMountDecorator(kubernetesJobManagerParameters, KubernetesConfigOptions.JOBMANAGER_VOLUME_MOUNT);
	}

	@Test
	public void testWhetherPodOrContainerIsDecorated() {
		final FlinkPod resultFlinkPod = volumeMountDecorator.decorateFlinkPod(baseFlinkPod);

		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "pvc-mount1"));
		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "pvc-mount2"));
		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "edir-mount"));
		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "hp-mount2"));

		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "pvc-mount1"));
		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "pvc-mount2"));
		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "edir-mount"));
		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "hp-mount"));

		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "pvc-mount1", "/opt/pvcclaim/tes1/"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "pvc-mount2", "/opt/pvcclaim/test/path1"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "pvc-mount2", "/opt/pvcclaim/test/path2"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "edir-mount", "/emptydirclaim"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "hp-mount", "/var/local/hp"));

		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "pvc-mount1", "/opt/pvcclaim/tes1/"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "pvc-mount2", "/opt/pvcclaim/test/path1"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "pvc-mount2", "/opt/pvcclaim/test/path2"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "edir-mount", "/emptydirclaim"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "hp-mount", "/var/local/hp"));
	}
}
