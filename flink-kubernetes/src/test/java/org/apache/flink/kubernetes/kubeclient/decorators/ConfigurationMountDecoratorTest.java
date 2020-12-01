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
public class ConfigurationMountDecoratorTest extends KubernetesJobManagerTestBase {

	private ConfigurationMountDecorator configurationMountDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		this.flinkConfig.setString(KubernetesConfigOptions.JOBMANAGER_CONFIGURATION_MOUNT.key(),
			"secret:secret-mount1:/opt/secret/1:test-secret1,secret:secret-mount2:/opt/secret/2:test-secret2:token->value,configmap:configmap-mount:/opt/configmap:test-configmap");
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		this.configurationMountDecorator = new ConfigurationMountDecorator(kubernetesJobManagerParameters, KubernetesConfigOptions.JOBMANAGER_CONFIGURATION_MOUNT);
	}

	@Test
	public void testWhetherPodOrContainerIsDecorated() {
		final FlinkPod resultFlinkPod = configurationMountDecorator.decorateFlinkPod(baseFlinkPod);

		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "secret-mount1"));
		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "secret-mount2"));
		assertFalse(VolumeTestUtils.podHasVolume(baseFlinkPod.getPod(), "configmap-mount"));

		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "secret-mount1"));
		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "secret-mount2"));
		assertTrue(VolumeTestUtils.podHasVolume(resultFlinkPod.getPod(), "configmap-mount"));

		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "secret-mount1", "/opt/secret/1"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "secret-mount2", "/opt/secret/2"));
		assertFalse(VolumeTestUtils.containerHasVolume(baseFlinkPod.getMainContainer(), "configmap-mount", "/opt/configmap"));

		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "secret-mount1", "/opt/secret/1"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "secret-mount2", "/opt/secret/2"));
		assertTrue(VolumeTestUtils.containerHasVolume(resultFlinkPod.getMainContainer(), "configmap-mount", "/opt/configmap"));
	}
}
