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
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link FlinkConfMountDecorator}.
 */
public class FlinkResorceMountDecoratorTest extends KubernetesJobManagerTestBase {

	private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";

	private FlinkResorceMountDecorator flinkResorceMountDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		this.flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		// Additional parameters for resource mount
		kubernetesJobManagerParameters.getFlinkConfiguration().setString(KubernetesConfigOptions.JOBMANAGER_PVC_MOUNT, "pvc-mount:testclaim:/opt/pvc");
		kubernetesJobManagerParameters.getFlinkConfiguration().setString(KubernetesConfigOptions.JOBMANAGER_SECRET_MOUNT, "secret-mount1:testsecret1:/opt/secret/1,secret-mount2:testsecret2:/opt/secret/2");
		kubernetesJobManagerParameters.getFlinkConfiguration().setString(KubernetesConfigOptions.JOBMANAGER_CONFIGMAP_MOUNT, "configmap-mount:testconfigmap:/opt/configmap");

		this.flinkResorceMountDecorator = new FlinkResorceMountDecorator(kubernetesJobManagerParameters, KubernetesConfigOptions.JOBMANAGER_PVC_MOUNT,
			KubernetesConfigOptions.JOBMANAGER_SECRET_MOUNT, KubernetesConfigOptions.JOBMANAGER_CONFIGMAP_MOUNT);
	}

	@Test
	public void testResourceMount() throws IOException {
		final FlinkPod resultFlinkPod = flinkResorceMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<Volume> expectedvolumes = Arrays.asList(
			new VolumeBuilder().withName("pvc-mount")
				.withNewPersistentVolumeClaim()
				.withClaimName("testclaim").withNewReadOnly(false)
				.endPersistentVolumeClaim()
				.build(),
			new VolumeBuilder().withName("secret-mount1")
				.withNewSecret()
				.withSecretName("testsecret1")
				.endSecret()
				.build(),
			new VolumeBuilder().withName("secret-mount2")
				.withNewSecret()
				.withSecretName("testsecret2")
				.endSecret()
				.build(),
			new VolumeBuilder().withName("configmap-mount")
				.withNewConfigMap()
				.withName("testconfigmap")
				.endConfigMap()
				.build()
		);

		List<Volume> volumes = resultFlinkPod.getPod().getSpec().getVolumes();
		System.out.println(volumes);
		assertEquals(expectedvolumes, volumes);

		final List<VolumeMount> expectedmounts = Arrays.asList(
			new VolumeMountBuilder().withName("pvc-mount").withMountPath("/opt/pvc")
				.build(),
			new VolumeMountBuilder().withName("secret-mount1").withMountPath("/opt/secret/1")
				.build(),
			new VolumeMountBuilder().withName("secret-mount2").withMountPath("/opt/secret/2")
				.build(),
			new VolumeMountBuilder().withName("configmap-mount").withMountPath("/opt/configmap")
				.build()
		);

		List<VolumeMount> mounts = resultFlinkPod.getMainContainer().getVolumeMounts();
		assertEquals(expectedmounts, mounts);
		System.out.println(mounts);
	}
}
