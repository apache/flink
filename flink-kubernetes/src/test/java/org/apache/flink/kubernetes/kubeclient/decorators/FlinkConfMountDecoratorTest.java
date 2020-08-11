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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.kubeclient.decorators.FlinkConfMountDecorator.getFlinkConfConfigMapName;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * General tests for the {@link FlinkConfMountDecorator}.
 */
public class FlinkConfMountDecoratorTest extends KubernetesJobManagerTestBase {

	private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";

	private FlinkConfMountDecorator flinkConfMountDecorator;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		this.flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		this.flinkConfMountDecorator = new FlinkConfMountDecorator(kubernetesJobManagerParameters);
	}

	@Test
	public void testWhetherPodOrContainerIsDecorated() {
		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);
		assertNotEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
		assertNotEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
	}

	@Test
	public void testConfigMap() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final List<HasMetadata> additionalResources = flinkConfMountDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, additionalResources.size());

		final ConfigMap resultConfigMap = (ConfigMap) additionalResources.get(0);

		assertEquals(Constants.API_VERSION, resultConfigMap.getApiVersion());

		assertEquals(getFlinkConfConfigMapName(CLUSTER_ID),
				resultConfigMap.getMetadata().getName());
		assertEquals(getCommonLabels(), resultConfigMap.getMetadata().getLabels());

		Map<String, String> resultDatas = resultConfigMap.getData();
		assertEquals("some data", resultDatas.get(CONFIG_FILE_LOGBACK_NAME));
		assertEquals("some data", resultDatas.get(CONFIG_FILE_LOG4J_NAME));

		final Configuration resultFlinkConfig = KubernetesTestUtils.loadConfigurationFromString(
			resultDatas.get(FLINK_CONF_FILENAME));
		assertThat(resultFlinkConfig.get(KubernetesConfigOptions.FLINK_CONF_DIR), is(FLINK_CONF_DIR_IN_POD));
		// The following config options should not be added to config map
		assertThat(resultFlinkConfig.get(KubernetesConfigOptions.KUBE_CONFIG_FILE), is(nullValue()));
		assertThat(resultFlinkConfig.get(DeploymentOptionsInternal.CONF_DIR), is(nullValue()));
	}

	@Test
	public void testDecoratedFlinkPodWithoutLog4jAndLogback() {
		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Collections.singletonList(
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
					.withName(getFlinkConfConfigMapName(CLUSTER_ID))
					.withItems(expectedKeyToPaths)
					.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());

		final List<VolumeMount> expectedVolumeMounts = Collections.singletonList(
			new VolumeMountBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withMountPath(FLINK_CONF_DIR_IN_POD)
			.build());
		assertEquals(expectedVolumeMounts, resultFlinkPod.getMainContainer().getVolumeMounts());
	}

	@Test
	public void testDecoratedFlinkPodWithLog4j() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey(CONFIG_FILE_LOG4J_NAME)
				.withPath(CONFIG_FILE_LOG4J_NAME)
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());
	}

	@Test
	public void testDecoratedFlinkPodWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey(CONFIG_FILE_LOGBACK_NAME)
				.withPath(CONFIG_FILE_LOGBACK_NAME)
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());
	}

	@Test
	public void testDecoratedFlinkPodWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey(CONFIG_FILE_LOGBACK_NAME)
				.withPath(CONFIG_FILE_LOGBACK_NAME)
				.build(),
			new KeyToPathBuilder()
				.withKey(CONFIG_FILE_LOG4J_NAME)
				.withPath(CONFIG_FILE_LOG4J_NAME)
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());
	}

	@Test
	public void testDecoratedFlinkContainer() {
		final Container resultMainContainer = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod).getMainContainer();
		assertEquals(1, resultMainContainer.getVolumeMounts().size());

		final VolumeMount volumeMount = resultMainContainer.getVolumeMounts().get(0);
		assertEquals(Constants.FLINK_CONF_VOLUME, volumeMount.getName());
		assertEquals(FLINK_CONF_DIR_IN_POD, volumeMount.getMountPath());
	}
}
