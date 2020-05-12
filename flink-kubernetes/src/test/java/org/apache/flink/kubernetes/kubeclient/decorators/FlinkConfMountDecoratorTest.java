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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link FlinkConfMountDecorator}.
 */
public class FlinkConfMountDecoratorTest extends KubernetesJobManagerTestBase {

	private static final String FLINK_CONF_DIR_IN_POD = "/opt/flink/flink-conf-";

	private FlinkConfMountDecorator flinkConfMountDecorator;

	@Before
	public void setup() throws Exception {
		super.setup();

		this.flinkConfig.set(KubernetesConfigOptions.FLINK_CONF_DIR, FLINK_CONF_DIR_IN_POD);

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
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final List<HasMetadata> additionalResources = flinkConfMountDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, additionalResources.size());

		final ConfigMap resultConfigMap = (ConfigMap) additionalResources.get(0);

		assertEquals(Constants.API_VERSION, resultConfigMap.getApiVersion());

		assertEquals(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID),
				resultConfigMap.getMetadata().getName());
		assertEquals(getCommonLabels(), resultConfigMap.getMetadata().getLabels());

		Map<String, String> resultDatas = resultConfigMap.getData();
		assertEquals("some data", resultDatas.get("logback.xml"));
		assertEquals("some data", resultDatas.get("log4j.properties"));
		assertTrue(resultDatas.get(FLINK_CONF_FILENAME).contains(KubernetesConfigOptions.FLINK_CONF_DIR.key() +
				": " + FLINK_CONF_DIR_IN_POD));
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
					.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
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
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("log4j.properties")
				.withPath("log4j.properties")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());
	}

	@Test
	public void testDecoratedFlinkPodWithLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("logback.xml")
				.withPath("logback.xml")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
				.withItems(expectedKeyToPaths)
				.endConfigMap()
				.build());
		assertEquals(expectedVolumes, resultFlinkPod.getPod().getSpec().getVolumes());
	}

	@Test
	public void testDecoratedFlinkPodWithLog4jAndLogback() throws IOException {
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");

		final FlinkPod resultFlinkPod = flinkConfMountDecorator.decorateFlinkPod(baseFlinkPod);

		final List<KeyToPath> expectedKeyToPaths = Arrays.asList(
			new KeyToPathBuilder()
				.withKey("logback.xml")
				.withPath("logback.xml")
				.build(),
			new KeyToPathBuilder()
				.withKey("log4j.properties")
				.withPath("log4j.properties")
				.build(),
			new KeyToPathBuilder()
				.withKey(FLINK_CONF_FILENAME)
				.withPath(FLINK_CONF_FILENAME)
				.build());
		final List<Volume> expectedVolumes = Collections.singletonList(
			new VolumeBuilder()
				.withName(Constants.FLINK_CONF_VOLUME)
				.withNewConfigMap()
				.withName(flinkConfMountDecorator.getFlinkConfConfigMapName(CLUSTER_ID))
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
