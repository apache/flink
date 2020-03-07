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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.HadoopConfMountDecorator;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.PodSpec;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the Hadoop configuration setting decorated via {@link KubernetesJobManagerFactory}.
 */
public class KubernetesJobManagerHadoopConfTest extends KubernetesJobManagerFactoryTest {

	private static final String EXISTING_HADOOP_CONF_CONFIG_MAP = "hadoop-conf";

	@Before
	public void setup() throws Exception {
		super.setup();
	}

	@Test
	public void testExistingHadoopConfigMap() throws IOException {
		flinkConfig.set(KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);
		kubernetesJobManagerSpecification = KubernetesJobManagerFactory.createJobManagerComponent(kubernetesJobManagerParameters);

		assertFalse(kubernetesJobManagerSpecification.getAccompanyingResources().stream()
			.anyMatch(resource -> resource.getMetadata().getName().equals(HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID))));

		final PodSpec podSpec = kubernetesJobManagerSpecification.getDeployment().getSpec().getTemplate().getSpec();
		assertTrue(podSpec.getVolumes().stream().anyMatch(volume -> volume.getConfigMap().getName().equals(EXISTING_HADOOP_CONF_CONFIG_MAP)));
	}

	@Test
	public void testHadoopConfConfigMap() throws IOException {
		setHadoopConfDirEnv();
		generateHadoopConfFileItems();
		kubernetesJobManagerSpecification = KubernetesJobManagerFactory.createJobManagerComponent(kubernetesJobManagerParameters);

		final ConfigMap resultConfigMap = (ConfigMap) kubernetesJobManagerSpecification.getAccompanyingResources()
			.stream()
			.filter(x -> x instanceof ConfigMap &&
				x.getMetadata().getName().equals(HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID)))
			.collect(Collectors.toList())
			.get(0);

		assertEquals(2, resultConfigMap.getMetadata().getLabels().size());

		final Map<String, String> resultDatas = resultConfigMap.getData();
		assertEquals(2, resultDatas.size());
		assertEquals("some data", resultDatas.get("core-site.xml"));
		assertEquals("some data", resultDatas.get("hdfs-site.xml"));
	}

	@Test
	public void testEmptyHadoopConfDirectory() throws IOException {
		setHadoopConfDirEnv();
		kubernetesJobManagerSpecification = KubernetesJobManagerFactory.createJobManagerComponent(kubernetesJobManagerParameters);

		assertFalse(kubernetesJobManagerSpecification.getAccompanyingResources().stream()
			.anyMatch(resource -> resource.getMetadata().getName().equals(HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID))));
	}
}
