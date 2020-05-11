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

import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link KubernetesTaskManagerFactory}.
 */
public class KubernetesTaskManagerFactoryTest extends KubernetesTaskManagerTestBase {

	private Pod resultPod;

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		setHadoopConfDirEnv();
		generateHadoopConfFileItems();

		this.resultPod =
			KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(kubernetesTaskManagerParameters).getInternalResource();
	}

	@Test
	public void testPod() {
		assertEquals(POD_NAME, this.resultPod.getMetadata().getName());
		assertEquals(5, this.resultPod.getMetadata().getLabels().size());
		assertEquals(2, this.resultPod.getSpec().getVolumes().size());
	}

	@Test
	public void testContainer() {
		final List<Container> resultContainers = this.resultPod.getSpec().getContainers();
		assertEquals(1, resultContainers.size());

		final Container resultMainContainer = resultContainers.get(0);
		assertEquals(
			KubernetesTaskManagerParameters.TASK_MANAGER_MAIN_CONTAINER_NAME,
			resultMainContainer.getName());
		assertEquals(CONTAINER_IMAGE, resultMainContainer.getImage());
		assertEquals(CONTAINER_IMAGE_PULL_POLICY.name(), resultMainContainer.getImagePullPolicy());

		assertEquals(4, resultMainContainer.getEnv().size());
		assertTrue(resultMainContainer.getEnv()
			.stream()
			.anyMatch(envVar -> envVar.getName().equals("key1")));

		assertEquals(1, resultMainContainer.getPorts().size());
		assertEquals(1, resultMainContainer.getCommand().size());
		assertEquals(3, resultMainContainer.getArgs().size());
		assertEquals(2, resultMainContainer.getVolumeMounts().size());
	}
}
