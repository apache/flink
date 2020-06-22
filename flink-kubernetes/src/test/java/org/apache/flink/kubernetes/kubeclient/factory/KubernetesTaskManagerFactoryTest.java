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

import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import org.junit.Test;

import java.util.List;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link KubernetesTaskManagerFactory}.
 */
public class KubernetesTaskManagerFactoryTest extends KubernetesTaskManagerTestBase {

	private Pod resultPod;

	@Override
	protected void setupFlinkConfig() {
		super.setupFlinkConfig();

		flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_KEYTAB, kerberosDir.toString() + "/" + KEYTAB_FILE);
		flinkConfig.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, "test");
		flinkConfig.set(SecurityOptions.KERBEROS_KRB5_PATH, kerberosDir.toString() + "/" + KRB5_CONF_FILE);
	}

	@Override
	protected void onSetup() throws Exception {
		super.onSetup();

		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

		setHadoopConfDirEnv();
		generateHadoopConfFileItems();

		generateKerberosFileItems();

		this.resultPod =
			KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(kubernetesTaskManagerParameters).getInternalResource();
	}

	@Test
	public void testPod() {
		assertEquals(POD_NAME, this.resultPod.getMetadata().getName());
		assertEquals(5, this.resultPod.getMetadata().getLabels().size());
		assertEquals(4, this.resultPod.getSpec().getVolumes().size());
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

		assertEquals(3, resultMainContainer.getEnv().size());
		assertTrue(resultMainContainer.getEnv()
			.stream()
			.anyMatch(envVar -> envVar.getName().equals("key1")));

		assertEquals(1, resultMainContainer.getPorts().size());
		assertEquals(1, resultMainContainer.getCommand().size());
		assertEquals(2, resultMainContainer.getArgs().size());
		assertEquals(4, resultMainContainer.getVolumeMounts().size());
	}
}
