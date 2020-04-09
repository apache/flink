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
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * General tests for the {@link InitJobManagerDecorator}.
 */
public class InitTaskManagerDecoratorTest extends KubernetesTaskManagerTestBase {

	private static final List<String> IMAGE_PULL_SECRETS = Arrays.asList("s1", "s2", "s3");
	private static final Map<String, String> ANNOTATIONS = new HashMap<String, String>() {
		{
			put("a1", "v1");
			put("a2", "v2");
		}
	};

	private Pod resultPod;
	private Container resultMainContainer;

	@Before
	public void setup() throws Exception {
		super.setup();
		this.flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS, IMAGE_PULL_SECRETS);
		this.flinkConfig.set(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS, ANNOTATIONS);

		final InitTaskManagerDecorator initTaskManagerDecorator =
			new InitTaskManagerDecorator(kubernetesTaskManagerParameters);

		final FlinkPod resultFlinkPod = initTaskManagerDecorator.decorateFlinkPod(this.baseFlinkPod);
		this.resultPod = resultFlinkPod.getPod();
		this.resultMainContainer = resultFlinkPod.getMainContainer();
	}

	@Test
	public void testApiVersion() {
		assertEquals(Constants.API_VERSION, this.resultPod.getApiVersion());
	}

	@Test
	public void testMainContainerName() {
		assertEquals(
			kubernetesTaskManagerParameters.getTaskManagerMainContainerName(),
			this.resultMainContainer.getName());
	}

	@Test
	public void testMainContainerImage() {
		assertEquals(CONTAINER_IMAGE, this.resultMainContainer.getImage());
	}

	@Test
	public void testMainContainerImagePullPolicy() {
		assertEquals(CONTAINER_IMAGE_PULL_POLICY.name(), this.resultMainContainer.getImagePullPolicy());
	}

	@Test
	public void testMainContainerResourceRequirements() {
		final ResourceRequirements resourceRequirements = this.resultMainContainer.getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertEquals(Double.toString(TASK_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(TOTAL_PROCESS_MEMORY + "Mi", requests.get("memory").getAmount());

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertEquals(Double.toString(TASK_MANAGER_CPU), limits.get("cpu").getAmount());
		assertEquals(TOTAL_PROCESS_MEMORY + "Mi", limits.get("memory").getAmount());
	}

	@Test
	public void testMainContainerPorts() {
		final List<ContainerPort> expectedContainerPorts = Collections.singletonList(
			new ContainerPortBuilder()
				.withName(Constants.TASK_MANAGER_RPC_PORT_NAME)
				.withContainerPort(RPC_PORT)
			.build());

		assertEquals(expectedContainerPorts, this.resultMainContainer.getPorts());
	}

	@Test
	public void testMainContainerEnv() {
		final Map<String, String> expectedEnvVars = new HashMap<>(customizedEnvs);
		expectedEnvVars.put(Constants.ENV_FLINK_POD_NAME, POD_NAME);

		final Map<String, String> resultEnvVars = this.resultMainContainer.getEnv()
			.stream()
			.collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));

		assertEquals(expectedEnvVars, resultEnvVars);
	}

	@Test
	public void testPodName() {
		assertEquals(POD_NAME, this.resultPod.getMetadata().getName());
	}

	@Test
	public void testPodLabels() {
		final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		expectedLabels.putAll(userLabels);

		assertEquals(expectedLabels, this.resultPod.getMetadata().getLabels());
	}

	@Test
	public void testPodAnnotations() {
		final Map<String, String> resultAnnotations = kubernetesTaskManagerParameters.getAnnotations();
		assertThat(resultAnnotations, is(equalTo(ANNOTATIONS)));
	}

	@Test
	public void testImagePullSecrets() {
		final List<String> resultSecrets = this.resultPod.getSpec().getImagePullSecrets()
			.stream()
			.map(LocalObjectReference::getName)
			.collect(Collectors.toList());

		assertEquals(IMAGE_PULL_SECRETS, resultSecrets);
	}

	@Test
	public void testNodeSelector() {
		assertThat(this.resultPod.getSpec().getNodeSelector(), is(equalTo(nodeSelector)));
	}
}
