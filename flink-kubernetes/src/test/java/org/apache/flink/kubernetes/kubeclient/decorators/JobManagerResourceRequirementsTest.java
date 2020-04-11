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
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * General tests for the {@link InitJobManagerDecorator} regarding the k8s resource requirements.
 */
public class JobManagerResourceRequirementsTest extends KubernetesJobManagerTestBase {

	@Before
	public void setup() throws Exception {
		super.setup();
	}

	@Test
	public void testCpuAndMemory() {
		final double expectedCpuRequest = 1.2;
		final double expectedCpuLimit = 2.2;

		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, expectedCpuRequest);
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT, expectedCpuLimit);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final ResourceRequirements resourceRequirements =
			initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertThat(requests.get("cpu").getAmount(), is(Double.toString(expectedCpuRequest)));
		assertThat(requests.get("memory").getAmount(), is(JOB_MANAGER_MEMORY + "Mi"));

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertThat(limits.get("cpu").getAmount(), is(Double.toString(expectedCpuLimit)));
		assertThat(limits.get("memory").getAmount(), is(JOB_MANAGER_MEMORY + "Mi"));
	}

	@Test
	public void testCpuLimitUnsetThenFallbackToConfiguredCpuRequest() {
		final double expectedCpuRequest = 1.1;

		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, expectedCpuRequest);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final ResourceRequirements resourceRequirements =
			initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertThat(requests.get("cpu").getAmount(), is(Double.toString(expectedCpuRequest)));

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertThat(limits.get("cpu").getAmount(), is(Double.toString(expectedCpuRequest)));
	}

	@Test
	public void testCpuLimitUnsetThenFallbackToDefaultCpuRequest() {
		final double defaultCpuRequest = KubernetesConfigOptions.JOB_MANAGER_CPU.defaultValue();

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final ResourceRequirements resourceRequirements =
			initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertThat(requests.get("cpu").getAmount(), is(Double.toString(defaultCpuRequest)));

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertThat(limits.get("cpu").getAmount(), is(Double.toString(defaultCpuRequest)));
	}

	@Test
	public void testCpuLimitMustNotBeLessThanCpuRequest() {
		final double expectedCpuRequest = 1.1;
		final double expectedCpuLimit = 1.0;

		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, expectedCpuRequest);
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT, expectedCpuLimit);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		assertThrows(
			"must be less than or equal to",
			IllegalArgumentException.class,
			() -> initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources()
		);
	}

	@Test
	public void testCpuLimitMustNotBeLessThanZero() {
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT, -1.0);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		assertThrows(
			"must be greater than or equal to 0",
			IllegalArgumentException.class,
			() -> initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources()
		);
	}

	@Test
	public void testCpuLimitEqualsToZero() {
		// test only cpuLimit equals to 0
		testCpuLimitEqualsToZeroInternal(1.1, 0.0);

		// test both cpu request and limit equals to 0
		testCpuLimitEqualsToZeroInternal(0.0, 0.0);
	}

	@Test
	public void testOnlyCpuRequestEqualsToZero() {
		final double expectedCpuRequest = 0.0;
		final double expectedCpuLimit = 1.0;

		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, expectedCpuRequest);
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT, expectedCpuLimit);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final ResourceRequirements resourceRequirements =
			initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertThat(requests.get("cpu").getAmount(), is(equalTo(Double.toString(expectedCpuRequest))));

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertThat(limits.get("cpu").getAmount(), is(equalTo(Double.toString(expectedCpuLimit))));
	}

	private void testCpuLimitEqualsToZeroInternal(double expectedCpuRequest, double expectedCpuLimit) {
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, expectedCpuRequest);
		this.flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT, expectedCpuLimit);

		final InitJobManagerDecorator initJobManagerDecorator = new InitJobManagerDecorator(this.kubernetesJobManagerParameters);
		final ResourceRequirements resourceRequirements =
			initJobManagerDecorator.decorateFlinkPod(this.baseFlinkPod).getMainContainer().getResources();

		final Map<String, Quantity> requests = resourceRequirements.getRequests();
		assertThat(requests.get("cpu").getAmount(), is(equalTo(Double.toString(expectedCpuRequest))));

		final Map<String, Quantity> limits = resourceRequirements.getLimits();
		assertThat(limits.containsKey("cpu"), is(false));
	}
}
