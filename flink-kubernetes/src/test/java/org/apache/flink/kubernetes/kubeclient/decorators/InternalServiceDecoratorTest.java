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

import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * General tests for the {@link InternalServiceDecorator}.
 */
public class InternalServiceDecoratorTest extends KubernetesJobManagerTestBase {

	private InternalServiceDecorator internalServiceDecorator;

	@Before
	public void setup() throws Exception {
		super.setup();
		this.internalServiceDecorator = new InternalServiceDecorator(this.kubernetesJobManagerParameters);
	}

	@Test
	public void testBuildAccompanyingKubernetesResources() throws IOException {
		final List<HasMetadata> resources = this.internalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, resources.size());

		assertEquals(
			KubernetesUtils.getInternalServiceName(CLUSTER_ID) + "." + NAMESPACE,
			this.flinkConfig.getString(JobManagerOptions.ADDRESS));

		final Service internalService = (Service) resources.get(0);

		assertEquals(Constants.API_VERSION, internalService.getApiVersion());

		assertEquals(KubernetesUtils.getInternalServiceName(CLUSTER_ID), internalService.getMetadata().getName());

		final Map<String, String> expectedLabels = getCommonLabels();
		assertEquals(expectedLabels, internalService.getMetadata().getLabels());

		assertEquals("ClusterIP", internalService.getSpec().getType());

		List<ServicePort> expectedServicePorts = Arrays.asList(
			new ServicePortBuilder()
				.withName(Constants.REST_PORT_NAME)
				.withPort(REST_PORT)
				.build(),
			new ServicePortBuilder()
				.withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
				.withPort(RPC_PORT)
				.build(),
			new ServicePortBuilder()
				.withName(Constants.BLOB_SERVER_PORT_NAME)
				.withPort(BLOB_SERVER_PORT)
				.build());
		assertEquals(expectedServicePorts, internalService.getSpec().getPorts());

		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		expectedLabels.putAll(userLabels);
		assertEquals(expectedLabels, internalService.getSpec().getSelector());
	}
}
