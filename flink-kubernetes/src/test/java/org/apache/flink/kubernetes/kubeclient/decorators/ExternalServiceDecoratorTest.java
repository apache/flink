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
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * General tests for the {@link ExternalServiceDecorator}.
 */
public class ExternalServiceDecoratorTest extends KubernetesJobManagerTestBase {

	private ExternalServiceDecorator externalServiceDecorator;

	private Map<String, String> customizedAnnotations = new HashMap<String, String>() {
		{
			put("annotation1", "annotation-value1");
			put("annotation2", "annotation-value2");
		}
	};

	@Before
	public void setup() throws Exception {
		super.setup();

		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS, customizedAnnotations);
		this.externalServiceDecorator = new ExternalServiceDecorator(this.kubernetesJobManagerParameters);
	}

	@Test
	public void testBuildAccompanyingKubernetesResources() throws IOException {
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(1, resources.size());

		final Service restService = (Service) resources.get(0);

		assertEquals(Constants.API_VERSION, restService.getApiVersion());

		assertEquals(KubernetesUtils.getRestServiceName(CLUSTER_ID), restService.getMetadata().getName());

		final Map<String, String> expectedLabels = getCommonLabels();
		assertEquals(expectedLabels, restService.getMetadata().getLabels());

		assertEquals(KubernetesConfigOptions.ServiceExposedType.LoadBalancer.name(), restService.getSpec().getType());

		final List<ServicePort> expectedServicePorts = Collections.singletonList(
			new ServicePortBuilder()
				.withName(Constants.REST_PORT_NAME)
				.withPort(REST_PORT)
				.withNewTargetPort(Integer.valueOf(REST_BIND_PORT))
				.build());
		assertEquals(expectedServicePorts, restService.getSpec().getPorts());

		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		expectedLabels.putAll(userLabels);
		assertEquals(expectedLabels, restService.getSpec().getSelector());

		final Map<String, String> resultAnnotations = restService.getMetadata().getAnnotations();
		assertThat(resultAnnotations, is(equalTo(customizedAnnotations)));
	}

	@Test
	public void testSetServiceExposedType() throws IOException {
		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.NodePort);
		final List<HasMetadata> resources = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(KubernetesConfigOptions.ServiceExposedType.NodePort.name(),
			((Service) resources.get(0)).getSpec().getType());

		this.flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE, KubernetesConfigOptions.ServiceExposedType.ClusterIP);
		final List<HasMetadata> servicesWithClusterIP = this.externalServiceDecorator.buildAccompanyingKubernetesResources();
		assertEquals(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name(), ((Service) servicesWithClusterIP.get(0)).getSpec().getType());
	}
}
