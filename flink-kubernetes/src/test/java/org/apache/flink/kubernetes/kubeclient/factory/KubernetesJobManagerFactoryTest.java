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
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentSpec;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the {@link KubernetesJobManagerFactory}.
 */
public class KubernetesJobManagerFactoryTest extends KubernetesJobManagerTestBase {

	private static final String SERVICE_ACCOUNT_NAME = "service-test";
	private static final String ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private KubernetesJobManagerSpecification kubernetesJobManagerSpecification;

	@Before
	public void setup() throws Exception {
		super.setup();

		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);
		flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);

		this.kubernetesJobManagerSpecification =
			KubernetesJobManagerFactory.createJobManagerComponent(kubernetesJobManagerParameters);
	}

	@Test
	public void testDeploymentMetadata() {
		final Deployment resultDeployment = this.kubernetesJobManagerSpecification.getDeployment();
		assertEquals(Constants.APPS_API_VERSION, resultDeployment.getApiVersion());
		assertEquals(KubernetesUtils.getDeploymentName(CLUSTER_ID), resultDeployment.getMetadata().getName());
		final Map<String, String> expectedLabels = getCommonLabels();
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
		assertEquals(expectedLabels, resultDeployment.getMetadata().getLabels());
	}

	@Test
	public void testDeploymentSpec() {
		final DeploymentSpec resultDeploymentSpec = this.kubernetesJobManagerSpecification.getDeployment().getSpec();
		assertEquals(1, resultDeploymentSpec.getReplicas().intValue());

		final Map<String, String> expectedLabels =  new HashMap<>(getCommonLabels());
		expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);

		assertEquals(expectedLabels, resultDeploymentSpec.getTemplate().getMetadata().getLabels());
		assertEquals(expectedLabels, resultDeploymentSpec.getSelector().getMatchLabels());

		assertNotNull(resultDeploymentSpec.getTemplate().getSpec());
	}

	@Test
	public void testPodSpec() {
		final PodSpec resultPodSpec =
			this.kubernetesJobManagerSpecification.getDeployment().getSpec().getTemplate().getSpec();

		assertEquals(1, resultPodSpec.getContainers().size());
		assertEquals(SERVICE_ACCOUNT_NAME, resultPodSpec.getServiceAccountName());
		assertEquals(1, resultPodSpec.getVolumes().size());

		final Container resultedMainContainer = resultPodSpec.getContainers().get(0);
		assertEquals(KubernetesJobManagerParameters.JOB_MANAGER_MAIN_CONTAINER_NAME, resultedMainContainer.getName());
		assertEquals(CONTAINER_IMAGE, resultedMainContainer.getImage());
		assertEquals(CONTAINER_IMAGE_PULL_POLICY.name(), resultedMainContainer.getImagePullPolicy());

		assertEquals(3, resultedMainContainer.getEnv().size());
		assertTrue(resultedMainContainer.getEnv()
				.stream()
				.anyMatch(envVar -> envVar.getName().equals("key1")));

		assertEquals(3, resultedMainContainer.getPorts().size());

		final Map<String, Quantity> requests = resultedMainContainer.getResources().getRequests();
		assertEquals(Double.toString(JOB_MANAGER_CPU), requests.get("cpu").getAmount());
		assertEquals(JOB_MANAGER_MEMORY + "Mi", requests.get("memory").getAmount());

		assertEquals(1, resultedMainContainer.getCommand().size());
		assertEquals(3, resultedMainContainer.getArgs().size());

		assertEquals(1, resultedMainContainer.getVolumeMounts().size());
	}

	@Test
	public void testAdditionalResourcesSize() {
		final List<HasMetadata> resultAdditionalResources = this.kubernetesJobManagerSpecification.getAccompanyingResources();
		assertEquals(3, resultAdditionalResources.size());

		final List<HasMetadata> resultServices = resultAdditionalResources
			.stream()
			.filter(x -> x instanceof Service)
			.collect(Collectors.toList());
		assertEquals(2, resultServices.size());

		final List<HasMetadata> resultConfigMaps = resultAdditionalResources
			.stream()
			.filter(x -> x instanceof ConfigMap)
			.collect(Collectors.toList());
		assertEquals(1, resultConfigMaps.size());
	}

	@Test
	public void testServices() {
		final List<Service> resultServices = this.kubernetesJobManagerSpecification.getAccompanyingResources()
			.stream()
			.filter(x -> x instanceof Service)
			.map(x -> (Service) x)
			.collect(Collectors.toList());

		assertEquals(2, resultServices.size());

		final List<Service> internalServiceCandidates = resultServices
				.stream()
				.filter(x -> x.getMetadata().getName().equals(KubernetesUtils.getInternalServiceName(CLUSTER_ID)))
				.collect(Collectors.toList());
		assertEquals(1, internalServiceCandidates.size());

		final List<Service> restServiceCandidates = resultServices
				.stream()
				.filter(x -> x.getMetadata().getName().equals(KubernetesUtils.getRestServiceName(CLUSTER_ID)))
				.collect(Collectors.toList());
		assertEquals(1, restServiceCandidates.size());

		final Service resultInternalService = internalServiceCandidates.get(0);
		assertEquals(2, resultInternalService.getMetadata().getLabels().size());

		assertEquals(resultInternalService.getSpec().getType(), "ClusterIP");
		assertEquals(3, resultInternalService.getSpec().getPorts().size());
		assertEquals(3, resultInternalService.getSpec().getSelector().size());

		final Service resultRestService = restServiceCandidates.get(0);
		assertEquals(2, resultRestService.getMetadata().getLabels().size());

		assertEquals(resultRestService.getSpec().getType(), "LoadBalancer");
		assertEquals(1, resultRestService.getSpec().getPorts().size());
		assertEquals(3, resultRestService.getSpec().getSelector().size());
	}

	@Test
	public void testFlinkConfConfigMap() {
		final ConfigMap resultConfigMap = (ConfigMap) this.kubernetesJobManagerSpecification.getAccompanyingResources()
			.stream()
			.filter(x -> x instanceof ConfigMap)
			.collect(Collectors.toList())
			.get(0);

		assertEquals(2, resultConfigMap.getMetadata().getLabels().size());

		final Map<String, String> resultDatas = resultConfigMap.getData();
		assertEquals(3, resultDatas.size());
		assertEquals("some data", resultDatas.get("log4j.properties"));
		assertEquals("some data", resultDatas.get("logback.xml"));
		assertTrue(resultDatas.get(FLINK_CONF_FILENAME)
			.contains(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS.key() + ": " + ENTRY_POINT_CLASS));
	}
}
