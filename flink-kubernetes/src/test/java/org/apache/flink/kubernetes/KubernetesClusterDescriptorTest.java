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

package org.apache.flink.kubernetes;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link KubernetesClusterDescriptor}.
 */
public class KubernetesClusterDescriptorTest extends KubernetesTestBase {
	private static final String MOCK_SERVICE_HOST_NAME = "mock-host-name-of-service";
	private static final String MOCK_SERVICE_IP = "192.168.0.1";

	private final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
		.createClusterSpecification();

	@Before
	public void setup() throws Exception {
		super.setup();

		mockGetRestServiceWithLoadBalancer(MOCK_SERVICE_HOST_NAME, MOCK_SERVICE_IP);
	}

	@Test
	public void testDeploySessionCluster() throws Exception {
		final ClusterClient<String> clusterClient = deploySessionCluster();
		// Check updated flink config options
		assertEquals(String.valueOf(Constants.BLOB_SERVER_PORT), flinkConfig.getString(BlobServerOptions.PORT));
		assertEquals(String.valueOf(Constants.TASK_MANAGER_RPC_PORT), flinkConfig.getString(TaskManagerOptions.RPC_PORT));
		assertEquals(KubernetesUtils.getInternalServiceName(CLUSTER_ID) + "." +
			NAMESPACE, flinkConfig.getString(JobManagerOptions.ADDRESS));

		final Deployment jmDeployment = kubeClient
			.apps()
			.deployments()
			.list()
			.getItems()
			.get(0);

		final Container jmContainer = jmDeployment
			.getSpec()
			.getTemplate()
			.getSpec()
			.getContainers()
			.get(0);

		assertEquals(
			clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getRequests().get(Constants.RESOURCE_NAME_MEMORY).getAmount());
		assertEquals(
			clusterSpecification.getMasterMemoryMB() + Constants.RESOURCE_UNIT_MB,
			jmContainer.getResources().getLimits().get(Constants.RESOURCE_NAME_MEMORY).getAmount());

		clusterClient.close();
	}

	@Test
	public void testDeployHighAvailabilitySessionCluster() throws ClusterDeploymentException {
		flinkConfig.setString(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.toString());
		final ClusterClient<String> clusterClient = deploySessionCluster();

		final KubernetesClient kubeClient = server.getClient();
		final Container jmContainer = kubeClient
			.apps()
			.deployments()
			.list()
			.getItems()
			.get(0)
			.getSpec()
			.getTemplate()
			.getSpec()
			.getContainers()
			.get(0);
		assertTrue(
			"Environment " + ENV_FLINK_POD_IP_ADDRESS + " should be set.",
			jmContainer.getEnv().stream()
				.map(EnvVar::getName)
				.collect(Collectors.toList())
				.contains(ENV_FLINK_POD_IP_ADDRESS));

		clusterClient.close();
	}

	@Test
	public void testKillCluster() throws Exception {
		final KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(flinkConfig, flinkKubeClient);

		final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.createClusterSpecification();

		descriptor.deploySessionCluster(clusterSpecification);

		final KubernetesClient kubeClient = server.getClient();
		assertEquals(2, kubeClient.services().list().getItems().size());

		descriptor.killCluster(CLUSTER_ID);

		// Mock kubernetes server do not delete the accompanying resources by gc.
		assertTrue(kubeClient.apps().deployments().list().getItems().isEmpty());
		assertEquals(2, kubeClient.services().list().getItems().size());
		assertEquals(1, kubeClient.configMaps().list().getItems().size());
	}

	private ClusterClient<String> deploySessionCluster() throws ClusterDeploymentException {
		final KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(flinkConfig, flinkKubeClient);

		final ClusterClient<String> clusterClient = descriptor
			.deploySessionCluster(clusterSpecification)
			.getClusterClient();

		assertEquals(CLUSTER_ID, clusterClient.getClusterId());
		// Both HA and non-HA mode, the web interface should always be the Kubernetes exposed service address.
		assertEquals(String.format("http://%s:8081", MOCK_SERVICE_IP), clusterClient.getWebInterfaceURL());

		return clusterClient;
	}

	private void mockGetRestServiceWithLoadBalancer(String hostname, String ip) {
		final String restServiceName = KubernetesUtils.getRestServiceName(CLUSTER_ID);

		final String path = String.format("/api/v1/namespaces/%s/services/%s", NAMESPACE, restServiceName);
		server.expect()
			.get()
			.withPath(path)
			.andReturn(200, buildMockRestService(hostname, ip))
			.always();
	}

	private Service buildMockRestService(String hostname, String ip) {
		final Service service = new ServiceBuilder()
			.editOrNewMetadata()
				.withName(KubernetesUtils.getRestServiceName(CLUSTER_ID))
				.endMetadata()
			.editOrNewSpec()
				.endSpec()
			.build();

		service.setStatus(new ServiceStatusBuilder()
			.withLoadBalancer(new LoadBalancerStatus(Collections.singletonList(
				new LoadBalancerIngress(hostname, ip)))).build());

		return service;
	}
}
