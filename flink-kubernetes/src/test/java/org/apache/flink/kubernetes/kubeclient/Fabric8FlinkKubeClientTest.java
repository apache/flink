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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesJobManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Fabric implementation of {@link FlinkKubeClient}.
 */
public class Fabric8FlinkKubeClientTest extends KubernetesTestBase {
	private static final int REST_PORT = 9081;
	private static final int RPC_PORT = 7123;
	private static final int BLOB_SERVER_PORT = 8346;

	private static final double JOB_MANAGER_CPU = 2.0;
	private static final int JOB_MANAGER_MEMORY = 768;

	private static final String SERVICE_ACCOUNT_NAME = "service-test";

	private static final String ENTRY_POINT_CLASS = KubernetesSessionClusterEntrypoint.class.getCanonicalName();

	private KubernetesJobManagerSpecification kubernetesJobManagerSpecification;

	@Before
	public void setup() throws Exception {
		super.setup();

		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "logback.xml");
		KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, "log4j.properties");

		flinkConfig.set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, CONTAINER_IMAGE_PULL_POLICY);
		flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);
		flinkConfig.set(RestOptions.PORT, REST_PORT);
		flinkConfig.set(JobManagerOptions.PORT, RPC_PORT);
		flinkConfig.set(BlobServerOptions.PORT, Integer.toString(BLOB_SERVER_PORT));
		flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, JOB_MANAGER_CPU);
		flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);

		final ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(JOB_MANAGER_MEMORY)
			.setTaskManagerMemoryMB(1000)
			.setSlotsPerTaskManager(3)
			.createClusterSpecification();

		final KubernetesJobManagerParameters kubernetesJobManagerParameters =
			new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);
		this.kubernetesJobManagerSpecification =
			KubernetesJobManagerFactory.createJobManagerComponent(kubernetesJobManagerParameters);
	}

	@Test
	public void testCreateFlinkMasterComponent() throws Exception {
		flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

		final List<Deployment> resultedDeployments = kubeClient.apps().deployments()
			.inNamespace(NAMESPACE)
			.list()
			.getItems();
		assertEquals(1, resultedDeployments.size());

		final List<ConfigMap> resultedConfigMaps = kubeClient.configMaps()
			.inNamespace(NAMESPACE)
			.list()
			.getItems();
		assertEquals(1, resultedConfigMaps.size());

		final List<Service> resultedServices = kubeClient.services()
			.inNamespace(NAMESPACE)
			.list()
			.getItems();
		assertEquals(2, resultedServices.size());

		testOwnerReferenceSetting(resultedDeployments.get(0), resultedConfigMaps);
		testOwnerReferenceSetting(resultedDeployments.get(0), resultedServices);
	}

	private <T extends HasMetadata> void testOwnerReferenceSetting(
		HasMetadata ownerReference,
		List<T> resources) {
		resources.forEach(resource -> {
			List<OwnerReference> ownerReferences = resource.getMetadata().getOwnerReferences();
			assertEquals(1, ownerReferences.size());
			assertEquals(ownerReference.getMetadata().getUid(), ownerReferences.get(0).getUid());
		});
	}

	@Test
	public void testCreateFlinkTaskManagerPod() throws Exception {
		this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

		final KubernetesPod kubernetesPod = new KubernetesPod(new PodBuilder()
			.editOrNewMetadata()
			.withName("mock-task-manager-pod")
			.endMetadata()
			.editOrNewSpec()
			.endSpec()
			.build());
		this.flinkKubeClient.createTaskManagerPod(kubernetesPod);

		final Pod resultTaskManagerPod =
			this.kubeClient.pods().inNamespace(NAMESPACE).withName("mock-task-manager-pod").get();

		assertEquals(
			this.kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems().get(0).getMetadata().getUid(),
			resultTaskManagerPod.getMetadata().getOwnerReferences().get(0).getUid());
	}

	@Test
	public void testStopPod() {
		final String podName = "pod-for-delete";
		final Pod pod = new PodBuilder()
			.editOrNewMetadata()
			.withName(podName)
			.endMetadata()
			.editOrNewSpec()
			.endSpec()
			.build();

		this.kubeClient.pods().inNamespace(NAMESPACE).create(pod);
		assertNotNull(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get());

		this.flinkKubeClient.stopPod(podName);
		assertNull(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get());
	}

	@Test
	public void testServiceLoadBalancerWithNoIP() {
		final String hostName = "test-host-name";
		mockRestServiceWithLB(hostName, "");

		final Endpoint resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);

		assertEquals(hostName, resultEndpoint.getAddress());
		assertEquals(REST_PORT, resultEndpoint.getPort());
	}

	@Test
	public void testServiceLoadBalancerEmptyHostAndIP() {
		mockRestServiceWithLB("", "");

		final Endpoint resultEndpoint1 = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
		assertNull(resultEndpoint1);
	}

	@Test
	public void testServiceLoadBalancerNullHostAndIP() {
		mockRestServiceWithLB(null, null);

		final Endpoint resultEndpoint2 = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
		assertNull(resultEndpoint2);
	}

	@Test
	public void testStopAndCleanupCluster() throws Exception {
		this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

		final KubernetesPod kubernetesPod = new KubernetesPod(new PodBuilder()
			.editOrNewMetadata()
			.withName("mock-task-manager-pod")
			.endMetadata()
			.editOrNewSpec()
			.endSpec()
			.build());
		this.flinkKubeClient.createTaskManagerPod(kubernetesPod);

		assertEquals(1, this.kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems().size());
		assertEquals(1, this.kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems().size());
		assertEquals(2, this.kubeClient.services().inNamespace(NAMESPACE).list().getItems().size());
		assertEquals(1, this.kubeClient.pods().inNamespace(NAMESPACE).list().getItems().size());

		this.flinkKubeClient.stopAndCleanupCluster(CLUSTER_ID);
		assertTrue(this.kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems().isEmpty());
	}

	private void mockRestServiceWithLB(@Nullable String hostname, @Nullable String ip) {
		final String restServiceName = KubernetesUtils.getRestServiceName(CLUSTER_ID);

		final String path = String.format("/api/v1/namespaces/%s/services/%s", NAMESPACE, restServiceName);
		server.expect()
			.withPath(path)
			.andReturn(200, buildMockRestServiceWithLB(hostname, ip))
			.always();

		final Service resultService = kubeClient.services()
			.inNamespace(NAMESPACE)
			.withName(KubernetesUtils.getRestServiceName(CLUSTER_ID))
			.get();
		assertNotNull(resultService);
	}

	private Service buildMockRestServiceWithLB(@Nullable String hostname, @Nullable String ip) {
		final Service service = new ServiceBuilder()
			.build();

		service.setStatus(new ServiceStatusBuilder()
			.withLoadBalancer(new LoadBalancerStatus(Collections.singletonList(
				new LoadBalancerIngress(hostname, ip)))).build());

		return service;
	}
}
