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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.cluster.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.kubeclient.KubeClient;

import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link KubernetesClusterDescriptor}.
 */
public class KubernetesClusterDescriptorTest extends KubernetesTestBase {

	@Test
	public void testDeploySessionCluster() throws Exception {

		String clusterId = "test-descriptor";

		final FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration());
		options.setClusterId(clusterId);

		KubeClient kubeClient = getClient(options);
		KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(options, kubeClient);

		setActionWatcher(clusterId);

		// configure slots
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1)
			.setTaskManagerMemoryMB(1)
			.setNumberTaskManagers(1)
			.setSlotsPerTaskManager(1)
			.createClusterSpecification();

		descriptor.deploySessionCluster(clusterSpecification);

		KubernetesClient internalClient = server.getClient();
		ServiceList serviceList = internalClient.services().list();
		assertEquals(1, serviceList.getItems().size());
		assertEquals(clusterId, serviceList.getItems().get(0).getMetadata().getName());
	}

	@Test
	public void testTryKillCluster() throws Exception {
		String clusterId = "test-descriptor";

		final FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration());
		options.setClusterId(clusterId);

		KubeClient kubeClient = getClient(options);
		KubernetesClusterDescriptor descriptor = new KubernetesClusterDescriptor(options, kubeClient);

		setActionWatcher(clusterId);

		// configure slots
		ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
			.setMasterMemoryMB(1)
			.setTaskManagerMemoryMB(1)
			.setNumberTaskManagers(1)
			.setSlotsPerTaskManager(1)
			.createClusterSpecification();

		descriptor.deploySessionCluster(clusterSpecification);

		KubernetesClient internalClient = server.getClient();
		assertEquals(1, internalClient.services().list().getItems().size());

		descriptor.killCluster(clusterId);
		assertEquals(0, internalClient.services().list().getItems().size());
	}

}
