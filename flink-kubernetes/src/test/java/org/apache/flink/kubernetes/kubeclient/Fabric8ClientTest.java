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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.kubeclient.fabric8.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.fabric8.FlinkService;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;

/**
 * Tests for Fabric implementation of {@link org.apache.flink.kubernetes.kubeclient.KubeClient}.
 * */
public class Fabric8ClientTest extends KubernetesTestBase {

	private final String clusterId = "abc";

	@Test
	public void testCreateClusterPod() {
		FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration());
		options.setClusterId(clusterId);
		Fabric8FlinkKubeClient flinkKubeClient = this.getClient(options);

		flinkKubeClient.createClusterPod();

		KubernetesClient client = server.getClient();
		PodList list = client.pods().list();
		Assert.assertEquals(1, list.getItems().size());

		Pod pod = list.getItems().get(0);

		Assert.assertEquals(options.getClusterId(), pod.getMetadata().getName());
		Assert.assertEquals(1, pod.getSpec().getContainers().size());
		Assert.assertEquals(4, pod.getSpec().getContainers().get(0).getPorts().size());
	}

	@Test
	public void testCreateService() throws  Exception {

		setActionWatcher(clusterId);

		FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration());
		options.setClusterId(clusterId);
		Fabric8FlinkKubeClient kubeClient = this.getClient(options);

		CompletableFuture<FlinkService> future = kubeClient.createClusterService(clusterId);

		KubernetesClient internalClient = server.getClient();
		ServiceList serviceList = internalClient.services().list();
		Assert.assertEquals(1, serviceList.getItems().size());

		Service service = serviceList.getItems().get(0);
		Assert.assertEquals(clusterId, service.getMetadata().getName());
		Assert.assertEquals(options.getNameSpace(), service.getMetadata().getNamespace());

		List<ServicePort> ports = service.getSpec().getPorts();
		Assert.assertEquals(4, ports.size());
		assertTrue(ports.stream().anyMatch(p -> p.getPort() == options.getConfiguration().getInteger(RestOptions.PORT)));

		FlinkService flinkService = future.get();
		Map<ConfigOption<Integer>, Endpoint> endpointMap = kubeClient.extractEndpoints(flinkService);
		Assert.assertEquals("192.168.0.1", endpointMap.get(RestOptions.PORT).getAddress());

	}
}
