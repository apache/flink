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

package org.apache.flink.kubernetes.kubeclient.fabric8;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.MixedMockKubernetesServer;
import org.apache.flink.kubernetes.kubeclient.Endpoint;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceList;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for Fabric implementation of KubeClient.
 * */
public class Fabric8ClientTest {

	@Rule
	public MixedMockKubernetesServer server = new MixedMockKubernetesServer(true, true);

	private Fabric8FlinkKubeClient getClient(FlinkKubernetesOptions options){
		KubernetesClient client = server.getClient();
		//only support namespace test
		options.setNamespace("test");
		Fabric8FlinkKubeClient flinkKubeClient = new Fabric8FlinkKubeClient(options, client);
		flinkKubeClient.initialize();
		return flinkKubeClient;
	}

	@Test
	public void testCreateClusterPod() {
		FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration(), "abc");
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

		ObjectMeta meta = new ObjectMeta();
		meta.setResourceVersion("1");

		Service serviceWithIngress = new ServiceBuilder()
			.withMetadata(meta)
			.withNewStatus()
			.withLoadBalancer(new LoadBalancerStatus(Arrays.asList(new LoadBalancerIngress("null", "192.168.0.1"))))
			.and()
			.build();

		server.expect()
			.withPath("/api/v1/namespaces/test/services?watch=true")
			.andUpgradeToWebSocket()
			.open()
			.waitFor(100)
			.andEmit(new WatchEvent(serviceWithIngress, "ADDED"))
			.done()
			.once();

		FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration(), "abc");
		Fabric8FlinkKubeClient flinkKubeClient = this.getClient(options);

		CompletableFuture<Endpoint> future = flinkKubeClient.createClusterService("abc");
		KubernetesClient client = server.getClient();
		ServiceList list = client.services().list();
		Assert.assertEquals(1, list.getItems().size());

		Service service = list.getItems().get(0);

		Assert.assertEquals("abc", service.getMetadata().getName());
		Assert.assertEquals(options.getNamespace(), service.getMetadata().getNamespace());

		List<ServicePort> ports = service.getSpec().getPorts();
		Assert.assertEquals(4, ports.size());
		Assert.assertTrue(ports.stream()
			.anyMatch(p -> p.getPort() == options.getConfiguration().getInteger(RestOptions.PORT)));

		Endpoint endpoint = future.get();
		Assert.assertEquals("192.168.0.1", endpoint.getAddress());
		Assert.assertEquals(options.getConfiguration().getInteger(RestOptions.PORT), endpoint.getPort());
	}
}
