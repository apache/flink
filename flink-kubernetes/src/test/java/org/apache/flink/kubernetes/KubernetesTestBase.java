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

import org.apache.flink.kubernetes.kubeclient.fabric8.Fabric8FlinkKubeClient;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.WatchEvent;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Rule;

import java.util.Arrays;
import java.util.UUID;

/**
 * Base test class for Kubernetes.
 */
public class KubernetesTestBase extends TestLogger {
	@Rule
	public MixedMockKubernetesServer server = new MixedMockKubernetesServer(false, true);

	protected Fabric8FlinkKubeClient getClient(FlinkKubernetesOptions options){
		KubernetesClient client = server.getClient();
		if (options.getServiceUUID() == null) {
			options.setServiceUUID(UUID.randomUUID().toString());
		}
		//only support namespace test
		options.setNameSpace("test");
		Fabric8FlinkKubeClient flinkKubeClient = new Fabric8FlinkKubeClient(options, client);
		flinkKubeClient.initialize();
		return flinkKubeClient;
	}

	protected void setActionWatcher(String resourceName) {

		ObjectMeta meta = new ObjectMeta();
		meta.setResourceVersion("1");
		meta.setNamespace(server.getClient().getNamespace());
		meta.setName(resourceName);

		Service serviceWithIngress = new ServiceBuilder()
			.withMetadata(meta)
			.withNewStatus()
			.withLoadBalancer(new LoadBalancerStatus(Arrays.asList(new LoadBalancerIngress("test-service", "192.168.0.1"))))
			.and()
			.build();

		String path = "/api/v1/namespaces/test/services?fieldSelector=metadata.name%3D" + resourceName + "&watch=true";
		server.expect()
			.withPath(path)
			.andUpgradeToWebSocket()
			.open()
			.waitFor(1000)
			.andEmit(new WatchEvent(serviceWithIngress, "ADDED"))
			.done()
			.once();

	}
}
