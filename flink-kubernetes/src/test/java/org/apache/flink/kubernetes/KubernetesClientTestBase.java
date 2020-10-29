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

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;

import javax.annotation.Nullable;

import java.util.Collections;

/**
 * Base class for {@link KubernetesClusterDescriptorTest} and
 * {@link org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClientTest}.
 */
public class KubernetesClientTestBase extends KubernetesTestBase {

	protected static final int REST_PORT = 9021;
	protected static final int NODE_PORT = 31234;

	protected void mockExpectedServiceFromServerSide(Service expectedService) {
		final String serviceName = expectedService.getMetadata().getName();
		final String path = String.format("/api/v1/namespaces/%s/services/%s", NAMESPACE, serviceName);
		server.expect()
			.get()
			.withPath(path)
			.andReturn(200, expectedService)
			.always();
	}

	protected void mockCreateConfigMapAlreadyExisting(ConfigMap configMap) {
		final String path = String.format("/api/v1/namespaces/%s/configmaps", NAMESPACE);
		server.expect()
			.post()
			.withPath(path)
			.andReturn(500, configMap)
			.always();
	}

	protected void mockReplaceConfigMapFailed(ConfigMap configMap) {
		final String name = configMap.getMetadata().getName();
		final String path = String.format("/api/v1/namespaces/%s/configmaps/%s", NAMESPACE, name);
		server.expect()
			.put()
			.withPath(path)
			.andReturn(500, configMap)
			.always();
	}

	protected Service buildExternalServiceWithLoadBalancer(@Nullable String hostname, @Nullable String ip) {
		final ServicePort servicePort = new ServicePortBuilder()
			.withName(Constants.REST_PORT_NAME)
			.withPort(REST_PORT)
			.withNewTargetPort(REST_PORT)
			.build();
		final ServiceStatus serviceStatus = new ServiceStatusBuilder()
			.withLoadBalancer(new LoadBalancerStatus(Collections.singletonList(new LoadBalancerIngress(hostname, ip))))
			.build();

		return buildExternalService(
			KubernetesConfigOptions.ServiceExposedType.LoadBalancer,
			servicePort,
			serviceStatus);
	}

	protected Service buildExternalServiceWithNodePort() {
		final ServicePort servicePort = new ServicePortBuilder()
			.withName(Constants.REST_PORT_NAME)
			.withPort(REST_PORT)
			.withNodePort(NODE_PORT)
			.withNewTargetPort(REST_PORT)
			.build();

		final ServiceStatus serviceStatus = new ServiceStatusBuilder()
			.withLoadBalancer(new LoadBalancerStatus(Collections.emptyList()))
			.build();

		return buildExternalService(
			KubernetesConfigOptions.ServiceExposedType.NodePort,
			servicePort,
			serviceStatus);
	}

	protected Service buildExternalServiceWithClusterIP() {
		final ServicePort servicePort = new ServicePortBuilder()
			.withName(Constants.REST_PORT_NAME)
			.withPort(REST_PORT)
			.withNewTargetPort(REST_PORT)
			.build();

		return buildExternalService(
			KubernetesConfigOptions.ServiceExposedType.ClusterIP,
			servicePort,
			null);
	}

	private Service buildExternalService(
			KubernetesConfigOptions.ServiceExposedType serviceExposedType,
			ServicePort servicePort,
			@Nullable ServiceStatus serviceStatus) {
		final ServiceBuilder serviceBuilder = new ServiceBuilder()
			.editOrNewMetadata()
				.withName(ExternalServiceDecorator.getExternalServiceName(CLUSTER_ID))
				.endMetadata()
			.editOrNewSpec()
				.withType(serviceExposedType.name())
				.addNewPortLike(servicePort)
					.endPort()
				.endSpec();

		if (serviceStatus != null) {
			serviceBuilder.withStatus(serviceStatus);
		}

		return serviceBuilder.build();
	}
}
