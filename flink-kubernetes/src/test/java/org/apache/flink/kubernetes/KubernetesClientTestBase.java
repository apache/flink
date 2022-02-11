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
import org.apache.flink.kubernetes.kubeclient.services.HeadlessClusterIPService;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.LoadBalancerStatus;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddressBuilder;
import io.fabric8.kubernetes.api.model.NodeBuilder;
import io.fabric8.kubernetes.api.model.NodeListBuilder;
import io.fabric8.kubernetes.api.model.NodeStatusBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.ServiceStatus;
import io.fabric8.kubernetes.api.model.ServiceStatusBuilder;
import io.fabric8.mockwebserver.dsl.DelayPathable;
import io.fabric8.mockwebserver.dsl.HttpMethodable;
import io.fabric8.mockwebserver.dsl.MockServerExpectation;
import io.fabric8.mockwebserver.dsl.ReturnOrWebsocketable;
import io.fabric8.mockwebserver.dsl.TimesOnceableOrHttpHeaderable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Base class for {@link KubernetesClusterDescriptorTest} and {@link
 * org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClientTest}.
 */
public class KubernetesClientTestBase extends KubernetesTestBase {

    protected static final int REST_PORT = 9021;
    protected static final int NODE_PORT = 31234;

    protected void mockExpectedNodesFromServerSide(List<String> addresses) {
        final List<Node> nodes = new ArrayList<>();
        Collections.shuffle(addresses);
        for (String address : addresses) {
            final String[] parts = address.split(":");
            Preconditions.checkState(
                    parts.length == 2, "Address should be in format \"<type>:<ip>\".");
            nodes.add(
                    new NodeBuilder()
                            .withStatus(
                                    new NodeStatusBuilder()
                                            .withAddresses(
                                                    new NodeAddressBuilder()
                                                            .withType(parts[0])
                                                            .withAddress(parts[1])
                                                            .build())
                                            .build())
                            .build());
        }
        server.expect()
                .get()
                .withPath("/api/v1/nodes")
                .andReturn(200, new NodeListBuilder().withItems(nodes).build())
                .always();
    }

    protected void mockExpectedServiceFromServerSide(Service expectedService) {
        final String serviceName = expectedService.getMetadata().getName();
        final String path =
                String.format("/api/v1/namespaces/%s/services/%s", NAMESPACE, serviceName);
        server.expect().get().withPath(path).andReturn(200, expectedService).always();
    }

    protected void mockCreateConfigMapAlreadyExisting(ConfigMap configMap) {
        final String path =
                String.format(
                        "/api/%s/namespaces/%s/configmaps",
                        configMap.getApiVersion(), configMap.getMetadata().getNamespace());
        server.expect().post().withPath(path).andReturn(500, configMap).always();
    }

    protected void mockGetConfigMapFailed(ConfigMap configMap) {
        mockConfigMapRequest(configMap, HttpMethodable::get);
    }

    protected void mockReplaceConfigMapFailed(ConfigMap configMap) {
        mockConfigMapRequest(configMap, HttpMethodable::put);
    }

    private void mockConfigMapRequest(
            ConfigMap configMap,
            Function<
                            MockServerExpectation,
                            DelayPathable<
                                    ReturnOrWebsocketable<TimesOnceableOrHttpHeaderable<Void>>>>
                    methodTypeSetter) {
        final String path =
                String.format(
                        "/api/%s/namespaces/%s/configmaps/%s",
                        configMap.getApiVersion(),
                        configMap.getMetadata().getNamespace(),
                        configMap.getMetadata().getName());
        methodTypeSetter.apply(server.expect()).withPath(path).andReturn(500, configMap).always();
    }

    protected Service buildExternalServiceWithLoadBalancer(
            @Nullable String hostname, @Nullable String ip) {
        final ServicePort servicePort =
                new ServicePortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withPort(REST_PORT)
                        .withNewTargetPort(REST_PORT)
                        .build();
        final ServiceStatus serviceStatus =
                new ServiceStatusBuilder()
                        .withLoadBalancer(
                                new LoadBalancerStatus(
                                        Collections.singletonList(
                                                new LoadBalancerIngress(
                                                        hostname, ip, new ArrayList<>()))))
                        .build();

        return buildExternalService(
                KubernetesConfigOptions.ServiceExposedType.LoadBalancer,
                servicePort,
                serviceStatus,
                false);
    }

    protected Service buildExternalServiceWithNodePort() {
        final ServicePort servicePort =
                new ServicePortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withPort(REST_PORT)
                        .withNodePort(NODE_PORT)
                        .withNewTargetPort(REST_PORT)
                        .build();

        final ServiceStatus serviceStatus =
                new ServiceStatusBuilder()
                        .withLoadBalancer(new LoadBalancerStatus(Collections.emptyList()))
                        .build();

        return buildExternalService(
                KubernetesConfigOptions.ServiceExposedType.NodePort,
                servicePort,
                serviceStatus,
                false);
    }

    protected Service buildExternalServiceWithClusterIP() {
        final ServicePort servicePort =
                new ServicePortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withPort(REST_PORT)
                        .withNewTargetPort(REST_PORT)
                        .build();

        return buildExternalService(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP, servicePort, null, false);
    }

    protected Service buildExternalServiceWithHeadlessClusterIP() {
        final ServicePort servicePort =
                new ServicePortBuilder()
                        .withName(Constants.REST_PORT_NAME)
                        .withPort(REST_PORT)
                        .withNewTargetPort(REST_PORT)
                        .build();

        return buildExternalService(
                KubernetesConfigOptions.ServiceExposedType.ClusterIP, servicePort, null, true);
    }

    private Service buildExternalService(
            KubernetesConfigOptions.ServiceExposedType serviceExposedType,
            ServicePort servicePort,
            @Nullable ServiceStatus serviceStatus,
            boolean isHeadlessSvc) {
        final ServiceBuilder serviceBuilder =
                new ServiceBuilder()
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

        if (isHeadlessSvc) {
            serviceBuilder
                    .editOrNewSpec()
                    .withClusterIP(HeadlessClusterIPService.HEADLESS_CLUSTER_IP)
                    .endSpec();
        }
        return serviceBuilder.build();
    }
}
