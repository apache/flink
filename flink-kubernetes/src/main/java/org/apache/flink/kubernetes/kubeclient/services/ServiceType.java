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

package org.apache.flink.kubernetes.kubeclient.services;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.Endpoint;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** An abstract class represents the service type that flink supported. */
public abstract class ServiceType {

    /**
     * Build up the external rest service template, according to the jobManager parameters.
     *
     * @param kubernetesJobManagerParameters the parameters of jobManager.
     * @return the external rest service
     */
    public Service buildUpExternalRestService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        final String serviceName =
                ExternalServiceDecorator.getExternalServiceName(
                        kubernetesJobManagerParameters.getClusterId());

        return new ServiceBuilder()
                .withApiVersion(Constants.API_VERSION)
                .withNewMetadata()
                .withName(serviceName)
                .withLabels(kubernetesJobManagerParameters.getCommonLabels())
                .withAnnotations(kubernetesJobManagerParameters.getRestServiceAnnotations())
                .endMetadata()
                .withNewSpec()
                .withType(
                        kubernetesJobManagerParameters
                                .getRestServiceExposedType()
                                .serviceType()
                                .getType())
                .withSelector(kubernetesJobManagerParameters.getSelectors())
                .addNewPort()
                .withName(Constants.REST_PORT_NAME)
                .withPort(kubernetesJobManagerParameters.getRestPort())
                .withNewTargetPort(kubernetesJobManagerParameters.getRestBindPort())
                .endPort()
                .endSpec()
                .build();
    }

    /**
     * Build up the internal service template, according to the jobManager parameters.
     *
     * @param kubernetesJobManagerParameters the parameters of jobManager.
     * @return the internal service
     */
    public abstract Service buildUpInternalService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters);

    /**
     * Gets the rest endpoint from the kubernetes service.
     *
     * @param targetService the target service to retrieve from.
     * @param internalClient the client to interact with api server.
     * @param nodePortAddressType the target address type of the node.
     * @return the rest endpoint.
     */
    public abstract Optional<Endpoint> getRestEndpoint(
            Service targetService,
            NamespacedKubernetesClient internalClient,
            KubernetesConfigOptions.NodePortAddressType nodePortAddressType);

    /**
     * Gets the rest port from the service port.
     *
     * @param port the target service port.
     * @return the rest port.
     */
    public abstract int getRestPort(ServicePort port);

    /**
     * Gets the type of the target kubernetes service.
     *
     * @return the type of the target kubernetes service.
     */
    public abstract String getType();

    /** Get rest port from the external Service. */
    public int getRestPortFromExternalService(Service externalService) {
        final List<ServicePort> servicePortCandidates =
                externalService.getSpec().getPorts().stream()
                        .filter(x -> x.getName().equals(Constants.REST_PORT_NAME))
                        .collect(Collectors.toList());

        if (servicePortCandidates.isEmpty()) {
            throw new RuntimeException(
                    "Failed to find port \""
                            + Constants.REST_PORT_NAME
                            + "\" in Service \""
                            + externalService.getMetadata().getName()
                            + "\"");
        }

        final ServicePort externalServicePort = servicePortCandidates.get(0);

        return getRestPort(externalServicePort);
    }

    // Helper method
    public static KubernetesConfigOptions.ServiceExposedType classify(Service service) {
        KubernetesConfigOptions.ServiceExposedType type =
                KubernetesConfigOptions.ServiceExposedType.valueOf(service.getSpec().getType());
        if (type == KubernetesConfigOptions.ServiceExposedType.ClusterIP) {
            if (HeadlessClusterIPService.HEADLESS_CLUSTER_IP.equals(
                    service.getSpec().getClusterIP())) {
                type = KubernetesConfigOptions.ServiceExposedType.Headless_ClusterIP;
            }
        }
        return type;
    }
}
