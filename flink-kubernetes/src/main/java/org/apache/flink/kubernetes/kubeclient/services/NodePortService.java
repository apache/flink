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
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.util.StringUtils;

import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/** The service type of NodePort. */
public class NodePortService extends ServiceType {

    private static final Logger LOG = LoggerFactory.getLogger(NodePortService.class);
    public static final NodePortService INSTANCE = new NodePortService();

    private NodePortService() {}

    @Override
    public Service buildUpInternalService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Endpoint> getRestEndpoint(
            Service targetService,
            NamespacedKubernetesClient internalClient,
            KubernetesConfigOptions.NodePortAddressType nodePortAddressType) {
        if (targetService.getStatus() == null) {
            return Optional.empty();
        }

        String address;
        // Node port is accessible on any node within kubernetes cluster. We'll
        // only consider IPs with the configured address type.
        address =
                internalClient.nodes().list().getItems().stream()
                        .filter(
                                node ->
                                        node.getSpec().getUnschedulable() == null
                                                || !node.getSpec().getUnschedulable())
                        .flatMap(node -> node.getStatus().getAddresses().stream())
                        .filter(
                                nodeAddress ->
                                        nodePortAddressType.name().equals(nodeAddress.getType()))
                        .map(NodeAddress::getAddress)
                        .filter(ip -> !ip.isEmpty())
                        .findAny()
                        .orElse(null);
        if (address == null) {
            LOG.warn(
                    "Unable to find any node ip with type [{}]. Please see [{}] config option for more details.",
                    nodePortAddressType,
                    KubernetesConfigOptions.REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE.key());
        }
        return StringUtils.isNullOrWhitespaceOnly(address)
                ? Optional.empty()
                : Optional.of(new Endpoint(address, getRestPortFromExternalService(targetService)));
    }

    @Override
    public int getRestPort(ServicePort port) {
        return port.getNodePort();
    }

    @Override
    public String getType() {
        return KubernetesConfigOptions.ServiceExposedType.NodePort.name();
    }
}
