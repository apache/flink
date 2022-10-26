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
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

import java.util.Optional;

/** The service type of ClusterIP. */
public class ClusterIPService extends ServiceType {

    public static final ClusterIPService INSTANCE = new ClusterIPService();

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
        int restPort = getRestPortFromExternalService(targetService);

        // Return the external service.namespace directly when using ClusterIP.
        return Optional.of(
                new Endpoint(KubernetesUtils.getNamespacedServiceName(targetService), restPort));
    }

    @Override
    public int getRestPort(ServicePort port) {
        return port.getPort();
    }

    @Override
    public String getType() {
        return KubernetesConfigOptions.ServiceExposedType.ClusterIP.name();
    }
}
