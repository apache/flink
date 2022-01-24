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

import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

/** The service type of Headless ClusterIP, which is an variant of ClusterIP. */
public class HeadlessClusterIPService extends ClusterIPService {

    public static final HeadlessClusterIPService INSTANCE = new HeadlessClusterIPService();
    public static final String HEADLESS_CLUSTER_IP = "None";

    @Override
    public Service buildUpExternalRestService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        final Service service = super.buildUpExternalRestService(kubernetesJobManagerParameters);
        service.getSpec().setClusterIP(HEADLESS_CLUSTER_IP);
        return service;
    }

    @Override
    public Service buildUpInternalService(
            KubernetesJobManagerParameters kubernetesJobManagerParameters) {
        String serviceName =
                InternalServiceDecorator.getInternalServiceName(
                        kubernetesJobManagerParameters.getClusterId());
        return new ServiceBuilder()
                .withApiVersion(Constants.API_VERSION)
                .withNewMetadata()
                .withName(serviceName)
                .withLabels(kubernetesJobManagerParameters.getCommonLabels())
                .endMetadata()
                .withNewSpec()
                .withClusterIP(HEADLESS_CLUSTER_IP)
                .withSelector(kubernetesJobManagerParameters.getSelectors())
                .addNewPort()
                .withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
                .withPort(kubernetesJobManagerParameters.getRPCPort())
                .endPort()
                .addNewPort()
                .withName(Constants.BLOB_SERVER_PORT_NAME)
                .withPort(kubernetesJobManagerParameters.getBlobServerPort())
                .endPort()
                .endSpec()
                .build();
    }
}
