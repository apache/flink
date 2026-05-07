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

import org.apache.flink.kubernetes.KubernetesClientTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ServiceType}. */
class ServiceTypeTest extends KubernetesClientTestBase {

    @Test
    void testServiceClassify() {
        assertThat(ServiceType.classify(buildExternalServiceWithClusterIP()))
                .isEqualByComparingTo(KubernetesConfigOptions.ServiceExposedType.ClusterIP);
        assertThat(ServiceType.classify(buildExternalServiceWithHeadlessClusterIP()))
                .isEqualByComparingTo(
                        KubernetesConfigOptions.ServiceExposedType.Headless_ClusterIP);
        assertThat(ServiceType.classify(buildExternalServiceWithNodePort()))
                .isEqualByComparingTo(KubernetesConfigOptions.ServiceExposedType.NodePort);
        assertThat(ServiceType.classify(buildExternalServiceWithLoadBalancer("", "")))
                .isEqualByComparingTo(KubernetesConfigOptions.ServiceExposedType.LoadBalancer);
    }

    @Test
    void testGetRestPortFromExternalServiceWithDefaultName() {
        final int restPort =
                ClusterIPService.INSTANCE.getRestPortFromExternalService(
                        buildExternalServiceWithClusterIP());
        assertThat(restPort).isEqualTo(REST_PORT);
    }

    @Test
    void testGetRestPortFromExternalServiceWithCustomName() {
        final Service service =
                new ServiceBuilder()
                        .withNewMetadata()
                        .withName("flink-cluster-rest")
                        .endMetadata()
                        .withNewSpec()
                        .withType(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name())
                        .addNewPort()
                        .withName("flink-rest")
                        .withPort(REST_PORT)
                        .withNewTargetPort(REST_PORT)
                        .endPort()
                        .endSpec()
                        .build();

        final int restPort = ClusterIPService.INSTANCE.getRestPortFromExternalService(service);
        assertThat(restPort).isEqualTo(REST_PORT);
    }

    @Test
    void testGetRestPortFromExternalServiceFailsWhenNoPorts() {
        final Service service =
                new ServiceBuilder()
                        .withNewMetadata()
                        .withName("flink-cluster-rest")
                        .endMetadata()
                        .withNewSpec()
                        .withType(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name())
                        .endSpec()
                        .build();

        assertThatThrownBy(() -> ClusterIPService.INSTANCE.getRestPortFromExternalService(service))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to find any port")
                .hasMessageContaining("flink-cluster-rest");
    }

    @Test
    void testGetRestPortFromExternalServiceFailsWhenMultiplePorts() {
        final Service service =
                new ServiceBuilder()
                        .withNewMetadata()
                        .withName("flink-cluster-rest")
                        .endMetadata()
                        .withNewSpec()
                        .withType(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name())
                        .addNewPort()
                        .withName("rest")
                        .withPort(REST_PORT)
                        .withNewTargetPort(REST_PORT)
                        .endPort()
                        .addNewPort()
                        .withName("extra")
                        .withPort(REST_PORT + 1)
                        .withNewTargetPort(REST_PORT + 1)
                        .endPort()
                        .endSpec()
                        .build();

        assertThatThrownBy(() -> ClusterIPService.INSTANCE.getRestPortFromExternalService(service))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Expected exactly one port")
                .hasMessageContaining("flink-cluster-rest")
                .hasMessageContaining("but found 2");
    }
}
