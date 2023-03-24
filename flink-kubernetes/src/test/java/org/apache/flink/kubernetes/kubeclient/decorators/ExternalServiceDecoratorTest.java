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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.kubeclient.services.HeadlessClusterIPService;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link ExternalServiceDecorator}. */
class ExternalServiceDecoratorTest extends KubernetesJobManagerTestBase {

    private ExternalServiceDecorator externalServiceDecorator;

    private Map<String, String> customizedAnnotations =
            new HashMap<String, String>() {
                {
                    put("annotation1", "annotation-value1");
                    put("annotation2", "annotation-value2");
                }
            };

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS, customizedAnnotations);
        this.externalServiceDecorator =
                new ExternalServiceDecorator(this.kubernetesJobManagerParameters);
    }

    @Test
    void testBuildAccompanyingKubernetesResources() throws IOException {
        final List<HasMetadata> resources =
                this.externalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(resources).hasSize(1);

        final Service restService = (Service) resources.get(0);

        assertThat(restService.getApiVersion()).isEqualTo(Constants.API_VERSION);

        assertThat(restService.getMetadata().getName())
                .isEqualTo(ExternalServiceDecorator.getExternalServiceName(CLUSTER_ID));

        final Map<String, String> expectedLabels = getCommonLabels();
        assertThat(restService.getMetadata().getLabels()).isEqualTo(expectedLabels);

        assertThat(restService.getSpec().getType())
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name());

        final List<ServicePort> expectedServicePorts =
                Collections.singletonList(
                        new ServicePortBuilder()
                                .withName(Constants.REST_PORT_NAME)
                                .withPort(REST_PORT)
                                .withNewTargetPort(Integer.valueOf(REST_BIND_PORT))
                                .build());
        assertThat(restService.getSpec().getPorts()).isEqualTo(expectedServicePorts);

        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        assertThat(restService.getSpec().getSelector()).isEqualTo(expectedLabels);

        final Map<String, String> resultAnnotations = restService.getMetadata().getAnnotations();
        assertThat(resultAnnotations).isEqualTo(customizedAnnotations);
    }

    @Test
    void testSetServiceExposedType() throws IOException {
        this.flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.NodePort);
        final List<HasMetadata> resources =
                this.externalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(((Service) resources.get(0)).getSpec().getType())
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.NodePort.name());

        this.flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.ClusterIP);
        final List<HasMetadata> servicesWithClusterIP =
                this.externalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(((Service) servicesWithClusterIP.get(0)).getSpec().getType())
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name());
    }

    @Test
    void testSetServiceExposedTypeWithHeadless() throws IOException {

        this.flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.Headless_ClusterIP);
        final List<HasMetadata> servicesWithHeadlessClusterIP =
                this.externalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(((Service) servicesWithHeadlessClusterIP.get(0)).getSpec().getType())
                .isEqualTo(KubernetesConfigOptions.ServiceExposedType.ClusterIP.name());
        assertThat(((Service) servicesWithHeadlessClusterIP.get(0)).getSpec().getClusterIP())
                .isEqualTo(HeadlessClusterIPService.HEADLESS_CLUSTER_IP);
    }
}
