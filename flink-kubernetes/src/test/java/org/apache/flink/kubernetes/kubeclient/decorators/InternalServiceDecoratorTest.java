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

import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** General tests for the {@link InternalServiceDecorator}. */
class InternalServiceDecoratorTest extends KubernetesJobManagerTestBase {

    private InternalServiceDecorator internalServiceDecorator;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.internalServiceDecorator =
                new InternalServiceDecorator(this.kubernetesJobManagerParameters);
    }

    @Test
    void testBuildAccompanyingKubernetesResources() throws IOException {
        final List<HasMetadata> resources =
                this.internalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(resources).hasSize(1);

        assertThat(InternalServiceDecorator.getNamespacedInternalServiceName(CLUSTER_ID, NAMESPACE))
                .isEqualTo(this.flinkConfig.getString(JobManagerOptions.ADDRESS));

        final Service internalService = (Service) resources.get(0);

        assertThat(internalService.getApiVersion()).isEqualTo(Constants.API_VERSION);

        assertThat(internalService.getMetadata().getName())
                .isEqualTo(InternalServiceDecorator.getInternalServiceName(CLUSTER_ID));

        final Map<String, String> expectedLabels = getCommonLabels();
        assertThat(internalService.getMetadata().getLabels()).isEqualTo(expectedLabels);

        assertThat(internalService.getSpec().getType()).isNull();
        assertThat(internalService.getSpec().getClusterIP()).isEqualTo("None");

        List<ServicePort> expectedServicePorts =
                Arrays.asList(
                        new ServicePortBuilder()
                                .withName(Constants.JOB_MANAGER_RPC_PORT_NAME)
                                .withPort(RPC_PORT)
                                .build(),
                        new ServicePortBuilder()
                                .withName(Constants.BLOB_SERVER_PORT_NAME)
                                .withPort(BLOB_SERVER_PORT)
                                .build());
        assertThat(internalService.getSpec().getPorts()).isEqualTo(expectedServicePorts);

        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        assertThat(internalService.getSpec().getSelector()).isEqualTo(expectedLabels);
    }

    @Test
    void testDisableInternalService() throws IOException {
        this.flinkConfig.setString(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        final List<HasMetadata> resources =
                this.internalServiceDecorator.buildAccompanyingKubernetesResources();
        assertThat(resources).isEmpty();
    }
}
