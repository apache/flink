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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** General tests for the {@link KubernetesJobManagerParameters}. */
class KubernetesJobManagerParametersTest extends KubernetesTestBase {

    private static final double JOB_MANAGER_CPU = 2.0;
    private static final double JOB_MANAGER_CPU_LIMIT_FACTOR = 2.5;
    private static final double JOB_MANAGER_MEMORY_LIMIT_FACTOR = 2.0;

    private final ClusterSpecification clusterSpecification =
            new ClusterSpecification.ClusterSpecificationBuilder()
                    .setMasterMemoryMB(JOB_MANAGER_MEMORY)
                    .setTaskManagerMemoryMB(1024)
                    .setSlotsPerTaskManager(1)
                    .createClusterSpecification();

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters =
            new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);

    @Test
    void testGetEnvironments() {
        final Map<String, String> expectedEnvironments = new HashMap<>();
        expectedEnvironments.put("k1", "v1");
        expectedEnvironments.put("k2", "v2");

        expectedEnvironments.forEach(
                (k, v) ->
                        flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + k, v));

        final Map<String, String> resultEnvironments =
                kubernetesJobManagerParameters.getEnvironments();

        assertThat(resultEnvironments).isEqualTo(expectedEnvironments);
    }

    @Test
    void testGetEmptyAnnotations() {
        assertThat(kubernetesJobManagerParameters.getAnnotations()).isEmpty();
    }

    @Test
    void testGetJobManagerAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>();
        expectedAnnotations.put("a1", "v1");
        expectedAnnotations.put("a2", "v2");

        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, expectedAnnotations);

        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getAnnotations();

        assertThat(resultAnnotations).isEqualTo(expectedAnnotations);
    }

    @Test
    void testGetServiceAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>();
        expectedAnnotations.put("a1", "v1");
        expectedAnnotations.put("a2", "v2");

        flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS, expectedAnnotations);

        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getRestServiceAnnotations();

        assertThat(resultAnnotations).isEqualTo(expectedAnnotations);
    }

    @Test
    void testGetJobManagerMemoryMB() {
        assertThat(kubernetesJobManagerParameters.getJobManagerMemoryMB())
                .isEqualTo(JOB_MANAGER_MEMORY);
    }

    @Test
    void testGetJobManagerCPU() {
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, JOB_MANAGER_CPU);
        assertThat(kubernetesJobManagerParameters.getJobManagerCPU())
                .isEqualTo(JOB_MANAGER_CPU, within(0.00001));
    }

    @Test
    void testGetJobManagerCPULimitFactor() {
        flinkConfig.set(
                KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR, JOB_MANAGER_CPU_LIMIT_FACTOR);
        assertThat(kubernetesJobManagerParameters.getJobManagerCPULimitFactor())
                .isEqualTo(JOB_MANAGER_CPU_LIMIT_FACTOR, within(0.00001));
    }

    @Test
    void testGetJobManagerMemoryLimitFactor() {
        flinkConfig.set(
                KubernetesConfigOptions.JOB_MANAGER_MEMORY_LIMIT_FACTOR,
                JOB_MANAGER_MEMORY_LIMIT_FACTOR);
        assertThat(kubernetesJobManagerParameters.getJobManagerMemoryLimitFactor())
                .isEqualTo(JOB_MANAGER_MEMORY_LIMIT_FACTOR, within(0.00001));
    }

    @Test
    void testGetRestPort() {
        flinkConfig.set(RestOptions.PORT, 12345);
        assertThat(kubernetesJobManagerParameters.getRestPort()).isEqualTo(12345);
    }

    @Test
    void testGetRpcPort() {
        flinkConfig.set(JobManagerOptions.PORT, 1234);
        assertThat(kubernetesJobManagerParameters.getRPCPort()).isEqualTo(1234);
    }

    @Test
    void testGetBlobServerPort() {
        flinkConfig.set(BlobServerOptions.PORT, "2345");
        assertThat(kubernetesJobManagerParameters.getBlobServerPort()).isEqualTo(2345);
    }

    @Test
    void testGetBlobServerPortException1() {
        flinkConfig.set(BlobServerOptions.PORT, "1000-2000");
        String errMsg =
                BlobServerOptions.PORT.key()
                        + " should be specified to a fixed port. Do not support a range of ports.";
        assertThatThrownBy(
                        () -> kubernetesJobManagerParameters.getBlobServerPort(),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(FlinkRuntimeException.class)
                                        .hasMessageMatching(errMsg));
    }

    @Test
    void testGetBlobServerPortException2() {
        flinkConfig.set(BlobServerOptions.PORT, "0");
        assertThatThrownBy(
                        () -> kubernetesJobManagerParameters.getBlobServerPort(),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(IllegalArgumentException.class)
                                        .hasMessageMatching(
                                                BlobServerOptions.PORT.key()
                                                        + " should not be 0."));
    }

    @Test
    void testGetServiceAccount() {
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, "flink");
        assertThat(kubernetesJobManagerParameters.getServiceAccount()).isEqualTo("flink");
    }

    @Test
    void testGetServiceAccountFallback() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, "flink-fallback");
        assertThat(kubernetesJobManagerParameters.getServiceAccount()).isEqualTo("flink-fallback");
    }

    @Test
    void testGetServiceAccountShouldReturnDefaultIfNotExplicitlySet() {
        assertThat(kubernetesJobManagerParameters.getServiceAccount()).isEqualTo("default");
    }

    @Test
    void testGetEntrypointMainClass() {
        final String entrypointClass = "org.flink.kubernetes.Entrypoint";
        flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, entrypointClass);
        assertThat(kubernetesJobManagerParameters.getEntrypointClass()).isEqualTo(entrypointClass);
    }

    @Test
    void testGetRestServiceExposedType() {
        flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.NodePort);
        assertThat(kubernetesJobManagerParameters.getRestServiceExposedType())
                .isEqualByComparingTo(KubernetesConfigOptions.ServiceExposedType.NodePort);
    }

    @Test
    void testPrioritizeBuiltInLabels() {
        final Map<String, String> userLabels = new HashMap<>();
        userLabels.put(Constants.LABEL_TYPE_KEY, "user-label-type");
        userLabels.put(Constants.LABEL_APP_KEY, "user-label-app");
        userLabels.put(Constants.LABEL_COMPONENT_KEY, "user-label-component-jm");

        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, userLabels);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        assertThat(kubernetesJobManagerParameters.getLabels()).isEqualTo(expectedLabels);
    }

    @Test
    public void testGetReplicasWithTwoShouldFailWhenHAIsNotEnabled() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        assertThatThrownBy(() -> kubernetesJobManagerParameters.getReplicas())
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    public void testGetReplicasWithInvalidValue() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 0);
        assertThatThrownBy(() -> kubernetesJobManagerParameters.getReplicas())
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testGetReplicas() {
        flinkConfig.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.KUBERNETES.name());
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        assertThat(kubernetesJobManagerParameters.getReplicas()).isEqualTo(2);
    }
}
