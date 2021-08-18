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
import org.apache.flink.kubernetes.highavailability.KubernetesHaServicesFactory;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** General tests for the {@link KubernetesJobManagerParameters}. */
public class KubernetesJobManagerParametersTest extends KubernetesTestBase {

    private static final double JOB_MANAGER_CPU = 2.0;

    private final ClusterSpecification clusterSpecification =
            new ClusterSpecification.ClusterSpecificationBuilder()
                    .setMasterMemoryMB(JOB_MANAGER_MEMORY)
                    .setTaskManagerMemoryMB(1024)
                    .setSlotsPerTaskManager(1)
                    .createClusterSpecification();

    private final KubernetesJobManagerParameters kubernetesJobManagerParameters =
            new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);

    @Test
    public void testGetEnvironments() {
        final Map<String, String> expectedEnvironments = new HashMap<>();
        expectedEnvironments.put("k1", "v1");
        expectedEnvironments.put("k2", "v2");

        expectedEnvironments.forEach(
                (k, v) ->
                        flinkConfig.setString(
                                ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + k, v));

        final Map<String, String> resultEnvironments =
                kubernetesJobManagerParameters.getEnvironments();

        assertEquals(expectedEnvironments, resultEnvironments);
    }

    @Test
    public void testGetEmptyAnnotations() {
        assertTrue(kubernetesJobManagerParameters.getAnnotations().isEmpty());
    }

    @Test
    public void testGetJobManagerAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>();
        expectedAnnotations.put("a1", "v1");
        expectedAnnotations.put("a2", "v2");

        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS, expectedAnnotations);

        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getAnnotations();

        assertThat(resultAnnotations, is(equalTo(expectedAnnotations)));
    }

    @Test
    public void testGetServiceAnnotations() {
        final Map<String, String> expectedAnnotations = new HashMap<>();
        expectedAnnotations.put("a1", "v1");
        expectedAnnotations.put("a2", "v2");

        flinkConfig.set(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS, expectedAnnotations);

        final Map<String, String> resultAnnotations =
                kubernetesJobManagerParameters.getRestServiceAnnotations();

        assertThat(resultAnnotations, is(equalTo(expectedAnnotations)));
    }

    @Test
    public void testGetJobManagerMemoryMB() {
        assertEquals(JOB_MANAGER_MEMORY, kubernetesJobManagerParameters.getJobManagerMemoryMB());
    }

    @Test
    public void testGetJobManagerCPU() {
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, JOB_MANAGER_CPU);
        assertEquals(JOB_MANAGER_CPU, kubernetesJobManagerParameters.getJobManagerCPU(), 0.00001);
    }

    @Test
    public void testGetRestPort() {
        flinkConfig.set(RestOptions.PORT, 12345);
        assertEquals(12345, kubernetesJobManagerParameters.getRestPort());
    }

    @Test
    public void testGetRpcPort() {
        flinkConfig.set(JobManagerOptions.PORT, 1234);
        assertEquals(1234, kubernetesJobManagerParameters.getRPCPort());
    }

    @Test
    public void testGetBlobServerPort() {
        flinkConfig.set(BlobServerOptions.PORT, "2345");
        assertEquals(2345, kubernetesJobManagerParameters.getBlobServerPort());
    }

    @Test
    public void testGetBlobServerPortException1() {
        flinkConfig.set(BlobServerOptions.PORT, "1000-2000");

        try {
            kubernetesJobManagerParameters.getBlobServerPort();
            fail("Should fail with an exception.");
        } catch (FlinkRuntimeException e) {
            assertThat(
                    e.getMessage(),
                    containsString(
                            BlobServerOptions.PORT.key()
                                    + " should be specified to a fixed port. Do not support a range of ports."));
        }
    }

    @Test
    public void testGetBlobServerPortException2() {
        flinkConfig.set(BlobServerOptions.PORT, "0");

        try {
            kubernetesJobManagerParameters.getBlobServerPort();
            fail("Should fail with an exception.");
        } catch (IllegalArgumentException e) {
            assertThat(
                    e.getMessage(),
                    containsString(BlobServerOptions.PORT.key() + " should not be 0."));
        }
    }

    @Test
    public void testGetServiceAccount() {
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, "flink");
        assertThat(kubernetesJobManagerParameters.getServiceAccount(), is("flink"));
    }

    @Test
    public void testGetServiceAccountFallback() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, "flink-fallback");
        assertThat(kubernetesJobManagerParameters.getServiceAccount(), is("flink-fallback"));
    }

    @Test
    public void testGetServiceAccountShouldReturnDefaultIfNotExplicitlySet() {
        assertThat(kubernetesJobManagerParameters.getServiceAccount(), is("default"));
    }

    @Test
    public void testGetEntrypointMainClass() {
        final String entrypointClass = "org.flink.kubernetes.Entrypoint";
        flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, entrypointClass);
        assertEquals(entrypointClass, kubernetesJobManagerParameters.getEntrypointClass());
    }

    @Test
    public void testGetRestServiceExposedType() {
        flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.NodePort);
        assertEquals(
                KubernetesConfigOptions.ServiceExposedType.NodePort,
                kubernetesJobManagerParameters.getRestServiceExposedType());
    }

    @Test
    public void testPrioritizeBuiltInLabels() {
        final Map<String, String> userLabels = new HashMap<>();
        userLabels.put(Constants.LABEL_TYPE_KEY, "user-label-type");
        userLabels.put(Constants.LABEL_APP_KEY, "user-label-app");
        userLabels.put(Constants.LABEL_COMPONENT_KEY, "user-label-component-jm");

        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_LABELS, userLabels);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_JOB_MANAGER);
        assertThat(kubernetesJobManagerParameters.getLabels(), is(equalTo(expectedLabels)));
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testGetReplicasWithTwoShouldFailWhenHAIsNotEnabled() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        kubernetesJobManagerParameters.getReplicas();
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testGetReplicasWithInvalidValue() {
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 0);
        kubernetesJobManagerParameters.getReplicas();
    }

    @Test
    public void testGetReplicas() {
        flinkConfig.set(
                HighAvailabilityOptions.HA_MODE,
                KubernetesHaServicesFactory.class.getCanonicalName());
        flinkConfig.set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS, 2);
        assertThat(kubernetesJobManagerParameters.getReplicas(), is(2));
    }
}
