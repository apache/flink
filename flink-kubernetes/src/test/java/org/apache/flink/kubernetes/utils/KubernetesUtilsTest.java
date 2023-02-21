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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KubernetesUtils}. */
class KubernetesUtilsTest extends KubernetesTestBase {

    private static final FlinkPod EMPTY_POD = new FlinkPod.Builder().build();

    @Test
    void testParsePortRange() {
        final Configuration cfg = new Configuration();
        cfg.set(BlobServerOptions.PORT, "50100-50200");
        assertThatThrownBy(
                        () -> KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(FlinkRuntimeException.class)
                                        .hasMessageContaining(BlobServerOptions.PORT.key()));
    }

    @Test
    void testParsePortNull() {
        final Configuration cfg = new Configuration();
        ConfigOption<String> testingPort =
                ConfigOptions.key("test.port").stringType().noDefaultValue();
        assertThatThrownBy(
                        () -> KubernetesUtils.parsePort(cfg, testingPort),
                        "Should fail with an exception.")
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(NullPointerException.class)
                                        .hasMessageContaining(
                                                testingPort.key() + " should not be null."));
    }

    @Test
    void testCheckWithDynamicPort() {
        testCheckAndUpdatePortConfigOption("0", "6123", "6123");
    }

    @Test
    void testCheckWithFixedPort() {
        testCheckAndUpdatePortConfigOption("6123", "16123", "6123");
    }

    @Test
    void testLoadPodFromNoSpecTemplate() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getNoSpecPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer()).isEqualTo(EMPTY_POD.getMainContainer());
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(0);
    }

    @Test
    void testLoadPodFromTemplateWithNonExistPathShouldFail() {
        final String nonExistFile = "/path/of/non-exist.yaml";
        final String msg = String.format("Pod template file %s does not exist.", nonExistFile);
        assertThatThrownBy(
                        () ->
                                KubernetesUtils.loadPodFromTemplateFile(
                                        flinkKubeClient,
                                        new File(nonExistFile),
                                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME),
                        "Kubernetes client should fail when the pod template file does not exist.")
                .satisfies(FlinkAssertions.anyCauseMatches(msg));
    }

    @Test
    void testLoadPodFromTemplateWithNoMainContainerShouldReturnEmptyMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        "nonExistMainContainer");
        assertThat(flinkPod.getMainContainer()).isEqualTo(EMPTY_POD.getMainContainer());
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(2);
    }

    @Test
    void testLoadPodFromTemplateAndCheckMetaData() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        // The pod name is defined in the test/resources/testing-pod-template.yaml.
        final String expectedPodName = "pod-template";
        assertThat(flinkPod.getPodWithoutMainContainer().getMetadata().getName())
                .isEqualTo(expectedPodName);
    }

    @Test
    void testLoadPodFromTemplateAndCheckInitContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers()).hasSize(1);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers().get(0))
                .isEqualTo(KubernetesPodTemplateTestUtils.createInitContainer());
    }

    @Test
    void testLoadPodFromTemplateAndCheckMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer().getName())
                .isEqualTo(KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getMainContainer().getVolumeMounts())
                .contains(KubernetesPodTemplateTestUtils.createVolumeMount());
    }

    @Test
    void testLoadPodFromTemplateAndCheckSideCarContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers()).hasSize(1);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers().get(0))
                .isEqualTo(KubernetesPodTemplateTestUtils.createSideCarContainer());
    }

    @Test
    void testLoadPodFromTemplateAndCheckVolumes() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getVolumes())
                .contains(KubernetesPodTemplateTestUtils.createVolumes());
    }

    @Test
    void testResolveUserDefinedValueWithNotDefinedInPodTemplate() {
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        null,
                        "container image");
        assertThat(resolvedImage).isEqualTo(CONTAINER_IMAGE);
    }

    @Test
    void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionExplicitlySet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage).isEqualTo(CONTAINER_IMAGE);
    }

    @Test
    void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionNotSet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        new Configuration(),
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage).isEqualTo(imageInPodTemplate);
    }

    private void testCheckAndUpdatePortConfigOption(
            String port, String fallbackPort, String expectedPort) {
        final Configuration cfg = new Configuration();
        cfg.setString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE, port);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                cfg,
                HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
                Integer.valueOf(fallbackPort));
        assertThat(cfg.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE))
                .isEqualTo(expectedPort);
    }
}
