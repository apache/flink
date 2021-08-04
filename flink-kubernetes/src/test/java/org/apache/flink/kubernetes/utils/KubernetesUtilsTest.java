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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for {@link KubernetesUtils}. */
public class KubernetesUtilsTest extends KubernetesTestBase {

    private static final FlinkPod EMPTY_POD = new FlinkPod.Builder().build();

    @Test
    public void testParsePortRange() {
        final Configuration cfg = new Configuration();
        cfg.set(BlobServerOptions.PORT, "50100-50200");
        try {
            KubernetesUtils.parsePort(cfg, BlobServerOptions.PORT);
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
    public void testParsePortNull() {
        final Configuration cfg = new Configuration();
        ConfigOption<String> testingPort =
                ConfigOptions.key("test.port").stringType().noDefaultValue();
        try {
            KubernetesUtils.parsePort(cfg, testingPort);
            fail("Should fail with an exception.");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString(testingPort.key() + " should not be null."));
        }
    }

    @Test
    public void testCheckWithDynamicPort() {
        testCheckAndUpdatePortConfigOption("0", "6123", "6123");
    }

    @Test
    public void testCheckWithFixedPort() {
        testCheckAndUpdatePortConfigOption("6123", "16123", "6123");
    }

    @Test
    public void testLoadPodFromTemplateWithNonExistPathShouldFail() {
        final String nonExistFile = "/path/of/non-exist.yaml";
        try {
            KubernetesUtils.loadPodFromTemplateFile(
                    flinkKubeClient,
                    new File(nonExistFile),
                    KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
            fail("Kubernetes client should fail when the pod template file does not exist.");
        } catch (Exception ex) {
            final String msg = String.format("Pod template file %s does not exist.", nonExistFile);
            assertThat(ex, FlinkMatchers.containsMessage(msg));
        }
    }

    @Test
    public void testLoadPodFromTemplateWithNoMainContainerShouldReturnEmptyMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        "nonExistMainContainer");
        assertThat(flinkPod.getMainContainer(), is(EMPTY_POD.getMainContainer()));
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers().size(), is(2));
    }

    @Test
    public void testLoadPodFromTemplateAndCheckMetaData() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        // The pod name is defined in the test/resources/testing-pod-template.yaml.
        final String expectedPodName = "pod-template";
        assertThat(
                flinkPod.getPodWithoutMainContainer().getMetadata().getName(), is(expectedPodName));
    }

    @Test
    public void testLoadPodFromTemplateAndCheckInitContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(
                flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers().size(), is(1));
        assertThat(
                flinkPod.getPodWithoutMainContainer().getSpec().getInitContainers().get(0),
                is(KubernetesPodTemplateTestUtils.createInitContainer()));
    }

    @Test
    public void testLoadPodFromTemplateAndCheckMainContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(
                flinkPod.getMainContainer().getName(),
                is(KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME));
        assertThat(
                flinkPod.getMainContainer().getVolumeMounts(),
                containsInAnyOrder(KubernetesPodTemplateTestUtils.createVolumeMount()));
    }

    @Test
    public void testLoadPodFromTemplateAndCheckSideCarContainer() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(flinkPod.getPodWithoutMainContainer().getSpec().getContainers().size(), is(1));
        assertThat(
                flinkPod.getPodWithoutMainContainer().getSpec().getContainers().get(0),
                is(KubernetesPodTemplateTestUtils.createSideCarContainer()));
    }

    @Test
    public void testLoadPodFromTemplateAndCheckVolumes() {
        final FlinkPod flinkPod =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        assertThat(
                flinkPod.getPodWithoutMainContainer().getSpec().getVolumes(),
                containsInAnyOrder(KubernetesPodTemplateTestUtils.createVolumes()));
    }

    @Test
    public void testResolveUserDefinedValueWithNotDefinedInPodTemplate() {
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        null,
                        "container image");
        assertThat(resolvedImage, is(CONTAINER_IMAGE));
    }

    @Test
    public void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionExplicitlySet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage, is(CONTAINER_IMAGE));
    }

    @Test
    public void testResolveUserDefinedValueWithDefinedInPodTemplateAndConfigOptionNotSet() {
        final String imageInPodTemplate = "image-in-pod-template:v1";
        final String resolvedImage =
                KubernetesUtils.resolveUserDefinedValue(
                        new Configuration(),
                        KubernetesConfigOptions.CONTAINER_IMAGE,
                        CONTAINER_IMAGE,
                        imageInPodTemplate,
                        "container image");
        assertThat(resolvedImage, is(imageInPodTemplate));
    }

    private void testCheckAndUpdatePortConfigOption(
            String port, String fallbackPort, String expectedPort) {
        final Configuration cfg = new Configuration();
        cfg.setString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE, port);
        KubernetesUtils.checkAndUpdatePortConfigOption(
                cfg,
                HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE,
                Integer.valueOf(fallbackPort));
        assertEquals(expectedPort, cfg.get(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE));
    }
}
