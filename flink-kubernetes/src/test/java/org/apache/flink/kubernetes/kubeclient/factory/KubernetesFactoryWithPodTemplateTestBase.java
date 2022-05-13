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

package org.apache.flink.kubernetes.kubeclient.factory;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.KubernetesPodTemplateTestUtils;
import org.apache.flink.kubernetes.KubernetesTestBase;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Toleration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test base for the {@link KubernetesJobManagerFactory} and {@link KubernetesTaskManagerFactory}
 * with pod template. These tests will ensure that annotations, labels, imagePullSecrets,
 * nodeSelector, tolerations, env, init container, sidecar container, volumes from pod template
 * should be kept after all decorators.
 */
public abstract class KubernetesFactoryWithPodTemplateTestBase extends KubernetesTestBase {

    private static final String ENTRY_POINT_CLASS =
            KubernetesSessionClusterEntrypoint.class.getCanonicalName();
    private static final int RESOURCE_MEMORY = 1456;

    private Pod resultPod;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);

        // Set fixed ports
        flinkConfig.set(RestOptions.PORT, Constants.REST_PORT);
        flinkConfig.set(BlobServerOptions.PORT, Integer.toString(Constants.BLOB_SERVER_PORT));
        flinkConfig.setString(
                TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));

        flinkConfig.set(
                TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(RESOURCE_MEMORY));
    }

    @Override
    public final void onSetup() throws Exception {
        final FlinkPod podTemplate =
                KubernetesUtils.loadPodFromTemplateFile(
                        flinkKubeClient,
                        KubernetesPodTemplateTestUtils.getPodTemplateFile(),
                        KubernetesPodTemplateTestUtils.TESTING_MAIN_CONTAINER_NAME);
        this.resultPod = getResultPod(podTemplate);
    }

    protected abstract Pod getResultPod(FlinkPod podTemplate) throws Exception;

    @Test
    void testInitContainerFromPodTemplate() {
        assertThat(resultPod.getSpec().getInitContainers())
                .containsExactly(
                        // The expected init container is defined in the
                        // test/resources/testing-pod-template.yaml.
                        KubernetesPodTemplateTestUtils.createInitContainer());
    }

    @Test
    void testSideCarContainerFromPodTemplate() {
        final Container sideCarContainer =
                KubernetesPodTemplateTestUtils.getContainerWithName(
                        resultPod.getSpec(),
                        KubernetesPodTemplateTestUtils.TESTING_SIDE_CAR_CONTAINER_NAME);
        // The expected sidecar is defined in the test/resources/testing-pod-template.yaml.
        assertThat(sideCarContainer)
                .isEqualTo(KubernetesPodTemplateTestUtils.createSideCarContainer());
    }

    @Test
    void testVolumesFromPodTemplate() {
        assertThat(resultPod.getSpec().getVolumes())
                .contains(
                        // The expected volume is defined in the
                        // test/resources/testing-pod-template.yaml.
                        KubernetesPodTemplateTestUtils.createVolumes());
    }

    @Test
    void testMainContainerVolumeMountsFromPodTemplate() {
        final Container mainContainer =
                KubernetesPodTemplateTestUtils.getContainerWithName(
                        resultPod.getSpec(), Constants.MAIN_CONTAINER_NAME);
        assertThat(mainContainer.getVolumeMounts())
                .contains(
                        // The expected volume mount is defined in the
                        // test/resources/testing-pod-template.yaml.
                        KubernetesPodTemplateTestUtils.createVolumeMount());
    }

    @Test
    void testAnnotationsFromPodTemplate() {
        assertThat(resultPod.getMetadata().getAnnotations())
                .containsEntry(
                        // The expected annotation is defined in the
                        // test/resources/testing-pod-template.yaml.
                        "annotation-key-of-pod-template", "annotation-value-of-pod-template");
    }

    @Test
    void testLabelsFromPodTemplate() {
        assertThat(resultPod.getMetadata().getLabels())
                .containsEntry(
                        // The expected label is defined in the
                        // test/resources/testing-pod-template.yaml.
                        "label-key-of-pod-template", "label-value-of-pod-template");
    }

    @Test
    void testImagePullSecretsFromPodTemplate() {
        assertThat(
                        resultPod.getSpec().getImagePullSecrets().stream()
                                .map(LocalObjectReference::getName))
                .contains(
                        // The expected image pull secret is defined in the
                        // test/resources/testing-pod-template.yaml.
                        "image-pull-secret-of-pod-template");
    }

    @Test
    void testNodeSelectorsFromPodTemplate() {
        assertThat(resultPod.getSpec().getNodeSelector())
                .containsEntry(
                        // The expected node selector is defined in the
                        // test/resources/testing-pod-template.yaml.
                        "node-selector-key-of-pod-template", "node-selector-value-of-pod-template");
    }

    @Test
    void testTolerationsFromPodTemplate() {
        assertThat(resultPod.getSpec().getTolerations().stream().map(Toleration::getKey))
                .contains(
                        // The expected toleration is defined in the
                        // test/resources/testing-pod-template.yaml.
                        "key2-of-pod-template");
    }

    @Test
    void testEnvFromPodTemplate() {
        final Container mainContainer =
                KubernetesPodTemplateTestUtils.getContainerWithName(
                        resultPod.getSpec(), Constants.MAIN_CONTAINER_NAME);
        assertThat(mainContainer.getEnv())
                .contains(
                        // The expected env is defined in the
                        // test/resources/testing-pod-template.yaml.
                        new EnvVarBuilder()
                                .withName("ENV_OF_POD_TEMPLATE")
                                .withValue("env-value-of-pod-template")
                                .build());
    }
}
