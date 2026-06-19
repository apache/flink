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

import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.kubernetes.utils.Constants.TASK_MANAGER_POD_TEMPLATE_FILE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** General tests for the {@link PodTemplateMountDecorator}. */
class PodTemplateMountDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String POD_TEMPLATE_FILE_NAME = "testing-pod-template.yaml";
    private static final String POD_TEMPLATE_DATA = "taskmanager pod template data";

    private PodTemplateMountDecorator podTemplateMountDecorator;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        this.flinkConfig.set(
                KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE,
                new File(flinkConfDir, POD_TEMPLATE_FILE_NAME).getAbsolutePath());
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.podTemplateMountDecorator =
                new PodTemplateMountDecorator(kubernetesJobManagerParameters);
    }

    @Test
    void testBuildAccompanyingKubernetesResourcesAddsPodTemplateAsConfigMap() throws IOException {
        KubernetesTestUtils.createTemporyFile(
                POD_TEMPLATE_DATA, flinkConfDir, POD_TEMPLATE_FILE_NAME);

        final List<HasMetadata> additionalResources =
                podTemplateMountDecorator.buildAccompanyingKubernetesResources();
        assertThat(additionalResources).hasSize(1);

        final ConfigMap resultConfigMap = (ConfigMap) additionalResources.get(0);

        final Map<String, String> resultData = resultConfigMap.getData();
        assertThat(resultData.get(TASK_MANAGER_POD_TEMPLATE_FILE_NAME))
                .isEqualTo(POD_TEMPLATE_DATA);
    }

    @Test
    void testDecoratorShouldFailWhenPodTemplateFileNotExist() {
        final String msg =
                String.format(
                        "Pod template file %s does not exist.",
                        new File(flinkConfDir, POD_TEMPLATE_FILE_NAME));
        assertThatThrownBy(
                        () -> podTemplateMountDecorator.buildAccompanyingKubernetesResources(),
                        "Decorator should fail when the pod template file does not exist.")
                .satisfies(FlinkAssertions.anyCauseMatches(msg));
    }

    @Test
    void testDecoratedFlinkPodWithTaskManagerPodTemplate() throws Exception {
        KubernetesTestUtils.createTemporyFile(
                POD_TEMPLATE_DATA, flinkConfDir, POD_TEMPLATE_FILE_NAME);

        final FlinkPod resultFlinkPod = podTemplateMountDecorator.decorateFlinkPod(baseFlinkPod);

        final List<KeyToPath> expectedKeyToPaths =
                Collections.singletonList(
                        new KeyToPathBuilder()
                                .withKey(TASK_MANAGER_POD_TEMPLATE_FILE_NAME)
                                .withPath(TASK_MANAGER_POD_TEMPLATE_FILE_NAME)
                                .build());
        final List<Volume> expectedVolumes = getExpectedVolumes(expectedKeyToPaths);
        assertThat(resultFlinkPod.getPodWithoutMainContainer().getSpec().getVolumes())
                .containsExactlyInAnyOrderElementsOf(expectedVolumes);

        final List<VolumeMount> expectedVolumeMounts =
                Collections.singletonList(
                        new VolumeMountBuilder()
                                .withName(Constants.POD_TEMPLATE_VOLUME)
                                .withMountPath(Constants.POD_TEMPLATE_DIR_IN_POD)
                                .build());
        assertThat(resultFlinkPod.getMainContainer().getVolumeMounts())
                .containsExactlyInAnyOrderElementsOf(expectedVolumeMounts);
    }

    private List<Volume> getExpectedVolumes(List<KeyToPath> keyToPaths) {
        return Collections.singletonList(
                new VolumeBuilder()
                        .withName(Constants.POD_TEMPLATE_VOLUME)
                        .withNewConfigMap()
                        .withName(Constants.POD_TEMPLATE_CONFIG_MAP_PREFIX + CLUSTER_ID)
                        .withItems(keyToPaths)
                        .endConfigMap()
                        .build());
    }
}
