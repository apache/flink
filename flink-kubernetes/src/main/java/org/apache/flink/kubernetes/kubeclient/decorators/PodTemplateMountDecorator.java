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
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionUtils;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.utils.Constants.POD_TEMPLATE_DIR_IN_POD;
import static org.apache.flink.kubernetes.utils.Constants.POD_TEMPLATE_VOLUME;
import static org.apache.flink.kubernetes.utils.Constants.TASK_MANAGER_POD_TEMPLATE_FILE_NAME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Ship and mount {@link Constants#TASK_MANAGER_POD_TEMPLATE_FILE_NAME} on the JobManager. */
public class PodTemplateMountDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;

    private final String podTemplateConfigMapName;

    public PodTemplateMountDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.podTemplateConfigMapName =
                Constants.POD_TEMPLATE_CONFIG_MAP_PREFIX + kubernetesComponentConf.getClusterId();
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (!getTaskManagerPodTemplateFile().isPresent()) {
            return flinkPod;
        }

        final Pod mountedPod = decoratePod(flinkPod.getPodWithoutMainContainer());

        final Container mountedMainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(POD_TEMPLATE_VOLUME)
                        .withMountPath(POD_TEMPLATE_DIR_IN_POD)
                        .endVolumeMount()
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(mountedPod)
                .withMainContainer(mountedMainContainer)
                .build();
    }

    private Pod decoratePod(Pod pod) {
        final List<KeyToPath> keyToPaths = new ArrayList<>();
        keyToPaths.add(
                new KeyToPathBuilder()
                        .withKey(TASK_MANAGER_POD_TEMPLATE_FILE_NAME)
                        .withPath(TASK_MANAGER_POD_TEMPLATE_FILE_NAME)
                        .build());
        final Volume podTemplateVolume =
                new VolumeBuilder()
                        .withName(POD_TEMPLATE_VOLUME)
                        .withNewConfigMap()
                        .withName(podTemplateConfigMapName)
                        .withItems(keyToPaths)
                        .endConfigMap()
                        .build();
        return new PodBuilder(pod)
                .editSpec()
                .addNewVolumeLike(podTemplateVolume)
                .endVolume()
                .endSpec()
                .build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return getTaskManagerPodTemplateFile()
                .map(
                        FunctionUtils.uncheckedFunction(
                                file -> {
                                    final Map<String, String> data = new HashMap<>();
                                    data.put(
                                            TASK_MANAGER_POD_TEMPLATE_FILE_NAME,
                                            Files.toString(file, StandardCharsets.UTF_8));
                                    final HasMetadata flinkConfConfigMap =
                                            new ConfigMapBuilder()
                                                    .withApiVersion(Constants.API_VERSION)
                                                    .withNewMetadata()
                                                    .withName(podTemplateConfigMapName)
                                                    .withLabels(
                                                            kubernetesComponentConf
                                                                    .getCommonLabels())
                                                    .endMetadata()
                                                    .addToData(data)
                                                    .build();
                                    return Collections.singletonList(flinkConfConfigMap);
                                }))
                .orElse(Collections.emptyList());
    }

    private Optional<File> getTaskManagerPodTemplateFile() {
        return kubernetesComponentConf
                .getFlinkConfiguration()
                .getOptional(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE)
                .map(
                        file -> {
                            final File podTemplateFile = new File(file);
                            if (!podTemplateFile.exists()) {
                                throw new FlinkRuntimeException(
                                        String.format(
                                                "Pod template file %s does not exist.", file));
                            }
                            return podTemplateFile;
                        });
    }
}
