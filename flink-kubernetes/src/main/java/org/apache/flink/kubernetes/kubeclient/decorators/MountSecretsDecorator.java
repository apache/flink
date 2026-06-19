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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Support mounting Secrets on the JobManager or TaskManager pod.. */
public class MountSecretsDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;

    public MountSecretsDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Pod podWithMount = decoratePod(flinkPod.getPodWithoutMainContainer());
        final Container containerWithMount = decorateMainContainer(flinkPod.getMainContainer());

        return new FlinkPod.Builder(flinkPod)
                .withPod(podWithMount)
                .withMainContainer(containerWithMount)
                .build();
    }

    private Container decorateMainContainer(Container container) {
        final VolumeMount[] volumeMounts =
                kubernetesComponentConf.getSecretNamesToMountPaths().entrySet().stream()
                        .map(
                                secretNameToPath ->
                                        new VolumeMountBuilder()
                                                .withName(
                                                        secretVolumeName(secretNameToPath.getKey()))
                                                .withMountPath(secretNameToPath.getValue())
                                                .build())
                        .toArray(VolumeMount[]::new);

        return new ContainerBuilder(container).addToVolumeMounts(volumeMounts).build();
    }

    private Pod decoratePod(Pod pod) {
        final Volume[] volumes =
                kubernetesComponentConf.getSecretNamesToMountPaths().keySet().stream()
                        .map(
                                secretName ->
                                        new VolumeBuilder()
                                                .withName(secretVolumeName(secretName))
                                                .withNewSecret()
                                                .withSecretName(secretName)
                                                .endSecret()
                                                .build())
                        .toArray(Volume[]::new);

        return new PodBuilder(pod).editOrNewSpec().addToVolumes(volumes).endSpec().build();
    }

    private String secretVolumeName(String secretName) {
        return secretName + "-volume";
    }
}
