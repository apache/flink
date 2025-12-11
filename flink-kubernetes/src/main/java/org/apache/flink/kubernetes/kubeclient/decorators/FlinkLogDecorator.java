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

import static org.apache.flink.kubernetes.utils.Constants.FLINK_LOG;
import static org.apache.flink.util.Preconditions.checkNotNull;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HostPathVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

/** Support mounting logs on the JobManager or TaskManager pod.. */
public class FlinkLogDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;

    public FlinkLogDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
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

        VolumeMount volumeMount = new VolumeMountBuilder().withName(FLINK_LOG)
                .withMountPath(kubernetesComponentConf.getVolumeMountPath()).build();

        return new ContainerBuilder(container).addToVolumeMounts(volumeMount).build();
    }

    private Pod decoratePod(Pod pod) {

        Volume volume = new VolumeBuilder().withName(FLINK_LOG).withHostPath(
                new HostPathVolumeSourceBuilder().withPath(kubernetesComponentConf.getVolumeLogs())
                        .withType("DirectoryOrCreate").build()).build();



        return new PodBuilder(pod).editOrNewSpec().addToVolumes(volume).endSpec().build();
    }

}
