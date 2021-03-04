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

package org.apache.flink.kubernetes;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Utilities for the Kubernetes pod template tests. The create methods are to provide the init
 * containers, sidecar containers, volumes which are specified in the
 * test-resources/testing-pod-template.yaml.
 */
public class KubernetesPodTemplateTestUtils {

    public static final String TESTING_MAIN_CONTAINER_NAME = "testing-main-container";
    public static final String TESTING_SIDE_CAR_CONTAINER_NAME = "sidecar-log-collector";

    private static final String TESTING_TEMPLATE_FILE_NAME = "testing-pod-template.yaml";

    public static File getPodTemplateFile() {
        final URL podTemplateUrl =
                KubernetesPodTemplateTestUtils.class
                        .getClassLoader()
                        .getResource(TESTING_TEMPLATE_FILE_NAME);
        assertThat(podTemplateUrl, not(nullValue()));
        return new File(podTemplateUrl.getPath());
    }

    public static Container createInitContainer() {
        return new ContainerBuilder()
                .withName("artifacts-fetcher")
                .withImage("testing-init-image")
                .withCommand(
                        "wget",
                        "https://path/of/StateMachineExample.jar",
                        "-O",
                        "/flink-artifact/myjob.jar")
                .withVolumeMounts(
                        new VolumeMountBuilder()
                                .withName("flink-artifact")
                                .withMountPath("/flink-artifact")
                                .build())
                .build();
    }

    public static Container createSideCarContainer() {
        return new ContainerBuilder()
                .withName("sidecar-log-collector")
                .withImage("test-sidecar-image")
                .withCommand("command-to-upload", "/flink-logs/jobmanager.log")
                .withVolumeMounts(
                        new VolumeMountBuilder()
                                .withName("flink-logs")
                                .withMountPath("/flink-logs")
                                .build())
                .build();
    }

    public static Volume[] createVolumes() {
        return new Volume[] {
            new VolumeBuilder()
                    .withName("flink-volume-hostpath")
                    .withNewHostPath()
                    .withNewPath("/tmp")
                    .withType("Directory")
                    .endHostPath()
                    .build(),
            new VolumeBuilder().withName("flink-artifact").withNewEmptyDir().endEmptyDir().build(),
            new VolumeBuilder().withName("flink-logs").withNewEmptyDir().endEmptyDir().build()
        };
    }

    public static VolumeMount[] createVolumeMount() {
        return new VolumeMount[] {
            new VolumeMountBuilder()
                    .withName("flink-volume-hostpath")
                    .withMountPath("/opt/flink/volumes/hostpath")
                    .build(),
            new VolumeMountBuilder()
                    .withName("flink-artifact")
                    .withMountPath("/opt/flink/artifacts")
                    .build(),
            new VolumeMountBuilder().withName("flink-logs").withMountPath("/opt/flink/log").build()
        };
    }

    public static Container getContainerWithName(PodSpec podSpec, String containerName) {
        final List<Container> containers =
                podSpec.getContainers().stream()
                        .filter(e -> e.getName().equals(containerName))
                        .collect(Collectors.toList());
        assertThat(containers.size(), is(1));
        return containers.get(0);
    }
}
