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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import io.fabric8.kubernetes.api.model.ConfigMap;
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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_MAP_PREFIX;
import static org.apache.flink.kubernetes.utils.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mounts the log4j.properties, logback.xml, and flink-conf.yaml configuration on the JobManager or
 * TaskManager pod.
 */
public class FlinkConfMountDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;

    public FlinkConfMountDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        final Pod mountedPod = decoratePod(flinkPod.getPodWithoutMainContainer());

        final Container mountedMainContainer =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(FLINK_CONF_VOLUME)
                        .withMountPath(kubernetesComponentConf.getFlinkConfDirInPod())
                        .endVolumeMount()
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(mountedPod)
                .withMainContainer(mountedMainContainer)
                .build();
    }

    private Pod decoratePod(Pod pod) {
        final List<KeyToPath> keyToPaths =
                getLocalLogConfFiles().stream()
                        .map(
                                file ->
                                        new KeyToPathBuilder()
                                                .withKey(file.getName())
                                                .withPath(file.getName())
                                                .build())
                        .collect(Collectors.toList());
        keyToPaths.add(
                new KeyToPathBuilder()
                        .withKey(FLINK_CONF_FILENAME)
                        .withPath(FLINK_CONF_FILENAME)
                        .build());

        final Volume flinkConfVolume =
                new VolumeBuilder()
                        .withName(FLINK_CONF_VOLUME)
                        .withNewConfigMap()
                        .withName(getFlinkConfConfigMapName(kubernetesComponentConf.getClusterId()))
                        .withItems(keyToPaths)
                        .endConfigMap()
                        .build();

        return new PodBuilder(pod)
                .editSpec()
                .addNewVolumeLike(flinkConfVolume)
                .endVolume()
                .endSpec()
                .build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        final String clusterId = kubernetesComponentConf.getClusterId();

        final Map<String, String> data = new HashMap<>();

        final List<File> localLogFiles = getLocalLogConfFiles();
        for (File file : localLogFiles) {
            data.put(file.getName(), Files.toString(file, StandardCharsets.UTF_8));
        }

        final Map<String, String> propertiesMap =
                getClusterSidePropertiesMap(kubernetesComponentConf.getFlinkConfiguration());
        data.put(FLINK_CONF_FILENAME, getFlinkConfData(propertiesMap));

        final ConfigMap flinkConfConfigMap =
                new ConfigMapBuilder()
                        .withApiVersion(Constants.API_VERSION)
                        .withNewMetadata()
                        .withName(getFlinkConfConfigMapName(clusterId))
                        .withLabels(kubernetesComponentConf.getCommonLabels())
                        .endMetadata()
                        .addToData(data)
                        .build();

        return Collections.singletonList(flinkConfConfigMap);
    }

    /** Get properties map for the cluster-side after removal of some keys. */
    private Map<String, String> getClusterSidePropertiesMap(Configuration flinkConfig) {
        final Configuration clusterSideConfig = flinkConfig.clone();
        // Remove some configuration options that should not be taken to cluster side.
        clusterSideConfig.removeConfig(KubernetesConfigOptions.KUBE_CONFIG_FILE);
        clusterSideConfig.removeConfig(DeploymentOptionsInternal.CONF_DIR);
        return clusterSideConfig.toMap();
    }

    @VisibleForTesting
    String getFlinkConfData(Map<String, String> propertiesMap) throws IOException {
        try (StringWriter sw = new StringWriter();
                PrintWriter out = new PrintWriter(sw)) {
            propertiesMap.forEach(
                    (k, v) -> {
                        out.print(k);
                        out.print(": ");
                        out.println(v);
                    });

            return sw.toString();
        }
    }

    private List<File> getLocalLogConfFiles() {
        final String confDir = kubernetesComponentConf.getConfigDirectory();
        final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
        final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);

        List<File> localLogConfFiles = new ArrayList<>();
        if (logbackFile.exists()) {
            localLogConfFiles.add(logbackFile);
        }
        if (log4jFile.exists()) {
            localLogConfFiles.add(log4jFile);
        }

        return localLogConfFiles;
    }

    @VisibleForTesting
    public static String getFlinkConfConfigMapName(String clusterId) {
        return CONFIG_MAP_PREFIX + clusterId;
    }
}
