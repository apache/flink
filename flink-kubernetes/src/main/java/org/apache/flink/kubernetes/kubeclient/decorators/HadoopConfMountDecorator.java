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
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.FileUtils;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mount the custom Hadoop Configuration to the JobManager(s)/TaskManagers. We provide two options:
 * 1. Mount an existing ConfigMap containing custom Hadoop Configuration. 2. Create and mount a
 * dedicated ConfigMap containing the custom Hadoop configuration from a local directory specified
 * via the HADOOP_CONF_DIR or HADOOP_HOME environment variable.
 */
public class HadoopConfMountDecorator extends AbstractKubernetesStepDecorator {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopConfMountDecorator.class);

    private final AbstractKubernetesParameters kubernetesParameters;

    public HadoopConfMountDecorator(AbstractKubernetesParameters kubernetesParameters) {
        this.kubernetesParameters = checkNotNull(kubernetesParameters);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        Volume hadoopConfVolume;

        final Optional<String> existingConfigMap =
                kubernetesParameters.getExistingHadoopConfigurationConfigMap();
        if (existingConfigMap.isPresent()) {
            hadoopConfVolume =
                    new VolumeBuilder()
                            .withName(Constants.HADOOP_CONF_VOLUME)
                            .withNewConfigMap()
                            .withName(existingConfigMap.get())
                            .endConfigMap()
                            .build();
        } else {
            final Optional<String> localHadoopConfigurationDirectory =
                    kubernetesParameters.getLocalHadoopConfigurationDirectory();
            if (!localHadoopConfigurationDirectory.isPresent()) {
                return flinkPod;
            }

            final List<File> hadoopConfigurationFileItems =
                    getHadoopConfigurationFileItems(localHadoopConfigurationDirectory.get());
            if (hadoopConfigurationFileItems.isEmpty()) {
                LOG.warn(
                        "Found 0 files in directory {}, skip to mount the Hadoop Configuration ConfigMap.",
                        localHadoopConfigurationDirectory.get());
                return flinkPod;
            }

            final List<KeyToPath> keyToPaths =
                    hadoopConfigurationFileItems.stream()
                            .map(
                                    file ->
                                            new KeyToPathBuilder()
                                                    .withKey(file.getName())
                                                    .withPath(file.getName())
                                                    .build())
                            .collect(Collectors.toList());

            hadoopConfVolume =
                    new VolumeBuilder()
                            .withName(Constants.HADOOP_CONF_VOLUME)
                            .withNewConfigMap()
                            .withName(
                                    getHadoopConfConfigMapName(kubernetesParameters.getClusterId()))
                            .withItems(keyToPaths)
                            .endConfigMap()
                            .build();
        }

        final Pod podWithHadoopConf =
                new PodBuilder(flinkPod.getPod())
                        .editOrNewSpec()
                        .addNewVolumeLike(hadoopConfVolume)
                        .endVolume()
                        .endSpec()
                        .build();

        final Container containerWithHadoopConf =
                new ContainerBuilder(flinkPod.getMainContainer())
                        .addNewVolumeMount()
                        .withName(Constants.HADOOP_CONF_VOLUME)
                        .withMountPath(Constants.HADOOP_CONF_DIR_IN_POD)
                        .endVolumeMount()
                        .addNewEnv()
                        .withName(Constants.ENV_HADOOP_CONF_DIR)
                        .withValue(Constants.HADOOP_CONF_DIR_IN_POD)
                        .endEnv()
                        .build();

        return new FlinkPod.Builder(flinkPod)
                .withPod(podWithHadoopConf)
                .withMainContainer(containerWithHadoopConf)
                .build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        if (kubernetesParameters.getExistingHadoopConfigurationConfigMap().isPresent()) {
            return Collections.emptyList();
        }

        final Optional<String> localHadoopConfigurationDirectory =
                kubernetesParameters.getLocalHadoopConfigurationDirectory();
        if (!localHadoopConfigurationDirectory.isPresent()) {
            return Collections.emptyList();
        }

        final List<File> hadoopConfigurationFileItems =
                getHadoopConfigurationFileItems(localHadoopConfigurationDirectory.get());
        if (hadoopConfigurationFileItems.isEmpty()) {
            LOG.warn(
                    "Found 0 files in directory {}, skip to create the Hadoop Configuration ConfigMap.",
                    localHadoopConfigurationDirectory.get());
            return Collections.emptyList();
        }

        final Map<String, String> data = new HashMap<>();
        for (File file : hadoopConfigurationFileItems) {
            data.put(file.getName(), FileUtils.readFileUtf8(file));
        }

        final ConfigMap hadoopConfigMap =
                new ConfigMapBuilder()
                        .withApiVersion(Constants.API_VERSION)
                        .withNewMetadata()
                        .withName(getHadoopConfConfigMapName(kubernetesParameters.getClusterId()))
                        .withLabels(kubernetesParameters.getCommonLabels())
                        .endMetadata()
                        .addToData(data)
                        .build();

        return Collections.singletonList(hadoopConfigMap);
    }

    private List<File> getHadoopConfigurationFileItems(String localHadoopConfigurationDirectory) {
        final List<String> expectedFileNames = new ArrayList<>();
        expectedFileNames.add("core-site.xml");
        expectedFileNames.add("hdfs-site.xml");

        final File directory = new File(localHadoopConfigurationDirectory);
        if (directory.exists() && directory.isDirectory()) {
            return Arrays.stream(directory.listFiles())
                    .filter(
                            file ->
                                    file.isFile()
                                            && expectedFileNames.stream()
                                                    .anyMatch(name -> file.getName().equals(name)))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    public static String getHadoopConfConfigMapName(String clusterId) {
        return Constants.HADOOP_CONF_CONFIG_MAP_PREFIX + clusterId;
    }
}
