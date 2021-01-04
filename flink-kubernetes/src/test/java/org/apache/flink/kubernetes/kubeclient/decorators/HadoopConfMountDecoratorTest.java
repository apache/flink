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
import org.apache.flink.kubernetes.kubeclient.KubernetesJobManagerTestBase;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapVolumeSource;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** General tests for the {@link HadoopConfMountDecorator}. */
public class HadoopConfMountDecoratorTest extends KubernetesJobManagerTestBase {

    private static final String EXISTING_HADOOP_CONF_CONFIG_MAP = "hadoop-conf";

    private HadoopConfMountDecorator hadoopConfMountDecorator;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        this.hadoopConfMountDecorator =
                new HadoopConfMountDecorator(kubernetesJobManagerParameters);
    }

    @Test
    public void testExistingHadoopConfigMap() throws IOException {
        flinkConfig.set(
                KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);
        assertEquals(0, hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size());

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        final List<Volume> volumes = resultFlinkPod.getPod().getSpec().getVolumes();
        assertTrue(
                volumes.stream()
                        .anyMatch(
                                volume ->
                                        volume.getConfigMap()
                                                .getName()
                                                .equals(EXISTING_HADOOP_CONF_CONFIG_MAP)));
    }

    @Test
    public void testExistingConfigMapPrecedeOverHadoopConfEnv() throws IOException {
        // set existing ConfigMap
        flinkConfig.set(
                KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP, EXISTING_HADOOP_CONF_CONFIG_MAP);

        // set HADOOP_CONF_DIR
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();

        assertEquals(0, hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size());

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        final List<Volume> volumes = resultFlinkPod.getPod().getSpec().getVolumes();
        assertTrue(
                volumes.stream()
                        .anyMatch(
                                volume ->
                                        volume.getConfigMap()
                                                .getName()
                                                .equals(EXISTING_HADOOP_CONF_CONFIG_MAP)));
        assertFalse(
                volumes.stream()
                        .anyMatch(
                                volume ->
                                        volume.getConfigMap()
                                                .getName()
                                                .equals(
                                                        HadoopConfMountDecorator
                                                                .getHadoopConfConfigMapName(
                                                                        CLUSTER_ID))));
    }

    @Test
    public void testHadoopConfDirectoryUnset() throws IOException {
        assertEquals(0, hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size());

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
        assertEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
    }

    @Test
    public void testEmptyHadoopConfDirectory() throws IOException {
        setHadoopConfDirEnv();

        assertEquals(0, hadoopConfMountDecorator.buildAccompanyingKubernetesResources().size());

        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);
        assertEquals(baseFlinkPod.getPod(), resultFlinkPod.getPod());
        assertEquals(baseFlinkPod.getMainContainer(), resultFlinkPod.getMainContainer());
    }

    @Test
    public void testHadoopConfConfigMap() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();

        final List<HasMetadata> additionalResources =
                hadoopConfMountDecorator.buildAccompanyingKubernetesResources();
        assertEquals(1, additionalResources.size());

        final ConfigMap resultConfigMap = (ConfigMap) additionalResources.get(0);

        assertEquals(Constants.API_VERSION, resultConfigMap.getApiVersion());
        assertEquals(
                HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID),
                resultConfigMap.getMetadata().getName());
        assertEquals(getCommonLabels(), resultConfigMap.getMetadata().getLabels());

        Map<String, String> resultDatas = resultConfigMap.getData();
        assertEquals("some data", resultDatas.get("core-site.xml"));
        assertEquals("some data", resultDatas.get("hdfs-site.xml"));
    }

    @Test
    public void testPodWithHadoopConfVolume() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);

        final List<Volume> resultVolumes = resultFlinkPod.getPod().getSpec().getVolumes();
        assertEquals(1, resultVolumes.size());

        final Volume resultVolume = resultVolumes.get(0);
        assertEquals(Constants.HADOOP_CONF_VOLUME, resultVolume.getName());

        final ConfigMapVolumeSource resultVolumeConfigMap = resultVolume.getConfigMap();
        assertEquals(
                HadoopConfMountDecorator.getHadoopConfConfigMapName(CLUSTER_ID),
                resultVolumeConfigMap.getName());

        final Map<String, String> expectedKeyToPaths =
                new HashMap<String, String>() {
                    {
                        put("hdfs-site.xml", "hdfs-site.xml");
                        put("core-site.xml", "core-site.xml");
                    }
                };
        final Map<String, String> resultKeyToPaths =
                resultVolumeConfigMap.getItems().stream()
                        .collect(Collectors.toMap(KeyToPath::getKey, KeyToPath::getPath));
        assertEquals(expectedKeyToPaths, resultKeyToPaths);
    }

    @Test
    public void testMainContainerWithHadoopConfVolumeMount() throws IOException {
        setHadoopConfDirEnv();
        generateHadoopConfFileItems();
        final FlinkPod resultFlinkPod = hadoopConfMountDecorator.decorateFlinkPod(baseFlinkPod);

        final List<VolumeMount> resultVolumeMounts =
                resultFlinkPod.getMainContainer().getVolumeMounts();
        assertEquals(1, resultVolumeMounts.size());
        final VolumeMount resultVolumeMount = resultVolumeMounts.get(0);
        assertEquals(Constants.HADOOP_CONF_VOLUME, resultVolumeMount.getName());
        assertEquals(Constants.HADOOP_CONF_DIR_IN_POD, resultVolumeMount.getMountPath());

        final Map<String, String> expectedEnvs =
                new HashMap<String, String>() {
                    {
                        put(Constants.ENV_HADOOP_CONF_DIR, Constants.HADOOP_CONF_DIR_IN_POD);
                    }
                };
        final Map<String, String> resultEnvs =
                resultFlinkPod.getMainContainer().getEnv().stream()
                        .collect(Collectors.toMap(EnvVar::getName, EnvVar::getValue));
        assertEquals(expectedEnvs, resultEnvs);
    }
}
