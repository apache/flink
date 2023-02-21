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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** General tests for the {@link AbstractKubernetesParameters}. */
public class AbstractKubernetesParametersTest {

    private final Configuration flinkConfig = new Configuration();
    private final TestingKubernetesParameters testingKubernetesParameters =
            new TestingKubernetesParameters(flinkConfig);

    @Test
    void testClusterIdMustNotBeBlank() {
        flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, "  ");
        assertThatThrownBy(testingKubernetesParameters::getClusterId)
                .satisfies(anyCauseMatches(IllegalArgumentException.class, "must not be blank"));
    }

    @Test
    void testClusterIdLengthLimitation() {
        final String stringWithIllegalLength =
                StringUtils.generateRandomAlphanumericString(
                        new Random(), Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + 1);
        flinkConfig.set(KubernetesConfigOptions.CLUSTER_ID, stringWithIllegalLength);
        assertThatThrownBy(testingKubernetesParameters::getClusterId)
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "must be no more than "
                                        + Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID
                                        + " characters"));
    }

    @Test
    void getConfigDirectory() {
        final String confDir = "/path/of/flink-conf";
        flinkConfig.set(DeploymentOptionsInternal.CONF_DIR, confDir);
        assertThat(testingKubernetesParameters.getConfigDirectory()).isEqualTo(confDir);
    }

    @Test
    void getConfigDirectoryFallbackToPodConfDir() {
        final String confDirInPod = flinkConfig.get(KubernetesConfigOptions.FLINK_CONF_DIR);
        assertThat(testingKubernetesParameters.getConfigDirectory()).isEqualTo(confDirInPod);
    }

    @Test
    void testGetLocalHadoopConfigurationDirectoryReturnEmptyWhenHadoopEnvIsNotSet()
            throws Exception {
        runTestWithEmptyEnv(
                () -> {
                    final Optional<String> optional =
                            testingKubernetesParameters.getLocalHadoopConfigurationDirectory();
                    assertThat(optional).isNotPresent();
                });
    }

    @Test
    void testGetLocalHadoopConfigurationDirectoryFromHadoopConfDirEnv() throws Exception {
        runTestWithEmptyEnv(
                () -> {
                    final String hadoopConfDir = "/etc/hadoop/conf";
                    setEnv(Constants.ENV_HADOOP_CONF_DIR, hadoopConfDir);

                    final Optional<String> optional =
                            testingKubernetesParameters.getLocalHadoopConfigurationDirectory();
                    assertThat(optional).isPresent();
                    assertThat(optional.get()).isEqualTo(hadoopConfDir);
                });
    }

    @Test
    void testGetLocalHadoopConfigurationDirectoryFromHadoop2HomeEnv(@TempDir Path temporaryFolder)
            throws Exception {
        runTestWithEmptyEnv(
                () -> {
                    final String hadoopHome = temporaryFolder.toAbsolutePath().toString();
                    Files.createDirectories(temporaryFolder.resolve(Paths.get("etc", "hadoop")));
                    setEnv(Constants.ENV_HADOOP_HOME, hadoopHome);

                    final Optional<String> optional =
                            testingKubernetesParameters.getLocalHadoopConfigurationDirectory();
                    assertThat(optional).isPresent();
                    assertThat(optional.get()).isEqualTo(hadoopHome + "/etc/hadoop");
                });
    }

    @Test
    void testGetLocalHadoopConfigurationDirectoryFromHadoop1HomeEnv(@TempDir Path temporaryFolder)
            throws Exception {
        runTestWithEmptyEnv(
                () -> {
                    final String hadoopHome = temporaryFolder.toAbsolutePath().toString();
                    Files.createDirectory(temporaryFolder.resolve("conf"));
                    setEnv(Constants.ENV_HADOOP_HOME, hadoopHome);

                    final Optional<String> optional =
                            testingKubernetesParameters.getLocalHadoopConfigurationDirectory();
                    assertThat(optional).isPresent();
                    assertThat(optional.get()).isEqualTo(hadoopHome + "/conf");
                });
    }

    private void runTestWithEmptyEnv(RunnableWithException testMethod) throws Exception {
        final Map<String, String> current = new HashMap<>(System.getenv());
        // Clear the environments
        CommonTestUtils.setEnv(Collections.emptyMap(), true);
        testMethod.run();
        // Restore the environments
        CommonTestUtils.setEnv(current, true);
    }

    private void setEnv(String key, String value) {
        final Map<String, String> map = new HashMap<>();
        map.put(key, value);
        CommonTestUtils.setEnv(map, false);
    }

    /** KubernetesParameters for testing usecase. */
    public static class TestingKubernetesParameters extends AbstractKubernetesParameters {

        public TestingKubernetesParameters(Configuration flinkConfig) {
            super(flinkConfig);
        }

        @Override
        public Map<String, String> getLabels() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getSelectors() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getNodeSelector() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getEnvironments() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getAnnotations() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public List<Map<String, String>> getTolerations() {
            throw new UnsupportedOperationException("NOT supported");
        }
    }
}
