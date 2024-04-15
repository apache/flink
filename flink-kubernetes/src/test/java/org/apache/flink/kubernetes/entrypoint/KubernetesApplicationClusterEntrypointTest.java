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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;

/** Tests for {@link KubernetesApplicationClusterEntrypointTest}. */
public class KubernetesApplicationClusterEntrypointTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesApplicationClusterEntrypointTest.class);
    public static final String TEST_NAMESPACE = "flink-artifact-namespace-test";
    public static final String TEST_CLUSTER_ID = "flink-artifact-cluster-id-test";
    private Configuration configuration;
    @TempDir Path tempDir;

    @BeforeEach
    public void setup() {
        configuration = new Configuration();
        configuration.setString(ArtifactFetchOptions.BASE_DIR, tempDir.toAbsolutePath().toString());
        configuration.setString(KubernetesConfigOptions.NAMESPACE, TEST_NAMESPACE);
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, TEST_CLUSTER_ID);
    }

    @Test
    public void testGenerateJarDir() {
        String baseDir = KubernetesApplicationClusterEntrypoint.generateJarDir(configuration);
        String expectedDir =
                String.join(
                        File.separator,
                        new String[] {tempDir.toString(), TEST_NAMESPACE, TEST_CLUSTER_ID});
        Assertions.assertEquals(expectedDir, baseDir);
    }
}
