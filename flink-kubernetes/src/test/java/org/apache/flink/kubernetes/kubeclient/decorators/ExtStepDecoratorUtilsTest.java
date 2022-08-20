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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.kubernetes.kubeclient.decorators.extended.ExtTestDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** The test class for {@link ExtStepDecoratorUtils}. */
public class ExtStepDecoratorUtilsTest {
    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String EXT_DECORATOR_NAME = "ext-decorator";
    private static final String EXT_JAR = EXT_DECORATOR_NAME + "-test-jar.jar";

    @Test
    public void testLoadExtStepDecorators() throws IOException {
        File testRootFolder = temporaryFolder.newFolder();
        File testPluginFolder = new File(testRootFolder, EXT_DECORATOR_NAME);
        assertTrue(testPluginFolder.mkdirs());
        File testPluginJar = new File("target", EXT_JAR);
        assertTrue(testPluginJar.exists());
        Files.copy(testPluginJar.toPath(), Paths.get(testPluginFolder.toString(), EXT_JAR));
        Map<String, String> origEnv = System.getenv();
        try {
            HashMap<String, String> currentEnv = new HashMap<>(origEnv);
            currentEnv.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, testRootFolder.getPath());
            CommonTestUtils.setEnv(currentEnv);
            final Configuration flinkConfig = new Configuration();
            final ClusterSpecification clusterSpecification =
                    new ClusterSpecification.ClusterSpecificationBuilder()
                            .setMasterMemoryMB(1024)
                            .setTaskManagerMemoryMB(1024)
                            .setSlotsPerTaskManager(3)
                            .createClusterSpecification();
            KubernetesJobManagerParameters kubernetesJobManagerParameters =
                    new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);
            assertEquals(
                    ExtTestDecorator.class,
                    ExtStepDecoratorUtils.loadExtStepDecorators(kubernetesJobManagerParameters)
                            .get(0)
                            .getClass());
        } finally {
            CommonTestUtils.setEnv(origEnv);
        }
    }
}
