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

package org.apache.flink.yarn;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.Collections;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;

/** Test cases for the deployment of Yarn Flink application clusters. */
public class YARNApplicationITCase extends YarnTestBase {

    private static final Duration yarnAppTerminateTimeout = Duration.ofSeconds(30);
    private static final int sleepIntervalInMS = 100;

    @BeforeClass
    public static void setup() {
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-application");
        startYARNWithConfig(YARN_CONFIGURATION, true);
    }

    @Test
    public void testApplicationClusterWithLocalUserJarAndFirstUserJarInclusion() throws Exception {
        runTest(
                () ->
                        deployApplication(
                                createDefaultConfiguration(
                                        getTestingJar(),
                                        YarnConfigOptions.UserJarInclusion.FIRST)));
    }

    @Test
    public void testApplicationClusterWithLocalUserJarAndDisableUserJarInclusion()
            throws Exception {
        runTest(
                () ->
                        deployApplication(
                                createDefaultConfiguration(
                                        getTestingJar(),
                                        YarnConfigOptions.UserJarInclusion.DISABLED)));
    }

    @Test
    public void testApplicationClusterWithRemoteUserJar() throws Exception {
        final Path testingJar = getTestingJar();
        final Path remoteJar =
                new Path(miniDFSCluster.getFileSystem().getHomeDirectory(), testingJar.getName());
        miniDFSCluster.getFileSystem().copyFromLocalFile(testingJar, remoteJar);
        runTest(
                () ->
                        deployApplication(
                                createDefaultConfiguration(
                                        remoteJar, YarnConfigOptions.UserJarInclusion.DISABLED)));
    }

    private void deployApplication(Configuration configuration) throws Exception {
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                createYarnClusterDescriptor(configuration)) {

            final int masterMemory =
                    yarnClusterDescriptor
                            .getFlinkConfiguration()
                            .get(JobManagerOptions.TOTAL_PROCESS_MEMORY)
                            .getMebiBytes();
            final ClusterSpecification clusterSpecification =
                    new ClusterSpecification.ClusterSpecificationBuilder()
                            .setMasterMemoryMB(masterMemory)
                            .setTaskManagerMemoryMB(1024)
                            .setSlotsPerTaskManager(1)
                            .createClusterSpecification();

            try (ClusterClient<ApplicationId> clusterClient =
                    yarnClusterDescriptor
                            .deployApplicationCluster(
                                    clusterSpecification,
                                    ApplicationConfiguration.fromConfiguration(configuration))
                            .getClusterClient()) {

                ApplicationId applicationId = clusterClient.getClusterId();

                waitApplicationFinishedElseKillIt(
                        applicationId,
                        yarnAppTerminateTimeout,
                        yarnClusterDescriptor,
                        sleepIntervalInMS);
            }
        }
    }

    private Path getTestingJar() throws FileNotFoundException {
        return new Path(getTestJarPath("StreamingWordCount.jar").toURI());
    }

    private Configuration createDefaultConfiguration(
            Path userJar, YarnConfigOptions.UserJarInclusion userJarInclusion) {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(768));
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        configuration.setString(AkkaOptions.ASK_TIMEOUT, "30 s");
        configuration.set(DeploymentOptions.TARGET, YarnDeploymentTarget.APPLICATION.getName());
        configuration.setString(CLASSPATH_INCLUDE_USER_JAR, userJarInclusion.toString());
        configuration.set(PipelineOptions.JARS, Collections.singletonList(userJar.toString()));

        return configuration;
    }
}
