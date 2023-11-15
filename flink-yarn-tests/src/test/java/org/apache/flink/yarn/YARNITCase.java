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

import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.testjob.YarnTestArchiveJob;
import org.apache.flink.yarn.testjob.YarnTestCacheJob;
import org.apache.flink.yarn.util.TestUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.yarn.configuration.YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR;
import static org.assertj.core.api.Assertions.assertThat;

/** Test cases for the deployment of Yarn Flink clusters. */
class YARNITCase extends YarnTestBase {

    private static final Duration yarnAppTerminateTimeout = Duration.ofSeconds(10);
    private static final int sleepIntervalInMS = 100;

    @BeforeAll
    public static void setup() {
        YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, "flink-yarn-tests-per-job");
        startYARNWithConfig(YARN_CONFIGURATION, true);
    }

    @Test
    void testPerJobModeWithEnableSystemClassPathIncludeUserJar() throws Exception {
        runTest(
                () ->
                        deployPerJob(
                                createDefaultConfiguration(
                                        YarnConfigOptions.UserJarInclusion.FIRST),
                                getTestingJobGraph(),
                                true));
    }

    @Test
    void testPerJobModeWithDisableSystemClassPathIncludeUserJar() throws Exception {
        runTest(
                () ->
                        deployPerJob(
                                createDefaultConfiguration(
                                        YarnConfigOptions.UserJarInclusion.DISABLED),
                                getTestingJobGraph(),
                                true));
    }

    @Test
    void testPerJobModeWithDistributedCache(@TempDir File tempDir) throws Exception {
        runTest(
                () ->
                        deployPerJob(
                                createDefaultConfiguration(
                                        YarnConfigOptions.UserJarInclusion.DISABLED),
                                YarnTestCacheJob.getDistributedCacheJobGraph(tempDir),
                                true));
    }

    @Test
    void testPerJobWithProvidedLibDirs() throws Exception {
        final Path remoteLib =
                new Path(
                        miniDFSCluster.getFileSystem().getUri().toString() + "/flink-provided-lib");
        miniDFSCluster
                .getFileSystem()
                .copyFromLocalFile(new Path(flinkLibFolder.toURI()), remoteLib);
        miniDFSCluster.getFileSystem().setPermission(remoteLib, new FsPermission("755"));

        final Configuration flinkConfig =
                createDefaultConfiguration(YarnConfigOptions.UserJarInclusion.DISABLED);
        flinkConfig.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                Collections.singletonList(remoteLib.toString()));
        runTest(() -> deployPerJob(flinkConfig, getTestingJobGraph(), false));
    }

    @Test
    void testPerJobWithArchive(@TempDir File tempDir) throws Exception {
        final Configuration flinkConfig =
                createDefaultConfiguration(YarnConfigOptions.UserJarInclusion.DISABLED);
        final JobGraph archiveJobGraph =
                YarnTestArchiveJob.getArchiveJobGraph(tempDir, flinkConfig);
        runTest(() -> deployPerJob(flinkConfig, archiveJobGraph, true));
    }

    private void deployPerJob(Configuration configuration, JobGraph jobGraph, boolean withDist)
            throws Exception {
        jobGraph.setJobType(JobType.STREAMING);
        try (final YarnClusterDescriptor yarnClusterDescriptor =
                withDist
                        ? createYarnClusterDescriptor(configuration)
                        : createYarnClusterDescriptorWithoutLibDir(configuration)) {

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

            File testingJar =
                    TestUtils.findFile("..", new TestUtils.TestJarFinder("flink-yarn-tests"));

            jobGraph.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
            try (ClusterClient<ApplicationId> clusterClient =
                    yarnClusterDescriptor
                            .deployJobCluster(clusterSpecification, jobGraph, false)
                            .getClusterClient()) {

                for (DistributedCache.DistributedCacheEntry entry :
                        jobGraph.getUserArtifacts().values()) {
                    assertThat(Utils.isRemotePath(entry.filePath)).isTrue();
                }

                ApplicationId applicationId = clusterClient.getClusterId();

                final CompletableFuture<JobResult> jobResultCompletableFuture =
                        clusterClient.requestJobResult(jobGraph.getJobID());

                final JobResult jobResult = jobResultCompletableFuture.get();

                assertThat(jobResult).isNotNull();
                assertThat(jobResult.getSerializedThrowable()).isNotPresent();

                checkStagingDirectory(configuration, applicationId);

                waitApplicationFinishedElseKillIt(
                        applicationId,
                        yarnAppTerminateTimeout,
                        yarnClusterDescriptor,
                        sleepIntervalInMS);
            }
        }
    }

    private void checkStagingDirectory(Configuration flinkConfig, ApplicationId appId)
            throws IOException {
        final List<String> providedLibDirs = flinkConfig.get(YarnConfigOptions.PROVIDED_LIB_DIRS);
        final boolean isProvidedLibDirsConfigured =
                providedLibDirs != null && !providedLibDirs.isEmpty();

        try (final FileSystem fs = FileSystem.get(YARN_CONFIGURATION)) {
            final Path stagingDirectory =
                    new Path(fs.getHomeDirectory(), ".flink/" + appId.toString());
            if (isProvidedLibDirsConfigured) {
                assertThat(fs.exists(new Path(stagingDirectory, flinkLibFolder.getName())))
                        .isFalse();
            } else {
                assertThat(fs.exists(new Path(stagingDirectory, flinkLibFolder.getName())))
                        .isTrue();
            }
        }
    }

    private JobGraph getTestingJobGraph() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.addSource(new NoDataSource()).shuffle().sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private Configuration createDefaultConfiguration(
            YarnConfigOptions.UserJarInclusion userJarInclusion) {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(768));
        configuration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        configuration.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofSeconds(30));
        configuration.set(CLASSPATH_INCLUDE_USER_JAR, userJarInclusion);

        return configuration;
    }
}
