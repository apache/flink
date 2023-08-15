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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.util.concurrent.Executors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.yarn.util.TestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;

/** Test cases which ensure that the Yarn containers are started with the correct settings. */
class YarnConfigurationITCase extends YarnTestBase {

    private static final Time TIMEOUT = Time.seconds(10L);

    /** Tests that the Flink components are started with the correct memory settings. */
    @Test
    void testFlinkContainerMemory() throws Exception {
        runTest(
                () -> {
                    final YarnClient yarnClient = getYarnClient();
                    final Configuration configuration = new Configuration(flinkConfiguration);

                    final int slotsPerTaskManager = 3;
                    configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, slotsPerTaskManager);
                    final int masterMemory = 768;
                    configuration.set(
                            JobManagerOptions.TOTAL_PROCESS_MEMORY,
                            MemorySize.ofMebiBytes(masterMemory));

                    final TaskExecutorProcessSpec tmResourceSpec =
                            TaskExecutorProcessUtils.processSpecFromConfig(configuration);
                    final int taskManagerMemory =
                            tmResourceSpec.getTotalProcessMemorySize().getMebiBytes();

                    final YarnConfiguration yarnConfiguration = getYarnConfiguration();
                    final YarnClusterDescriptor clusterDescriptor =
                            new YarnClusterDescriptor(
                                    configuration,
                                    yarnConfiguration,
                                    yarnClient,
                                    YarnClientYarnClusterInformationRetriever.create(yarnClient),
                                    true);

                    clusterDescriptor.setLocalJarPath(new Path(flinkUberjar.getAbsolutePath()));
                    clusterDescriptor.addShipFiles(
                            Arrays.stream(Objects.requireNonNull(flinkLibFolder.listFiles()))
                                    .map(file -> new Path(file.toURI()))
                                    .collect(Collectors.toList()));

                    final File streamingWordCountFile = getTestJarPath("WindowJoin.jar");

                    final PackagedProgram packagedProgram =
                            PackagedProgram.newBuilder().setJarFile(streamingWordCountFile).build();
                    final JobGraph jobGraph =
                            PackagedProgramUtils.createJobGraph(
                                    packagedProgram, configuration, 1, false);

                    try {
                        final ClusterSpecification clusterSpecification =
                                new ClusterSpecification.ClusterSpecificationBuilder()
                                        .setMasterMemoryMB(masterMemory)
                                        .setTaskManagerMemoryMB(taskManagerMemory)
                                        .setSlotsPerTaskManager(slotsPerTaskManager)
                                        .createClusterSpecification();

                        final ClusterClient<ApplicationId> clusterClient =
                                clusterDescriptor
                                        .deployJobCluster(clusterSpecification, jobGraph, true)
                                        .getClusterClient();

                        final ApplicationId clusterId = clusterClient.getClusterId();

                        final RestClient restClient =
                                new RestClient(configuration, Executors.directExecutor());

                        try {
                            final ApplicationReport applicationReport =
                                    yarnClient.getApplicationReport(clusterId);

                            final ApplicationAttemptId currentApplicationAttemptId =
                                    applicationReport.getCurrentApplicationAttemptId();

                            // wait until we have second container allocated
                            List<ContainerReport> containers =
                                    yarnClient.getContainers(currentApplicationAttemptId);

                            while (containers.size() < 2) {
                                // this is nasty but Yarn does not offer a better way to wait
                                Thread.sleep(50L);
                                containers = yarnClient.getContainers(currentApplicationAttemptId);
                            }

                            for (ContainerReport container : containers) {
                                if (container.getContainerId().getId() == 1) {
                                    // this should be the application master
                                    assertThat(container.getAllocatedResource().getMemorySize())
                                            .isEqualTo(masterMemory);
                                } else {
                                    assertThat(container.getAllocatedResource().getMemorySize())
                                            .isEqualTo(taskManagerMemory);
                                }
                            }

                            final URI webURI = new URI(clusterClient.getWebInterfaceURL());

                            CompletableFuture<TaskManagersInfo> taskManagersInfoCompletableFuture;
                            Collection<TaskManagerInfo> taskManagerInfos;

                            while (true) {
                                taskManagersInfoCompletableFuture =
                                        restClient.sendRequest(
                                                webURI.getHost(),
                                                webURI.getPort(),
                                                TaskManagersHeaders.getInstance(),
                                                EmptyMessageParameters.getInstance(),
                                                EmptyRequestBody.getInstance());

                                final TaskManagersInfo taskManagersInfo =
                                        taskManagersInfoCompletableFuture.get();

                                taskManagerInfos = taskManagersInfo.getTaskManagerInfos();

                                // wait until the task manager has registered and reported its slots
                                if (hasTaskManagerConnectedAndReportedSlots(taskManagerInfos)) {
                                    break;
                                } else {
                                    Thread.sleep(100L);
                                }
                            }

                            // there should be at least one TaskManagerInfo
                            final TaskManagerInfo taskManagerInfo =
                                    taskManagerInfos.iterator().next();

                            assertThat(taskManagerInfo.getNumberSlots())
                                    .isEqualTo(slotsPerTaskManager);

                            final long expectedHeapSizeBytes =
                                    tmResourceSpec.getJvmHeapMemorySize().getBytes();

                            // We compare here physical memory assigned to a container with the heap
                            // memory that we should pass to
                            // jvm as Xmx parameter. Those value might differ significantly due to
                            // system page size or jvm
                            // implementation therefore we use 15% threshold here.
                            assertThat(
                                            (double)
                                                            taskManagerInfo
                                                                    .getHardwareDescription()
                                                                    .getSizeOfJvmHeap()
                                                    / (double) expectedHeapSizeBytes)
                                    .isCloseTo(1.0, Offset.offset(0.15));

                            final int expectedManagedMemoryMB =
                                    tmResourceSpec.getManagedMemorySize().getMebiBytes();

                            assertThat(
                                            (int)
                                                    (taskManagerInfo
                                                                    .getHardwareDescription()
                                                                    .getSizeOfManagedMemory()
                                                            >> 20))
                                    .isEqualTo(expectedManagedMemoryMB);
                        } finally {
                            restClient.shutdown(TIMEOUT);
                            clusterClient.close();
                        }

                        clusterDescriptor.killCluster(clusterId);

                    } finally {
                        clusterDescriptor.close();
                    }
                });
    }

    private boolean hasTaskManagerConnectedAndReportedSlots(
            Collection<TaskManagerInfo> taskManagerInfos) {
        if (taskManagerInfos.isEmpty()) {
            return false;
        } else {
            final TaskManagerInfo taskManagerInfo = taskManagerInfos.iterator().next();
            return taskManagerInfo.getNumberSlots() > 0;
        }
    }
}
