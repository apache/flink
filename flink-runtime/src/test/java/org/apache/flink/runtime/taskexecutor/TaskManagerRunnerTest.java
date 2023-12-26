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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenReceiverRepository;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TimeUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TaskManagerRunner}. */
class TaskManagerRunnerTest {

    @TempDir private Path temporaryFolder;

    private TaskManagerRunner taskManagerRunner;

    @AfterEach
    void after() throws Exception {
        if (taskManagerRunner != null) {
            taskManagerRunner.close();
        }
    }

    @Test
    void testShouldShutdownOnFatalError() throws Exception {
        Configuration configuration = createConfiguration();
        // very high timeout, to ensure that we don't fail because of registration timeouts
        configuration.set(TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("42 h"));
        taskManagerRunner = createTaskManagerRunner(configuration);

        taskManagerRunner.onFatalError(new RuntimeException());

        assertThatFuture(taskManagerRunner.getTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(TaskManagerRunner.Result.FAILURE);
    }

    @Test
    void testShouldShutdownIfRegistrationWithJobManagerFails() throws Exception {
        Configuration configuration = createConfiguration();
        configuration.set(
                TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("10 ms"));
        taskManagerRunner = createTaskManagerRunner(configuration);

        assertThatFuture(taskManagerRunner.getTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(TaskManagerRunner.Result.FAILURE);
    }

    @Test
    void testGenerateTaskManagerResourceIDWithMetaData() throws Exception {
        final Configuration configuration = createConfiguration();
        final String metadata = "test";
        configuration.set(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, metadata);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1).unwrap();

        assertThat(taskManagerResourceID.getMetadata()).isEqualTo(metadata);
    }

    @Test
    void testGenerateTaskManagerResourceIDWithoutMetaData() throws Exception {
        final Configuration configuration = createConfiguration();
        final String resourceID = "test";
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, resourceID);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1).unwrap();

        assertThat(taskManagerResourceID.getMetadata()).isEmpty();
        assertThat(taskManagerResourceID.getStringWithMetadata()).isEqualTo("test");
    }

    @Test
    void testGenerateTaskManagerResourceIDWithConfig() throws Exception {
        final Configuration configuration = createConfiguration();
        final String resourceID = "test";
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, resourceID);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1).unwrap();

        assertThat(taskManagerResourceID.getResourceIdString()).isEqualTo(resourceID);
    }

    @Test
    void testGenerateTaskManagerResourceIDWithRemoteRpcService() throws Exception {
        final Configuration configuration = createConfiguration();
        final String rpcAddress = "flink";
        final int rpcPort = 9090;
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, rpcAddress, rpcPort)
                        .unwrap();

        assertThat(taskManagerResourceID).isNotNull();
        assertThat(taskManagerResourceID.getResourceIdString())
                .contains(rpcAddress + ":" + rpcPort);
    }

    @Test
    void testGenerateTaskManagerResourceIDWithLocalRpcService() throws Exception {
        final Configuration configuration = createConfiguration();
        final String rpcAddress = "";
        final int rpcPort = -1;
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, rpcAddress, rpcPort)
                        .unwrap();

        assertThat(taskManagerResourceID).isNotNull();
        assertThat(taskManagerResourceID.getResourceIdString())
                .contains(InetAddress.getLocalHost().getHostName());
    }

    @Test
    void testUnexpectedTaskManagerTerminationFailsRunnerFatally() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final TestingTaskExecutorService taskExecutorService =
                TestingTaskExecutorService.newBuilder()
                        .setTerminationFuture(terminationFuture)
                        .build();
        final TaskManagerRunner taskManagerRunner =
                createTaskManagerRunner(
                        createConfiguration(),
                        createTaskExecutorServiceFactory(taskExecutorService));

        terminationFuture.complete(null);

        assertThatFuture(taskManagerRunner.getTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(TaskManagerRunner.Result.FAILURE);
    }

    @Test
    void testUnexpectedTaskManagerTerminationAfterRunnerCloseWillBeIgnored() throws Exception {
        final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
        final TestingTaskExecutorService taskExecutorService =
                TestingTaskExecutorService.newBuilder()
                        .setTerminationFuture(terminationFuture)
                        .withManualTerminationFutureCompletion()
                        .build();
        final TaskManagerRunner taskManagerRunner =
                createTaskManagerRunner(
                        createConfiguration(),
                        createTaskExecutorServiceFactory(taskExecutorService));

        taskManagerRunner.closeAsync();

        terminationFuture.complete(null);

        assertThatFuture(taskManagerRunner.getTerminationFuture())
                .eventuallySucceeds()
                .isEqualTo(TaskManagerRunner.Result.SUCCESS);
    }

    @Test
    void testWorkingDirIsSetupWhenStartingTaskManagerRunner() throws Exception {
        final File workingDirBase = TempDirUtils.newFolder(temporaryFolder);
        final ResourceID taskManagerResourceId = new ResourceID("foobar");

        final Configuration configuration =
                createConfigurationWithWorkingDirectory(workingDirBase, taskManagerResourceId);
        final File workingDir =
                ClusterEntrypointUtils.generateTaskManagerWorkingDirectoryFile(
                        configuration, taskManagerResourceId);
        final TaskManagerRunner taskManagerRunner = createTaskManagerRunner(configuration);

        try {
            assertThat(workingDir).exists();
        } finally {
            taskManagerRunner.close();
        }

        assertThat(workingDir)
                .withFailMessage(
                        "The working dir should be cleaned up when stopping the TaskManager process gracefully.")
                .doesNotExist();
    }

    @Nonnull
    private Configuration createConfigurationWithWorkingDirectory(
            File workingDirBase, ResourceID taskManagerResourceId) {
        final Configuration configuration = createConfiguration();

        configuration.set(
                ClusterOptions.PROCESS_WORKING_DIR_BASE, workingDirBase.getAbsolutePath());
        configuration.set(
                TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, taskManagerResourceId.toString());
        return configuration;
    }

    @Test
    void testWorkingDirIsNotDeletedInCaseOfFailure() throws Exception {
        final File workingDirBase = TempDirUtils.newFolder(temporaryFolder);
        final ResourceID resourceId = ResourceID.generate();

        final Configuration configuration =
                createConfigurationWithWorkingDirectory(workingDirBase, resourceId);

        final TaskManagerRunner taskManagerRunner =
                createTaskManagerRunner(
                        configuration, new TestingFailingTaskExecutorServiceFactory());

        taskManagerRunner.getTerminationFuture().join();

        assertThat(
                        ClusterEntrypointUtils.generateTaskManagerWorkingDirectoryFile(
                                configuration, resourceId))
                .exists();
    }

    @Nonnull
    private TaskManagerRunner.TaskExecutorServiceFactory createTaskExecutorServiceFactory(
            TestingTaskExecutorService taskExecutorService) {
        return (configuration,
                resourceID,
                rpcService,
                highAvailabilityServices,
                heartbeatServices,
                metricRegistry,
                blobCacheService,
                localCommunicationOnly,
                externalResourceInfoProvider,
                workingDirectory,
                fatalErrorHandler,
                delegationTokenReceiverRepository) -> taskExecutorService;
    }

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.setString(JobManagerOptions.ADDRESS, "localhost");
        configuration.setString(TaskManagerOptions.HOST, "localhost");
        return TaskExecutorResourceUtils.adjustForLocalExecution(configuration);
    }

    private static TaskManagerRunner createTaskManagerRunner(final Configuration configuration)
            throws Exception {
        return createTaskManagerRunner(configuration, TaskManagerRunner::createTaskExecutorService);
    }

    private static TaskManagerRunner createTaskManagerRunner(
            final Configuration configuration,
            TaskManagerRunner.TaskExecutorServiceFactory taskExecutorServiceFactory)
            throws Exception {
        final PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);
        TaskManagerRunner taskManagerRunner =
                new TaskManagerRunner(configuration, pluginManager, taskExecutorServiceFactory);
        taskManagerRunner.start();
        return taskManagerRunner;
    }

    private static class TestingFailingTaskExecutorServiceFactory
            implements TaskManagerRunner.TaskExecutorServiceFactory {
        @Override
        public TaskManagerRunner.TaskExecutorService createTaskExecutor(
                Configuration configuration,
                ResourceID resourceID,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                MetricRegistry metricRegistry,
                BlobCacheService blobCacheService,
                boolean localCommunicationOnly,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                WorkingDirectory workingDirectory,
                FatalErrorHandler fatalErrorHandler,
                DelegationTokenReceiverRepository delegationTokenReceiverRepository) {
            return TestingTaskExecutorService.newBuilder()
                    .setStartRunnable(
                            () ->
                                    fatalErrorHandler.onFatalError(
                                            new FlinkException(
                                                    "Cannot instantiate the TaskExecutorService.")))
                    .build();
        }
    }
}
