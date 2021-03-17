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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TimeUtils;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/** Tests for the {@link TaskManagerRunner}. */
public class TaskManagerRunnerTest extends TestLogger {

    @Rule public final Timeout timeout = Timeout.seconds(30);

    private TaskManagerRunner taskManagerRunner;

    @After
    public void after() throws Exception {
        System.setSecurityManager(null);
        if (taskManagerRunner != null) {
            taskManagerRunner.close();
        }
    }

    @Test
    public void testShouldShutdownOnFatalError() throws Exception {
        Configuration configuration = createConfiguration();
        // very high timeout, to ensure that we don't fail because of registration timeouts
        configuration.set(TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("42 h"));
        taskManagerRunner = createTaskManagerRunner(configuration);

        taskManagerRunner.onFatalError(new RuntimeException());

        assertThat(
                taskManagerRunner.getTerminationFuture().join(),
                is(equalTo(TaskManagerRunner.Result.FAILURE)));
    }

    @Test
    public void testShouldShutdownIfRegistrationWithJobManagerFails() throws Exception {
        Configuration configuration = createConfiguration();
        configuration.set(
                TaskManagerOptions.REGISTRATION_TIMEOUT, TimeUtils.parseDuration("10 ms"));
        taskManagerRunner = createTaskManagerRunner(configuration);

        assertThat(
                taskManagerRunner.getTerminationFuture().join(),
                is(equalTo(TaskManagerRunner.Result.FAILURE)));
    }

    @Test
    public void testGenerateTaskManagerResourceIDWithMetaData() throws Exception {
        final Configuration configuration = createConfiguration();
        final String metadata = "test";
        configuration.set(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, metadata);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1);

        assertThat(taskManagerResourceID.getMetadata(), equalTo(metadata));
    }

    @Test
    public void testGenerateTaskManagerResourceIDWithoutMetaData() throws Exception {
        final Configuration configuration = createConfiguration();
        final String resourceID = "test";
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, resourceID);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1);

        assertThat(taskManagerResourceID.getMetadata(), equalTo(""));
        assertThat(taskManagerResourceID.getStringWithMetadata(), equalTo("test"));
    }

    @Test
    public void testGenerateTaskManagerResourceIDWithConfig() throws Exception {
        final Configuration configuration = createConfiguration();
        final String resourceID = "test";
        configuration.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, resourceID);
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, "", -1);

        assertThat(taskManagerResourceID.getResourceIdString(), equalTo(resourceID));
    }

    @Test
    public void testGenerateTaskManagerResourceIDWithRemoteRpcService() throws Exception {
        final Configuration configuration = createConfiguration();
        final String rpcAddress = "flink";
        final int rpcPort = 9090;
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, rpcAddress, rpcPort);

        assertThat(taskManagerResourceID, notNullValue());
        assertThat(
                taskManagerResourceID.getResourceIdString(),
                containsString(rpcAddress + ":" + rpcPort));
    }

    @Test
    public void testGenerateTaskManagerResourceIDWithLocalRpcService() throws Exception {
        final Configuration configuration = createConfiguration();
        final String rpcAddress = "";
        final int rpcPort = -1;
        final ResourceID taskManagerResourceID =
                TaskManagerRunner.getTaskManagerResourceID(configuration, rpcAddress, rpcPort);

        assertThat(taskManagerResourceID, notNullValue());
        assertThat(
                taskManagerResourceID.getResourceIdString(),
                containsString(InetAddress.getLocalHost().getHostName()));
    }

    @Test
    public void testUnexpectedTaskManagerTerminationFailsRunnerFatally() throws Exception {
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

        assertThat(
                taskManagerRunner.getTerminationFuture().join(),
                is(equalTo(TaskManagerRunner.Result.FAILURE)));
    }

    @Test
    public void testUnexpectedTaskManagerTerminationAfterRunnerCloseWillBeIgnored()
            throws Exception {
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

        assertThat(
                taskManagerRunner.getTerminationFuture().join(),
                is(equalTo(TaskManagerRunner.Result.SUCCESS)));
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
                fatalErrorHandler) -> taskExecutorService;
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
}
