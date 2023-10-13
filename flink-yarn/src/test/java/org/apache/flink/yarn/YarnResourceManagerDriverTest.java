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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriverTestBase;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_DIST_JAR;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManagerDriver.ERROR_MESSAGE_ON_SHUTDOWN_REQUEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for {@link YarnResourceManagerDriver}. */
public class YarnResourceManagerDriverTest extends ResourceManagerDriverTestBase<YarnWorkerNode> {

    private static final Logger log = LoggerFactory.getLogger(YarnResourceManagerDriverTest.class);

    private static final Resource testingResource =
            Resource.newInstance(
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES);
    private static final Priority testingPriority = Priority.newInstance(1);
    private static final Container testingContainer =
            createTestingContainerWithResource(testingResource, testingPriority, 1);
    private static final TaskExecutorProcessSpec testingTaskExecutorProcessSpec =
            new TaskExecutorProcessSpec(
                    new CPUResource(1),
                    MemorySize.ZERO,
                    MemorySize.ZERO,
                    MemorySize.ofMebiBytes(256),
                    MemorySize.ofMebiBytes(256),
                    MemorySize.ofMebiBytes(256),
                    MemorySize.ofMebiBytes(256),
                    MemorySize.ZERO,
                    MemorySize.ZERO,
                    Collections.emptyList());

    @TempDir private java.nio.file.Path tmpFolder;

    @Override
    protected Context createContext() {
        return new Context();
    }

    @Test
    void testRunAsyncCausesFatalError() throws Exception {
        new Context() {
            {
                final String exceptionMessage = "runAsyncCausesFatalError";
                addContainerRequestFutures.add(CompletableFuture.completedFuture(null));

                testingYarnAMRMClientAsyncBuilder.setGetMatchingRequestsFunction(
                        ignored -> {
                            throw new RuntimeException(exceptionMessage);
                        });

                final CompletableFuture<Throwable> throwableCompletableFuture =
                        new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(
                        throwableCompletableFuture::complete);

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getDriver()
                                                    .requestResource(
                                                            testingTaskExecutorProcessSpec));
                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));

                            Throwable t =
                                    throwableCompletableFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            final Optional<RuntimeException> optionalCause =
                                    ExceptionUtils.findThrowable(t, RuntimeException.class);

                            assertThat(optionalCause).isPresent();
                            assertThat(optionalCause.get()).hasMessage(exceptionMessage);
                        });
            }
        };
    }

    @Test
    void testShutdownRequestCausesFatalError() throws Exception {
        new Context() {
            {
                final CompletableFuture<Throwable> throwableCompletableFuture =
                        new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(
                        throwableCompletableFuture::complete);
                runTest(
                        () -> {
                            resourceManagerClientCallbackHandler.onShutdownRequest();

                            Throwable throwable =
                                    throwableCompletableFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(throwable)
                                    .satisfies(anyCauseMatches(ResourceManagerException.class))
                                    .satisfies(anyCauseMatches(ERROR_MESSAGE_ON_SHUTDOWN_REQUEST));
                        });
            }
        };
    }

    @Test
    void testOnErrorCausesFatalError() throws Exception {
        new Context() {
            {
                final CompletableFuture<Throwable> throwableCompletableFuture =
                        new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(
                        throwableCompletableFuture::complete);

                Throwable expectedThrowable = new Exception("test");
                runTest(
                        () -> {
                            resourceManagerClientCallbackHandler.onError(expectedThrowable);

                            Throwable actualThrowable =
                                    throwableCompletableFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(actualThrowable).isEqualTo(expectedThrowable);
                        });
            }
        };
    }

    @Test
    void testOnErrorAfterTerminationIgnored() throws Exception {
        new Context() {
            {
                final CompletableFuture<Throwable> throwableCompletableFuture =
                        new CompletableFuture<>();
                resourceEventHandlerBuilder.setOnErrorConsumer(
                        throwableCompletableFuture::complete);

                Throwable expectedThrowable = new Exception("test");
                runTest(
                        () -> {
                            getDriver().terminate();
                            resourceManagerClientCallbackHandler.onError(expectedThrowable);

                            assertThatThrownBy(
                                            () ->
                                                    throwableCompletableFuture.get(
                                                            TIMEOUT_SHOULD_NOT_HAPPEN_MS,
                                                            TimeUnit.MILLISECONDS))
                                    .isInstanceOf(TimeoutException.class);
                        });
            }
        };
    }

    @Test
    void testTerminationDoesNotBlock() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            try {
                                runInMainThread(() -> getDriver().terminate());
                            } catch (Exception ex) {
                                log.error("cannot terminate driver", ex);
                                fail("termination of driver failed");
                            }
                        });
            }
        };
    }

    @Test
    void testTerminationWaitsOnContainerStopSuccess() throws Exception {
        new Context() {
            {
                final CompletableFuture<ContainerId> containerIdFuture = new CompletableFuture<>();
                testingYarnNMClientAsyncBuilder.setStopContainerAsyncConsumer(
                        (containerId, ignored, callbackHandler) ->
                                containerIdFuture.complete(containerId));

                resetYarnNodeManagerClientFactory();

                runTest(
                        () -> {
                            // acquire a resource so we have something to release
                            final CompletableFuture<YarnWorkerNode> yarnWorkerFuture =
                                    runInMainThread(
                                                    () ->
                                                            getDriver()
                                                                    .requestResource(
                                                                            testingTaskExecutorProcessSpec))
                                            .thenCompose(Function.identity());

                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));

                            final YarnWorkerNode worker = yarnWorkerFuture.get();

                            // release the resource -- it will be blocked
                            // terminate driver this should wait on the callback
                            final CompletableFuture<Void> driverHasTerminatedFuture =
                                    runInMainThread(
                                            () -> {
                                                getDriver().releaseResource(worker);
                                                getDriver().terminate();
                                            });

                            assertThatThrownBy(
                                            () ->
                                                    driverHasTerminatedFuture.get(
                                                            20, TimeUnit.MILLISECONDS))
                                    .isInstanceOf(TimeoutException.class);

                            nodeManagerClientCallbackHandler.onContainerStopped(
                                    containerIdFuture.get());

                            // wait for completion of termination
                            // if this blocks forever, then our implementation is wrong
                            driverHasTerminatedFuture.get();
                        });
            }
        };
    }

    @Test
    void testTerminationWaitsOnContainerStopError() throws Exception {
        new Context() {
            {
                final CompletableFuture<ContainerId> containerIdFuture = new CompletableFuture<>();
                testingYarnNMClientAsyncBuilder.setStopContainerAsyncConsumer(
                        (containerId, ignored, callbackHandler) ->
                                containerIdFuture.complete(containerId));

                resetYarnNodeManagerClientFactory();

                runTest(
                        () -> {
                            // acquire a resource so we have something to release
                            final CompletableFuture<YarnWorkerNode> yarnWorkerFuture =
                                    runInMainThread(
                                                    () ->
                                                            getDriver()
                                                                    .requestResource(
                                                                            testingTaskExecutorProcessSpec))
                                            .thenCompose(Function.identity());

                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));

                            final YarnWorkerNode worker = yarnWorkerFuture.get();

                            // release the resource -- it will be blocked
                            // terminate driver this should wait on the callback
                            final CompletableFuture<Void> driverHasTerminatedFuture =
                                    runInMainThread(
                                            () -> {
                                                getDriver().releaseResource(worker);
                                                getDriver().terminate();
                                            });
                            assertThatThrownBy(
                                            () ->
                                                    driverHasTerminatedFuture.get(
                                                            20, TimeUnit.MILLISECONDS))
                                    .isInstanceOf(TimeoutException.class);

                            nodeManagerClientCallbackHandler.onStopContainerError(
                                    containerIdFuture.get(), null);

                            // wait for completion of termination
                            // if this blocks forever, then our implementation is wrong
                            driverHasTerminatedFuture.get();
                        });
            }
        };
    }

    /**
     * Tests that application files are deleted when the YARN application master is de-registered.
     */
    @Test
    void testDeleteApplicationFiles() throws Exception {
        new Context() {
            {
                final File applicationDir = Files.createTempDirectory(tmpFolder, ".flink").toFile();
                env.put(FLINK_YARN_FILES, applicationDir.getCanonicalPath());

                runTest(
                        () -> {
                            getDriver().deregisterApplication(ApplicationStatus.SUCCEEDED, null);
                            assertThat(applicationDir.toPath()).doesNotExist();
                        });
            }
        };
    }

    @Test
    void testOnContainerAllocated() throws Exception {
        new Context() {
            {
                addContainerRequestFutures.add(new CompletableFuture<>());

                testingYarnAMRMClientAsyncBuilder.setAddContainerRequestConsumer(
                        (ignored1, ignored2) ->
                                addContainerRequestFutures
                                        .get(
                                                addContainerRequestFuturesNumCompleted
                                                        .getAndIncrement())
                                        .complete(null));

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getDriver()
                                                    .requestResource(
                                                            testingTaskExecutorProcessSpec));
                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));

                            verifyFutureCompleted(addContainerRequestFutures.get(0));
                            verifyFutureCompleted(removeContainerRequestFuture);
                            verifyFutureCompleted(startContainerAsyncFuture);
                        });
            }
        };
    }

    @Test
    void testCancelRequestedResource() throws Exception {
        new Context() {
            {
                addContainerRequestFutures.add(new CompletableFuture<>());

                testingYarnAMRMClientAsyncBuilder.setAddContainerRequestConsumer(
                        (ignored1, ignored2) ->
                                addContainerRequestFutures
                                        .get(
                                                addContainerRequestFuturesNumCompleted
                                                        .getAndIncrement())
                                        .complete(null));

                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        CompletableFuture<YarnWorkerNode> requestFuture =
                                                getDriver()
                                                        .requestResource(
                                                                testingTaskExecutorProcessSpec);
                                        requestFuture.cancel(true);
                                    });

                            verifyFutureCompleted(addContainerRequestFutures.get(0));
                            verifyFutureCompleted(removeContainerRequestFuture);
                            assertThat(startContainerAsyncFuture.isDone()).isFalse();
                        });
            }
        };
    }

    @Test
    void testUpdateBlocklist() throws Exception {
        new Context() {
            {
                final Set<String> yarnReceivedBlocklist = new HashSet<>();
                testingYarnAMRMClientAsyncBuilder.setUpdateBlocklistConsumer(
                        (additions, removals) -> {
                            if (additions != null) {
                                yarnReceivedBlocklist.addAll(additions);
                            }

                            if (removals != null) {
                                yarnReceivedBlocklist.removeAll(removals);
                            }
                        });

                final Set<String> blockedNodes = new HashSet<>();
                setBlockedNodeRetriever(() -> blockedNodes);
                runTest(
                        () -> {
                            blockedNodes.addAll(Arrays.asList("node1", "node2", "node3"));
                            runInMainThread(
                                            () ->
                                                    getDriver()
                                                            .requestResource(
                                                                    TASK_EXECUTOR_PROCESS_SPEC))
                                    .get();
                            assertThat(yarnReceivedBlocklist)
                                    .containsExactlyInAnyOrder("node1", "node2", "node3");

                            blockedNodes.remove("node1");
                            runInMainThread(
                                            () ->
                                                    getDriver()
                                                            .requestResource(
                                                                    TASK_EXECUTOR_PROCESS_SPEC))
                                    .get();
                            assertThat(yarnReceivedBlocklist)
                                    .containsExactlyInAnyOrder("node2", "node3");
                        });
            }
        };
    }

    @Test
    void testOnSuccessfulContainerCompleted() throws Exception {
        runTestOnContainerCompleted(createSuccessfulCompletedContainerStatus());
    }

    @Test
    void testOnContainerCompletedBecauseDisksFailed() throws Exception {
        runTestOnContainerCompleted(createCompletedContainerStatusBecauseDisksFailed());
    }

    @Test
    void testOnContainerCompletedBecauseItWasAborted() throws Exception {
        runTestOnContainerCompleted(createCompletedContainerStatusBecauseItWasAborted());
    }

    @Test
    void testOnContainerCompletedBecauseItWasInvalid() throws Exception {
        runTestOnContainerCompleted(createCompletedContainerStatusBecauseItWasInvalid());
    }

    @Test
    void testOnContainerCompletedForUnknownCause() throws Exception {
        runTestOnContainerCompleted(createCompletedContainerStatusForUnknownCause());
    }

    @Test
    void testOnContainerCompletedBecauseItWasPreempted() throws Exception {
        runTestOnContainerCompleted(createCompletedContainerStatusBecauseItWasPreempted());
    }

    void runTestOnContainerCompleted(ContainerStatus completedContainerStatus) throws Exception {
        new Context() {
            {
                addContainerRequestFutures.add(new CompletableFuture<>());
                addContainerRequestFutures.add(new CompletableFuture<>());

                testingYarnAMRMClientAsyncBuilder.setAddContainerRequestConsumer(
                        (ignored1, ignored2) ->
                                addContainerRequestFutures
                                        .get(
                                                addContainerRequestFuturesNumCompleted
                                                        .getAndIncrement())
                                        .complete(null));
                resourceEventHandlerBuilder.setOnWorkerTerminatedConsumer(
                        (ignore1, ignore2) ->
                                getDriver().requestResource(testingTaskExecutorProcessSpec));

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getDriver()
                                                    .requestResource(
                                                            testingTaskExecutorProcessSpec));
                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));
                            resourceManagerClientCallbackHandler.onContainersCompleted(
                                    ImmutableList.of(completedContainerStatus));

                            verifyFutureCompleted(addContainerRequestFutures.get(1));
                        });
            }
        };
    }

    @Test
    void testOnStartContainerError() throws Exception {
        new Context() {
            {
                addContainerRequestFutures.add(new CompletableFuture<>());
                addContainerRequestFutures.add(new CompletableFuture<>());

                testingYarnAMRMClientAsyncBuilder.setAddContainerRequestConsumer(
                        (ignored1, ignored2) ->
                                addContainerRequestFutures
                                        .get(
                                                addContainerRequestFuturesNumCompleted
                                                        .getAndIncrement())
                                        .complete(null));
                resourceEventHandlerBuilder.setOnWorkerTerminatedConsumer(
                        (ignore1, ignore2) ->
                                getDriver().requestResource(testingTaskExecutorProcessSpec));

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getDriver()
                                                    .requestResource(
                                                            testingTaskExecutorProcessSpec));
                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    ImmutableList.of(testingContainer));
                            nodeManagerClientCallbackHandler.onStartContainerError(
                                    testingContainer.getId(), new Exception("start error"));

                            verifyFutureCompleted(releaseAssignedContainerFuture);
                            verifyFutureCompleted(addContainerRequestFutures.get(1));
                        });
            }
        };
    }

    @Test
    void testStartWorkerVariousSpec() throws Exception {
        final TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                new TaskExecutorProcessSpec(
                        new CPUResource(1),
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ofMebiBytes(50),
                        MemorySize.ofMebiBytes(50),
                        MemorySize.ofMebiBytes(50),
                        MemorySize.ofMebiBytes(50),
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        Collections.emptyList());
        final TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                new TaskExecutorProcessSpec(
                        new CPUResource(2),
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        MemorySize.ofMebiBytes(500),
                        MemorySize.ofMebiBytes(500),
                        MemorySize.ofMebiBytes(500),
                        MemorySize.ofMebiBytes(500),
                        MemorySize.ZERO,
                        MemorySize.ZERO,
                        Collections.emptyList());

        new Context() {
            {
                final String startCommand1 =
                        TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (50L << 20);
                final String startCommand2 =
                        TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
                final CompletableFuture<Void> startContainerAsyncCommandFuture1 =
                        new CompletableFuture<>();
                final CompletableFuture<Void> startContainerAsyncCommandFuture2 =
                        new CompletableFuture<>();
                prepareForTestStartTaskExecutorProcessVariousSpec(
                        startCommand1,
                        startCommand2,
                        startContainerAsyncCommandFuture1,
                        startContainerAsyncCommandFuture2,
                        taskExecutorProcessSpec1);

                testingYarnAMRMClientAsyncBuilder.setGetMatchingRequestsFunction(
                        tuple -> {
                            final Priority priority = tuple.f0;
                            final List<AMRMClient.ContainerRequest> matchingRequests =
                                    new ArrayList<>();
                            for (CompletableFuture<AMRMClient.ContainerRequest>
                                    addContainerRequestFuture : addContainerRequestFutures) {
                                final AMRMClient.ContainerRequest request =
                                        addContainerRequestFuture.getNow(null);
                                if (request != null && priority.equals(request.getPriority())) {
                                    assertThat(tuple.f2).isEqualTo(request.getCapability());
                                    matchingRequests.add(request);
                                }
                            }
                            return Collections.singletonList(matchingRequests);
                        });

                runTest(
                        () -> {
                            final Resource containerResource1 =
                                    ((YarnResourceManagerDriver) getDriver())
                                            .getContainerResource(taskExecutorProcessSpec1)
                                            .get();
                            final Resource containerResource2 =
                                    ((YarnResourceManagerDriver) getDriver())
                                            .getContainerResource(taskExecutorProcessSpec2)
                                            .get();
                            // Make sure two worker resource spec will be normalized to different
                            // container resources
                            assertThat(containerResource2).isNotEqualTo(containerResource1);

                            runInMainThread(
                                    () -> getDriver().requestResource(taskExecutorProcessSpec1));
                            runInMainThread(
                                    () -> getDriver().requestResource(taskExecutorProcessSpec2));

                            // Verify both containers requested
                            verifyFutureCompleted(addContainerRequestFutures.get(0));
                            verifyFutureCompleted(addContainerRequestFutures.get(1));

                            // Mock that container 1 is allocated
                            Container container1 =
                                    createTestingContainerWithResource(containerResource1);
                            resourceManagerClientCallbackHandler.onContainersAllocated(
                                    Collections.singletonList(container1));

                            // Verify that only worker with spec1 is started.
                            verifyFutureCompleted(startContainerAsyncCommandFuture1);
                            assertThat(startContainerAsyncCommandFuture2.isDone()).isFalse();

                            // Mock that container 1 is completed, while the worker is still pending
                            ContainerStatus testingContainerStatus =
                                    createTestingContainerCompletedStatus(container1.getId());
                            resourceManagerClientCallbackHandler.onContainersCompleted(
                                    Collections.singletonList(testingContainerStatus));

                            // Verify that only container 1 is requested again
                            verifyFutureCompleted(addContainerRequestFutures.get(2));
                            assertThat(addContainerRequestFutures.get(2).get().getCapability())
                                    .isEqualTo(containerResource1);
                            assertThat(addContainerRequestFutures.get(3).isDone()).isFalse();
                        });
            }
        };
    }

    private boolean containsStartCommand(
            ContainerLaunchContext containerLaunchContext, String command) {
        return containerLaunchContext.getCommands().stream().anyMatch(str -> str.contains(command));
    }

    private static Container createTestingContainerWithResource(
            Resource resource, Priority priority, int containerIdx) {
        final ContainerId containerId =
                ContainerId.newInstance(
                        ApplicationAttemptId.newInstance(
                                ApplicationId.newInstance(System.currentTimeMillis(), 1), 1),
                        containerIdx);
        final NodeId nodeId = NodeId.newInstance("container", 1234);
        return new TestingContainer(containerId, nodeId, resource, priority);
    }

    private class Context extends ResourceManagerDriverTestBase<YarnWorkerNode>.Context {
        private final CompletableFuture<Void> stopAndCleanupClusterFuture =
                new CompletableFuture<>();
        private final CompletableFuture<Resource> createTaskManagerContainerFuture =
                new CompletableFuture<>();
        final CompletableFuture<Void> stopContainerAsyncFuture = new CompletableFuture<>();
        final List<CompletableFuture<AMRMClient.ContainerRequest>> addContainerRequestFutures =
                new ArrayList<>();
        final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
        final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
        final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
        final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();
        final CompletableFuture<Void> resourceManagerClientInitFuture = new CompletableFuture<>();
        final CompletableFuture<Void> resourceManagerClientStartFuture = new CompletableFuture<>();
        final CompletableFuture<Void> resourceManagerClientStopFuture = new CompletableFuture<>();
        final CompletableFuture<Void> nodeManagerClientInitFuture = new CompletableFuture<>();
        final CompletableFuture<Void> nodeManagerClientStartFuture = new CompletableFuture<>();
        final CompletableFuture<Void> nodeManagerClientStopFuture = new CompletableFuture<>();

        AMRMClientAsync.CallbackHandler resourceManagerClientCallbackHandler;
        NMClientAsync.CallbackHandler nodeManagerClientCallbackHandler;
        TestingYarnNMClientAsync testingYarnNMClientAsync;
        TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;
        final TestingYarnNMClientAsync.Builder testingYarnNMClientAsyncBuilder =
                TestingYarnNMClientAsync.builder()
                        .setStartContainerAsyncConsumer(
                                (ignored1, ignored2, ignored3) ->
                                        startContainerAsyncFuture.complete(null))
                        .setStopContainerAsyncConsumer(
                                (ignored1, ignored2, ignored3) ->
                                        stopContainerAsyncFuture.complete(null))
                        .setClientInitRunnable(() -> nodeManagerClientInitFuture.complete(null))
                        .setClientStartRunnable(() -> nodeManagerClientStartFuture.complete(null))
                        .setClientStopRunnable(() -> nodeManagerClientStopFuture.complete(null));
        final TestingYarnAMRMClientAsync.Builder testingYarnAMRMClientAsyncBuilder =
                TestingYarnAMRMClientAsync.builder()
                        .setAddContainerRequestConsumer(
                                (request, handler) -> {
                                    createTaskManagerContainerFuture.complete(
                                            request.getCapability());
                                    resourceManagerClientCallbackHandler.onContainersAllocated(
                                            Collections.singletonList(testingContainer));
                                })
                        .setGetMatchingRequestsFunction(
                                ignored ->
                                        Collections.singletonList(
                                                Collections.singletonList(
                                                        ContainerRequestReflector.INSTANCE
                                                                .getContainerRequest(
                                                                        testingResource,
                                                                        Priority.UNDEFINED,
                                                                        null))))
                        .setRemoveContainerRequestConsumer(
                                (request, handler) -> removeContainerRequestFuture.complete(null))
                        .setReleaseAssignedContainerConsumer(
                                (ignored1, ignored2) ->
                                        releaseAssignedContainerFuture.complete(null))
                        .setUnregisterApplicationMasterConsumer(
                                (ignore1, ignore2, ignore3) ->
                                        stopAndCleanupClusterFuture.complete(null))
                        .setClientInitRunnable(() -> resourceManagerClientInitFuture.complete(null))
                        .setClientStartRunnable(
                                () -> resourceManagerClientStartFuture.complete(null))
                        .setClientStopRunnable(
                                () -> resourceManagerClientStopFuture.complete(null));
        final TestingYarnResourceManagerClientFactory testingYarnResourceManagerClientFactory =
                new TestingYarnResourceManagerClientFactory(
                        ((integer, handler) -> {
                            resourceManagerClientCallbackHandler = handler;
                            testingYarnAMRMClientAsync =
                                    testingYarnAMRMClientAsyncBuilder.build(handler);
                            return testingYarnAMRMClientAsync;
                        }));

        private TestingYarnNodeManagerClientFactory testingYarnNodeManagerClientFactory =
                new TestingYarnNodeManagerClientFactory(
                        (handler -> {
                            nodeManagerClientCallbackHandler = handler;
                            testingYarnNMClientAsync =
                                    testingYarnNMClientAsyncBuilder.build(handler);
                            return testingYarnNMClientAsync;
                        }));

        final Map<String, String> env = new HashMap<>();

        private int containerIdx = 0;

        void resetYarnNodeManagerClientFactory() {
            testingYarnNodeManagerClientFactory =
                    new TestingYarnNodeManagerClientFactory(
                            (handler -> {
                                nodeManagerClientCallbackHandler = handler;
                                testingYarnNMClientAsync =
                                        testingYarnNMClientAsyncBuilder.build(handler);
                                return testingYarnNMClientAsync;
                            }));
        }

        @Override
        protected void prepareRunTest() throws Exception {
            File home = Files.createTempDirectory(tmpFolder, "home").toFile();

            env.put(ENV_APP_ID, "foo");
            env.put(ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
            env.put(ENV_CLIENT_SHIP_FILES, "");
            env.put(ENV_FLINK_CLASSPATH, "");
            env.put(ENV_HADOOP_USER_NAME, "foo");
            env.putIfAbsent(FLINK_YARN_FILES, "");
            env.put(
                    FLINK_DIST_JAR,
                    new YarnLocalResourceDescriptor(
                                    "flink.jar",
                                    new Path("/tmp/flink.jar"),
                                    0,
                                    System.currentTimeMillis(),
                                    LocalResourceVisibility.APPLICATION,
                                    LocalResourceType.FILE)
                            .toString());
            env.put(ApplicationConstants.Environment.PWD.key(), home.getAbsolutePath());

            BootstrapTools.writeConfiguration(
                    flinkConfig, new File(home.getAbsolutePath(), FLINK_CONF_FILENAME));
        }

        @Override
        protected void preparePreviousAttemptWorkers() {
            testingYarnAMRMClientAsyncBuilder.setRegisterApplicationMasterFunction(
                    (ignored1, ignored2, ignored3) ->
                            new TestingRegisterApplicationMasterResponse(
                                    () -> Collections.singletonList(testingContainer)));
        }

        @Override
        protected ResourceManagerDriver<YarnWorkerNode> createResourceManagerDriver() {
            return new YarnResourceManagerDriver(
                    flinkConfig,
                    new YarnResourceManagerDriverConfiguration(env, "localhost:9000", null),
                    testingYarnResourceManagerClientFactory,
                    testingYarnNodeManagerClientFactory);
        }

        @Override
        protected void validateInitialization() throws Exception {
            assertThat(testingYarnAMRMClientAsync).isNotNull();
            assertThat(testingYarnNMClientAsync).isNotNull();
            verifyFutureCompleted(nodeManagerClientInitFuture);
            verifyFutureCompleted(nodeManagerClientStartFuture);
            verifyFutureCompleted(resourceManagerClientInitFuture);
            verifyFutureCompleted(resourceManagerClientStartFuture);
        }

        @Override
        protected void validateWorkersRecoveredFromPreviousAttempt(
                Collection<YarnWorkerNode> workers) {
            assumeThat(HadoopUtils.isMinHadoopVersion(2, 2)).isTrue();
            assertThat(workers).hasSize(1);

            final ResourceID resourceId = workers.iterator().next().getResourceID();
            assertThat(resourceId).hasToString(testingContainer.getId().toString());
        }

        @Override
        protected void validateTermination() throws Exception {
            verifyFutureCompleted(nodeManagerClientStopFuture);
            verifyFutureCompleted(resourceManagerClientStopFuture);
        }

        @Override
        protected void validateDeregisterApplication() throws Exception {
            verifyFutureCompleted(stopAndCleanupClusterFuture);
        }

        @Override
        protected void validateRequestedResources(
                Collection<TaskExecutorProcessSpec> taskExecutorProcessSpecs) throws Exception {
            assertThat(taskExecutorProcessSpecs).hasSize(1);
            final TaskExecutorProcessSpec taskExecutorProcessSpec =
                    taskExecutorProcessSpecs.iterator().next();

            final Resource resource =
                    createTaskManagerContainerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

            assertThat(resource.getMemorySize())
                    .isEqualTo(taskExecutorProcessSpec.getTotalProcessMemorySize().getMebiBytes());
            assertThat(resource.getVirtualCores())
                    .isEqualTo(taskExecutorProcessSpec.getCpuCores().getValue().intValue());
            verifyFutureCompleted(removeContainerRequestFuture);
        }

        @Override
        protected void validateReleaseResources(Collection<YarnWorkerNode> workerNodes)
                throws Exception {
            assertThat(workerNodes).hasSize(1);
            verifyFutureCompleted(stopContainerAsyncFuture);
            verifyFutureCompleted(releaseAssignedContainerFuture);
        }

        ContainerStatus createTestingContainerCompletedStatus(final ContainerId containerId) {
            return new TestingContainerStatus(
                    containerId, ContainerState.COMPLETE, "Test exit", -1);
        }

        Container createTestingContainerWithResource(Resource resource) {
            return YarnResourceManagerDriverTest.createTestingContainerWithResource(
                    resource, testingPriority, containerIdx++);
        }

        <T> void verifyFutureCompleted(CompletableFuture<T> future) throws Exception {
            future.get(TIMEOUT_SEC, TimeUnit.SECONDS);
        }

        void prepareForTestStartTaskExecutorProcessVariousSpec(
                String startCommand1,
                String startCommand2,
                CompletableFuture<Void> startContainerAsyncCommandFuture1,
                CompletableFuture<Void> startContainerAsyncCommandFuture2,
                TaskExecutorProcessSpec taskExecutorProcessSpec) {
            addContainerRequestFutures.add(new CompletableFuture<>());
            addContainerRequestFutures.add(new CompletableFuture<>());
            addContainerRequestFutures.add(new CompletableFuture<>());
            addContainerRequestFutures.add(new CompletableFuture<>());

            testingYarnAMRMClientAsyncBuilder.setAddContainerRequestConsumer(
                    (request, ignored) ->
                            addContainerRequestFutures
                                    .get(addContainerRequestFuturesNumCompleted.getAndIncrement())
                                    .complete(request));
            testingYarnNMClientAsyncBuilder.setStartContainerAsyncConsumer(
                    (ignored1, context, ignored3) -> {
                        if (containsStartCommand(context, startCommand1)) {
                            startContainerAsyncCommandFuture1.complete(null);
                        } else if (containsStartCommand(context, startCommand2)) {
                            startContainerAsyncCommandFuture2.complete(null);
                        }
                    });
            resourceEventHandlerBuilder.setOnWorkerTerminatedConsumer(
                    (ignore1, ignore2) -> getDriver().requestResource(taskExecutorProcessSpec));
        }
    }

    @Test
    void testGetContainerCompletedCauseForSuccess() {
        ContainerStatus containerStatus = createSuccessfulCompletedContainerStatus();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format("Container %s exited normally.", containerStatus.getContainerId()));
    }

    private ContainerStatus createSuccessfulCompletedContainerStatus() {
        return new TestingContainerStatus(
                testingContainer.getId(),
                ContainerState.COMPLETE,
                "success exit code",
                ContainerExitStatus.SUCCESS);
    }

    @Test
    void testGetContainerCompletedCauseForPreempted() {
        ContainerStatus containerStatus = createCompletedContainerStatusBecauseItWasPreempted();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format(
                        "Container %s was preempted by yarn.", containerStatus.getContainerId()));
    }

    private ContainerStatus createCompletedContainerStatusBecauseItWasPreempted() {
        return new TestingContainerStatus(
                testingContainer.getId(),
                ContainerState.COMPLETE,
                "preempted exit code",
                ContainerExitStatus.PREEMPTED);
    }

    @Test
    void testGetContainerCompletedCauseForInvalid() {
        ContainerStatus containerStatus = createCompletedContainerStatusBecauseItWasInvalid();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format("Container %s was invalid.", containerStatus.getContainerId()));
    }

    private ContainerStatus createCompletedContainerStatusBecauseItWasInvalid() {
        return new TestingContainerStatus(
                testingContainer.getId(),
                ContainerState.COMPLETE,
                "invalid exit code",
                ContainerExitStatus.INVALID);
    }

    @Test
    void testGetContainerCompletedCauseForAborted() {
        ContainerStatus containerStatus = createCompletedContainerStatusBecauseItWasAborted();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format(
                        "Container %s killed by YARN, either due to being released by the application or being 'lost' due to node failures etc.",
                        containerStatus.getContainerId()));
    }

    private ContainerStatus createCompletedContainerStatusBecauseItWasAborted() {
        return new TestingContainerStatus(
                testingContainer.getId(),
                ContainerState.COMPLETE,
                "aborted exit code",
                ContainerExitStatus.ABORTED);
    }

    @Test
    void testGetContainerCompletedCauseForDiskFailed() {
        ContainerStatus containerStatus = createCompletedContainerStatusBecauseDisksFailed();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format(
                        "Container %s is failed because threshold number of the nodemanager-local-directories or"
                                + " threshold number of the nodemanager-log-directories have become bad.",
                        containerStatus.getContainerId()));
    }

    private ContainerStatus createCompletedContainerStatusBecauseDisksFailed() {
        return new TestingContainerStatus(
                testingContainer.getId(),
                ContainerState.COMPLETE,
                "disk failed exit code",
                ContainerExitStatus.DISKS_FAILED);
    }

    @Test
    void testGetContainerCompletedCauseForUnknown() {
        ContainerStatus containerStatus = createCompletedContainerStatusForUnknownCause();
        testingGetContainerCompletedCause(
                containerStatus,
                String.format(
                        "Container %s marked as failed.\n Exit code:%s.",
                        containerStatus.getContainerId(), containerStatus.getExitStatus()));
    }

    private ContainerStatus createCompletedContainerStatusForUnknownCause() {
        return new TestingContainerStatus(
                testingContainer.getId(), ContainerState.COMPLETE, "unknown exit code", -1);
    }

    void testingGetContainerCompletedCause(
            ContainerStatus containerStatus, String expectedCompletedCause) {
        final String containerCompletedCause =
                YarnResourceManagerDriver.getContainerCompletedCause(containerStatus);
        assertThat(containerCompletedCause)
                .contains(expectedCompletedCause, containerStatus.getDiagnostics());
    }
}
