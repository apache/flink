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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.blocklist.NoOpBlocklistHandler;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceDeclaration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/** Tests for {@link ActiveResourceManager}. */
public class ActiveResourceManagerTest extends TestLogger {

    @ClassRule
    public static final TestingRpcServiceResource RPC_SERVICE_RESOURCE =
            new TestingRpcServiceResource();

    private static final long TIMEOUT_SEC = 5L;
    private static final Time TIMEOUT_TIME = Time.seconds(TIMEOUT_SEC);
    private static final Time TESTING_START_WORKER_INTERVAL = Time.milliseconds(50);
    private static final long TESTING_START_WORKER_TIMEOUT_MS = 50;

    private static final WorkerResourceSpec WORKER_RESOURCE_SPEC = WorkerResourceSpec.ZERO;
    private static final TaskExecutorMemoryConfiguration TESTING_CONFIG =
            new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 21L, 36L);

    /** Tests worker successfully requested, started and registered. */
    @Test
    public void testStartNewWorker() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture =
                        new CompletableFuture<>();

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(tmResourceId);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .requestNewWorker(
                                                                    WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec =
                                    requestWorkerFromDriverFuture.get(
                                            TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    taskExecutorProcessSpec,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // worker registered, verify registration succeeded
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceId);
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests request new workers when resources less than declared. */
    @Test
    public void testLessThanDeclareResource() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);
                final List<CompletableFuture<ResourceID>> resourceIdFutures = new ArrayList<>();
                resourceIdFutures.add(CompletableFuture.completedFuture(ResourceID.generate()));
                resourceIdFutures.add(new CompletableFuture<>());
                resourceIdFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec ->
                                resourceIdFutures.get(requestCount.getAndIncrement()));

                runTest(
                        () -> {
                            // request two new worker
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .requestNewWorker(WORKER_RESOURCE_SPEC))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .requestNewWorker(WORKER_RESOURCE_SPEC))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(requestCount.get(), is(2));

                            // release registered worker.
                            CompletableFuture<Void> declareResourceFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    3,
                                                                                    Collections
                                                                                            .emptySet()))));

                            declareResourceFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // request new worker.
                            assertThat(requestCount.get(), is(3));
                        });
            }
        };
    }

    /** Test release workers if more than resources declared. */
    @Test
    public void testMoreThanDeclaredResource() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);
                final List<CompletableFuture<ResourceID>> resourceIdFutures =
                        Arrays.asList(
                                CompletableFuture.completedFuture(ResourceID.generate()),
                                CompletableFuture.completedFuture(ResourceID.generate()),
                                CompletableFuture.completedFuture(ResourceID.generate()),
                                new CompletableFuture<>());

                final AtomicInteger releaseCount = new AtomicInteger(0);
                final List<CompletableFuture<ResourceID>> releaseResourceFutures =
                        Arrays.asList(
                                new CompletableFuture<>(),
                                new CompletableFuture<>(),
                                new CompletableFuture<>());

                driverBuilder
                        .setRequestResourceFunction(
                                taskExecutorProcessSpec ->
                                        resourceIdFutures.get(requestCount.getAndIncrement()))
                        .setReleaseResourceConsumer(
                                resourceID ->
                                        releaseResourceFutures
                                                .get(releaseCount.getAndIncrement())
                                                .complete(resourceID));

                runTest(
                        () -> {
                            runInMainThread(
                                    () -> {
                                        for (int i = 0; i < 4; i++) {
                                            getResourceManager()
                                                    .requestNewWorker(WORKER_RESOURCE_SPEC);
                                        }
                                    });

                            ResourceID unWantedResource = resourceIdFutures.get(0).get();
                            ResourceID normalResource = resourceIdFutures.get(1).get();
                            ResourceID startingResource = resourceIdFutures.get(2).get();
                            CompletableFuture<ResourceID> pendingRequestFuture =
                                    resourceIdFutures.get(3);

                            registerTaskExecutorAndSendSlotReport(unWantedResource, 1)
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            registerTaskExecutorAndSendSlotReport(normalResource, 1)
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(requestCount.get(), is(4));
                            assertThat(releaseCount.get(), is(0));

                            Set<InstanceID> unWantedWorkers =
                                    Collections.singleton(
                                            getResourceManager()
                                                    .getInstanceIdByResourceId(unWantedResource)
                                                    .get());

                            // release unwanted workers.
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    3,
                                                                                    unWantedWorkers))))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(releaseCount.get(), is(1));
                            assertThat(releaseResourceFutures.get(0).get(), is(unWantedResource));

                            // release pending workers.
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    2,
                                                                                    Collections
                                                                                            .emptySet()))))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(releaseCount.get(), is(1));
                            assertThat(pendingRequestFuture.isCancelled(), is(true));

                            // release starting workers.
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(releaseCount.get(), is(2));
                            assertThat(releaseResourceFutures.get(1).get(), is(startingResource));

                            // release last workers.
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    0,
                                                                                    Collections
                                                                                            .emptySet()))))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(releaseCount.get(), is(3));
                            assertThat(releaseResourceFutures.get(2).get(), is(normalResource));
                        });
            }
        };
    }

    /** Tests worker failed while requesting. */
    @Test
    public void testStartNewWorkerFailedRequesting() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<CompletableFuture<ResourceID>> resourceIdFutures = new ArrayList<>();
                resourceIdFutures.add(new CompletableFuture<>());
                resourceIdFutures.add(new CompletableFuture<>());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return resourceIdFutures.get(idx);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first request failed, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            resourceIdFutures
                                                    .get(0)
                                                    .completeExceptionally(
                                                            new Throwable("testing error")));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second request allocated, verify registration succeed
                            runInMainThread(() -> resourceIdFutures.get(1).complete(tmResourceId));
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceId);
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated after requested before registered. */
    @Test
    public void testWorkerTerminatedBeforeRegister() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<ResourceID> tmResourceIds = new ArrayList<>();
                tmResourceIds.add(ResourceID.generate());
                tmResourceIds.add(ResourceID.generate());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(tmResourceIds.get(idx));
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first worker failed before register, verify requesting another worker
                            // from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            tmResourceIds.get(0),
                                                            "terminate for testing"));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceIds.get(1));
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated after registered. */
    @Test
    public void testWorkerTerminatedAfterRegister() throws Exception {
        new Context() {
            {
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<ResourceID> tmResourceIds = new ArrayList<>();
                tmResourceIds.add(ResourceID.generate());
                tmResourceIds.add(ResourceID.generate());

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(tmResourceIds.get(idx));
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))));
                            TaskExecutorProcessSpec taskExecutorProcessSpec1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    taskExecutorProcessSpec1,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // first worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture1 =
                                    registerTaskExecutor(tmResourceIds.get(0));
                            assertThat(
                                    registerTaskExecutorFuture1.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));

                            // first worker terminated, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            tmResourceIds.get(0),
                                                            "terminate for testing"));
                            TaskExecutorProcessSpec taskExecutorProcessSpec2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            assertThat(taskExecutorProcessSpec2, is(taskExecutorProcessSpec1));

                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture2 =
                                    registerTaskExecutor(tmResourceIds.get(1));
                            assertThat(
                                    registerTaskExecutorFuture2.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests worker terminated and is no longer required. */
    @Test
    public void testWorkerTerminatedNoLongerRequired() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<CompletableFuture<TaskExecutorProcessSpec>>
                        requestWorkerFromDriverFutures = new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(tmResourceId);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .requestNewWorker(
                                                                    WORKER_RESOURCE_SPEC));
                            TaskExecutorProcessSpec taskExecutorProcessSpec =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(
                                    taskExecutorProcessSpec,
                                    is(
                                            TaskExecutorProcessUtils
                                                    .processSpecFromWorkerResourceSpec(
                                                            flinkConfig, WORKER_RESOURCE_SPEC)));

                            // worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceId);
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));

                            // worker terminated, verify not requesting new worker
                            runInMainThread(
                                            () -> {
                                                getResourceManager()
                                                        .onWorkerTerminated(
                                                                tmResourceId,
                                                                "terminate for testing");
                                                // needs to return something, so that we can use
                                                // `get()` to make sure the main thread processing
                                                // finishes before the assertions
                                                return null;
                                            })
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertFalse(requestWorkerFromDriverFutures.get(1).isDone());
                        });
            }
        };
    }

    @Test
    public void testCloseTaskManagerConnectionOnWorkerTerminated() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final CompletableFuture<TaskExecutorProcessSpec> requestWorkerFromDriverFuture =
                        new CompletableFuture<>();
                final CompletableFuture<Void> disconnectResourceManagerFuture =
                        new CompletableFuture<>();

                final TestingTaskExecutorGateway taskExecutorGateway =
                        new TestingTaskExecutorGatewayBuilder()
                                .setDisconnectResourceManagerConsumer(
                                        (ignore) -> disconnectResourceManagerFuture.complete(null))
                                .createTestingTaskExecutorGateway();

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            requestWorkerFromDriverFuture.complete(taskExecutorProcessSpec);
                            return CompletableFuture.completedFuture(tmResourceId);
                        });

                runTest(
                        () -> {
                            // request a new worker, terminate it after registered
                            runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .requestNewWorker(WORKER_RESOURCE_SPEC))
                                    .thenCompose(
                                            (ignore) ->
                                                    registerTaskExecutor(
                                                            tmResourceId, taskExecutorGateway))
                                    .thenRun(
                                            () ->
                                                    runInMainThread(
                                                            () ->
                                                                    getResourceManager()
                                                                            .onWorkerTerminated(
                                                                                    tmResourceId,
                                                                                    "terminate for testing")));
                            // verify task manager connection is closed
                            disconnectResourceManagerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    @Test
    public void testStartWorkerIntervalOnWorkerTerminationExceedFailureRate() throws Exception {
        new Context() {
            {
                flinkConfig.setDouble(ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE, 1);
                flinkConfig.set(
                        ResourceManagerOptions.START_WORKER_RETRY_INTERVAL,
                        Duration.ofMillis(TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                final AtomicInteger requestCount = new AtomicInteger(0);

                final List<ResourceID> tmResourceIds = new ArrayList<>();
                tmResourceIds.add(ResourceID.generate());
                tmResourceIds.add(ResourceID.generate());

                final List<CompletableFuture<Long>> requestWorkerFromDriverFutures =
                        new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(System.currentTimeMillis());
                            return CompletableFuture.completedFuture(tmResourceIds.get(idx));
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))));
                            long t1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // first worker failed before register, verify requesting another worker
                            // from driver
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerTerminated(
                                                            tmResourceIds.get(0),
                                                            "terminate for testing"));
                            long t2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // validate trying creating worker twice, with proper interval
                            assertThat(
                                    (t2 - t1),
                                    greaterThanOrEqualTo(
                                            TESTING_START_WORKER_INTERVAL.toMilliseconds()));
                            // second worker registered, verify registration succeed
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceIds.get(1));
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    @Test
    public void testStartWorkerIntervalOnRequestWorkerFailure() throws Exception {
        new Context() {
            {
                flinkConfig.setDouble(ResourceManagerOptions.START_WORKER_MAX_FAILURE_RATE, 1);
                flinkConfig.set(
                        ResourceManagerOptions.START_WORKER_RETRY_INTERVAL,
                        Duration.ofMillis(TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                final AtomicInteger requestCount = new AtomicInteger(0);
                final ResourceID tmResourceId = ResourceID.generate();

                final List<CompletableFuture<ResourceID>> resourceIdFutures = new ArrayList<>();
                resourceIdFutures.add(new CompletableFuture<>());
                resourceIdFutures.add(new CompletableFuture<>());

                final List<CompletableFuture<Long>> requestWorkerFromDriverFutures =
                        new ArrayList<>();
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());
                requestWorkerFromDriverFutures.add(new CompletableFuture<>());

                driverBuilder.setRequestResourceFunction(
                        taskExecutorProcessSpec -> {
                            int idx = requestCount.getAndIncrement();
                            assertThat(idx, lessThan(2));

                            requestWorkerFromDriverFutures
                                    .get(idx)
                                    .complete(System.currentTimeMillis());
                            return resourceIdFutures.get(idx);
                        });

                runTest(
                        () -> {
                            // received worker request, verify requesting from driver
                            CompletableFuture<Void> startNewWorkerFuture =
                                    runInMainThread(
                                            () ->
                                                    getResourceManager()
                                                            .declareResourceNeeded(
                                                                    Collections.singleton(
                                                                            new ResourceDeclaration(
                                                                                    WORKER_RESOURCE_SPEC,
                                                                                    1,
                                                                                    Collections
                                                                                            .emptySet()))));

                            startNewWorkerFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            long t1 =
                                    requestWorkerFromDriverFutures
                                            .get(0)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            // first request failed, verify requesting another worker from driver
                            runInMainThread(
                                    () ->
                                            resourceIdFutures
                                                    .get(0)
                                                    .completeExceptionally(
                                                            new Throwable("testing error")));
                            long t2 =
                                    requestWorkerFromDriverFutures
                                            .get(1)
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);

                            // validate trying creating worker twice, with proper interval
                            assertThat(
                                    (t2 - t1),
                                    greaterThanOrEqualTo(
                                            TESTING_START_WORKER_INTERVAL.toMilliseconds()));

                            // second worker registered, verify registration succeed
                            resourceIdFutures.get(1).complete(tmResourceId);
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceId);
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests workers from previous attempt successfully recovered and registered. */
    @Test
    public void testRecoverWorkerFromPreviousAttempt() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();

                runTest(
                        () -> {
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onPreviousAttemptWorkersRecovered(
                                                            Collections.singleton(tmResourceId)));
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(tmResourceId);
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Success.class));
                        });
            }
        };
    }

    /** Tests decline unknown worker registration. */
    @Test
    public void testRegisterUnknownWorker() throws Exception {
        new Context() {
            {
                runTest(
                        () -> {
                            CompletableFuture<RegistrationResponse> registerTaskExecutorFuture =
                                    registerTaskExecutor(ResourceID.generate());
                            assertThat(
                                    registerTaskExecutorFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    instanceOf(RegistrationResponse.Rejection.class));
                        });
            }
        };
    }

    @Test
    public void testOnError() throws Exception {
        new Context() {
            {
                final Throwable fatalError = new Throwable("Testing fatal error");
                runTest(
                        () -> {
                            runInMainThread(() -> getResourceManager().onError(fatalError));
                            final Throwable reportedError =
                                    getFatalErrorHandler()
                                            .getErrorFuture()
                                            .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                            assertThat(reportedError, is(fatalError));
                        });
            }
        };
    }

    @Test
    public void testWorkerRegistrationTimeout() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final CompletableFuture<ResourceID> releaseResourceFuture =
                        new CompletableFuture<>();

                flinkConfig.set(
                        ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT,
                        Duration.ofMillis(TESTING_START_WORKER_TIMEOUT_MS));

                driverBuilder
                        .setRequestResourceFunction(
                                taskExecutorProcessSpec ->
                                        CompletableFuture.completedFuture(tmResourceId))
                        .setReleaseResourceConsumer(releaseResourceFuture::complete);

                runTest(
                        () -> {
                            // request new worker
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .requestNewWorker(WORKER_RESOURCE_SPEC));

                            // verify worker is released due to not registered in time
                            assertThat(
                                    releaseResourceFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(tmResourceId));
                        });
            }
        };
    }

    @Test
    public void testWorkerRegistrationTimeoutNotCountingAllocationTime() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final CompletableFuture<ResourceID> requestResourceFuture =
                        new CompletableFuture<>();
                final CompletableFuture<ResourceID> releaseResourceFuture =
                        new CompletableFuture<>();

                flinkConfig.set(
                        ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT,
                        Duration.ofMillis(TESTING_START_WORKER_TIMEOUT_MS));

                driverBuilder
                        .setRequestResourceFunction(
                                taskExecutorProcessSpec -> requestResourceFuture)
                        .setReleaseResourceConsumer(releaseResourceFuture::complete);

                runTest(
                        () -> {
                            // request new worker
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .requestNewWorker(WORKER_RESOURCE_SPEC));

                            // resource allocation takes longer than worker registration timeout
                            try {
                                Thread.sleep(TESTING_START_WORKER_TIMEOUT_MS * 2);
                            } catch (InterruptedException e) {
                                fail();
                            }

                            final long start = System.nanoTime();

                            runInMainThread(() -> requestResourceFuture.complete(tmResourceId));

                            // worker registered, verify not released due to timeout
                            RegistrationResponse registrationResponse =
                                    registerTaskExecutor(tmResourceId).join();

                            final long registrationTime = (System.nanoTime() - start) / 1_000_000;

                            assumeTrue(
                                    "The registration must not take longer than the start worker timeout. If it does, then this indicates a very slow machine.",
                                    registrationTime < TESTING_START_WORKER_TIMEOUT_MS);
                            assertThat(
                                    registrationResponse,
                                    instanceOf(RegistrationResponse.Success.class));
                            assertFalse(releaseResourceFuture.isDone());
                        });
            }
        };
    }

    @Test
    public void testWorkerRegistrationTimeoutRecoveredFromPreviousAttempt() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId = ResourceID.generate();
                final CompletableFuture<ResourceID> releaseResourceFuture =
                        new CompletableFuture<>();

                flinkConfig.set(
                        ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT,
                        Duration.ofMillis(TESTING_START_WORKER_TIMEOUT_MS));

                driverBuilder.setReleaseResourceConsumer(releaseResourceFuture::complete);

                runTest(
                        () -> {
                            // workers recovered
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onPreviousAttemptWorkersRecovered(
                                                            Collections.singleton(tmResourceId)));

                            // verify worker is released due to not registered in time
                            assertThat(
                                    releaseResourceFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS),
                                    is(tmResourceId));
                        });
            }
        };
    }

    @Test
    public void testResourceManagerRecoveredAfterAllTMRegistered() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId1 = ResourceID.generate();
                final ResourceID tmResourceId2 = ResourceID.generate();

                runTest(
                        () -> {
                            // workers recovered
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onPreviousAttemptWorkersRecovered(
                                                            ImmutableSet.of(
                                                                    tmResourceId1, tmResourceId2)));

                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerRegistered(
                                                            tmResourceId1,
                                                            WorkerResourceSpec.ZERO));
                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerRegistered(
                                                            tmResourceId2,
                                                            WorkerResourceSpec.ZERO));
                            runInMainThread(
                                            () ->
                                                    assertTrue(
                                                            getResourceManager()
                                                                    .getReadyToServeFuture()
                                                                    .isDone()))
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    @Test
    public void testResourceManagerRecoveredAfterReconcileTimeout() throws Exception {
        new Context() {
            {
                final ResourceID tmResourceId1 = ResourceID.generate();
                final ResourceID tmResourceId2 = ResourceID.generate();

                flinkConfig.set(
                        ResourceManagerOptions.RESOURCE_MANAGER_PREVIOUS_WORKER_RECOVERY_TIMEOUT,
                        Duration.ofMillis(TESTING_START_WORKER_TIMEOUT_MS));

                runTest(
                        () -> {
                            // workers recovered
                            runInMainThread(
                                    () -> {
                                        getResourceManager()
                                                .onPreviousAttemptWorkersRecovered(
                                                        ImmutableSet.of(
                                                                tmResourceId1, tmResourceId2));
                                    });

                            runInMainThread(
                                    () ->
                                            getResourceManager()
                                                    .onWorkerRegistered(
                                                            tmResourceId1,
                                                            WorkerResourceSpec.ZERO));
                            getResourceManager()
                                    .getReadyToServeFuture()
                                    .get(TIMEOUT_SEC, TimeUnit.SECONDS);
                        });
            }
        };
    }

    private static class Context {

        final Configuration flinkConfig = new Configuration();
        final TestingResourceManagerDriver.Builder driverBuilder =
                new TestingResourceManagerDriver.Builder();
        final TestingSlotManagerBuilder slotManagerBuilder = new TestingSlotManagerBuilder();

        private ActiveResourceManager<ResourceID> resourceManager;
        private TestingFatalErrorHandler fatalErrorHandler;

        ActiveResourceManager<ResourceID> getResourceManager() {
            return resourceManager;
        }

        TestingFatalErrorHandler getFatalErrorHandler() {
            return fatalErrorHandler;
        }

        void runTest(RunnableWithException testMethod) throws Exception {
            fatalErrorHandler = new TestingFatalErrorHandler();
            resourceManager =
                    createAndStartResourceManager(
                            flinkConfig,
                            driverBuilder.build(),
                            slotManagerBuilder.createSlotManager());

            try {
                testMethod.run();
            } finally {
                resourceManager.close();
            }
        }

        private ActiveResourceManager<ResourceID> createAndStartResourceManager(
                Configuration configuration,
                ResourceManagerDriver<ResourceID> driver,
                SlotManager slotManager)
                throws Exception {
            final TestingRpcService rpcService = RPC_SERVICE_RESOURCE.getTestingRpcService();
            final MockResourceManagerRuntimeServices rmServices =
                    new MockResourceManagerRuntimeServices(rpcService, slotManager);
            final Duration retryInterval =
                    configuration.get(ResourceManagerOptions.START_WORKER_RETRY_INTERVAL);
            final Duration workerRegistrationTimeout =
                    configuration.get(ResourceManagerOptions.TASK_MANAGER_REGISTRATION_TIMEOUT);
            final Duration previousWorkerRecoverTimeout =
                    configuration.get(
                            ResourceManagerOptions
                                    .RESOURCE_MANAGER_PREVIOUS_WORKER_RECOVERY_TIMEOUT);

            final ActiveResourceManager<ResourceID> activeResourceManager =
                    new ActiveResourceManager<>(
                            driver,
                            configuration,
                            rpcService,
                            UUID.randomUUID(),
                            ResourceID.generate(),
                            rmServices.heartbeatServices,
                            rmServices.delegationTokenManager,
                            rmServices.slotManager,
                            NoOpResourceManagerPartitionTracker::get,
                            new NoOpBlocklistHandler.Factory(),
                            rmServices.jobLeaderIdService,
                            new ClusterInformation("localhost", 1234),
                            fatalErrorHandler,
                            UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                            ActiveResourceManagerFactory.createStartWorkerFailureRater(
                                    configuration),
                            retryInterval,
                            workerRegistrationTimeout,
                            previousWorkerRecoverTimeout,
                            ForkJoinPool.commonPool());

            activeResourceManager.start();
            activeResourceManager
                    .getStartedFuture()
                    .get(TIMEOUT_TIME.getSize(), TIMEOUT_TIME.getUnit());

            return activeResourceManager;
        }

        CompletableFuture<Void> runInMainThread(Runnable runnable) {
            return resourceManager.runInMainThread(
                    () -> {
                        runnable.run();
                        return null;
                    },
                    TIMEOUT_TIME);
        }

        <T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
            return resourceManager.runInMainThread(callable, TIMEOUT_TIME);
        }

        CompletableFuture<Acknowledge> registerTaskExecutorAndSendSlotReport(
                ResourceID resourceID, int slotNumber) {
            return registerTaskExecutor(resourceID)
                    .thenCompose(
                            response -> {
                                assertThat(
                                        response, instanceOf(RegistrationResponse.Success.class));

                                InstanceID instanceID =
                                        resourceManager.getInstanceIdByResourceId(resourceID).get();
                                Set<SlotStatus> slots = new HashSet<>();
                                for (int i = 0; i < slotNumber; i++) {
                                    slots.add(
                                            new SlotStatus(
                                                    new SlotID(resourceID, i),
                                                    ResourceProfile.ANY));
                                }
                                SlotReport slotReport = new SlotReport(slots);
                                return resourceManager
                                        .getSelfGateway(ResourceManagerGateway.class)
                                        .sendSlotReport(
                                                resourceID, instanceID, slotReport, TIMEOUT_TIME);
                            });
        }

        CompletableFuture<RegistrationResponse> registerTaskExecutor(ResourceID resourceID) {
            final TaskExecutorGateway taskExecutorGateway =
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();
            return registerTaskExecutor(resourceID, taskExecutorGateway);
        }

        CompletableFuture<RegistrationResponse> registerTaskExecutor(
                ResourceID resourceID, TaskExecutorGateway taskExecutorGateway) {
            RPC_SERVICE_RESOURCE
                    .getTestingRpcService()
                    .registerGateway(resourceID.toString(), taskExecutorGateway);

            final TaskExecutorRegistration taskExecutorRegistration =
                    new TaskExecutorRegistration(
                            resourceID.toString(),
                            resourceID,
                            1234,
                            23456,
                            new HardwareDescription(1, 2L, 3L, 4L),
                            TESTING_CONFIG,
                            ResourceProfile.ZERO,
                            ResourceProfile.ZERO,
                            resourceID.toString());

            return resourceManager
                    .getSelfGateway(ResourceManagerGateway.class)
                    .registerTaskExecutor(taskExecutorRegistration, TIMEOUT_TIME);
        }
    }
}
