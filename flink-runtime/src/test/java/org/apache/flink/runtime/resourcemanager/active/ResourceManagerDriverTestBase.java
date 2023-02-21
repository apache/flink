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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blocklist.BlockedNodeRetriever;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Common test cases for implementations of {@link ResourceManagerDriver}. */
@ExtendWith(TestLoggerExtension.class)
public abstract class ResourceManagerDriverTestBase<WorkerType extends ResourceIDRetrievable> {

    protected static final long TIMEOUT_SEC = 5L;
    protected static final long TIMEOUT_SHOULD_NOT_HAPPEN_MS = 10;

    protected static final TaskExecutorProcessSpec TASK_EXECUTOR_PROCESS_SPEC =
            TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                    new Configuration(), WorkerResourceSpec.ZERO);

    private static final String MAIN_THREAD_NAME = "testing-rpc-main-thread";
    private static final ScheduledExecutor MAIN_THREAD_EXECUTOR =
            new ScheduledExecutorServiceAdapter(
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> new Thread(runnable, MAIN_THREAD_NAME)));

    @Test
    void testInitialize() throws Exception {
        final Context context = createContext();
        context.runTest(context::validateInitialization);
    }

    @Test
    void testRecoverPreviousAttemptWorkers() throws Exception {
        final CompletableFuture<Collection<WorkerType>> recoveredWorkersFuture =
                new CompletableFuture<>();
        final Context context = createContext();
        context.resourceEventHandlerBuilder.setOnPreviousAttemptWorkersRecoveredConsumer(
                recoveredWorkersFuture::complete);
        context.preparePreviousAttemptWorkers();
        context.runTest(
                () ->
                        context.validateWorkersRecoveredFromPreviousAttempt(
                                recoveredWorkersFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS)));
    }

    @Test
    void testTerminate() throws Exception {
        final Context context = createContext();
        context.runTest(
                () -> {
                    context.getDriver().terminate();
                    context.validateTermination();
                });
    }

    @Test
    void testDeregisterApplicationSucceeded() throws Exception {
        testDeregisterApplication(ApplicationStatus.SUCCEEDED);
    }

    @Test
    void testDeregisterApplicationFailed() throws Exception {
        testDeregisterApplication(ApplicationStatus.FAILED);
    }

    @Test
    void testDeregisterApplicationCanceled() throws Exception {
        testDeregisterApplication(ApplicationStatus.CANCELED);
    }

    @Test
    void testDeregisterApplicationUnknown() throws Exception {
        testDeregisterApplication(ApplicationStatus.UNKNOWN);
    }

    private void testDeregisterApplication(ApplicationStatus status) throws Exception {
        final Context context = createContext();
        context.runTest(
                () -> {
                    context.getDriver().deregisterApplication(status, null);
                    context.validateDeregisterApplication();
                });
    }

    @Test
    void testRequestResource() throws Exception {
        final Context context = createContext();
        context.runTest(
                () -> {
                    context.runInMainThread(
                            () -> context.getDriver().requestResource(TASK_EXECUTOR_PROCESS_SPEC));
                    context.validateRequestedResources(
                            Collections.singleton(TASK_EXECUTOR_PROCESS_SPEC));
                });
    }

    @Test
    void testReleaseResource() throws Exception {
        final CompletableFuture<WorkerType> requestResourceFuture = new CompletableFuture<>();
        final CompletableFuture<WorkerType> releaseResourceFuture = new CompletableFuture<>();
        final Context context = createContext();
        context.runTest(
                () -> {
                    context.runInMainThread(
                            () ->
                                    context.getDriver()
                                            .requestResource(TASK_EXECUTOR_PROCESS_SPEC)
                                            .thenAccept(requestResourceFuture::complete));
                    requestResourceFuture.thenApply(
                            (workerNode) ->
                                    context.runInMainThread(
                                            () -> {
                                                context.getDriver().releaseResource(workerNode);
                                                releaseResourceFuture.complete(workerNode);
                                            }));
                    final WorkerType worker =
                            releaseResourceFuture.get(TIMEOUT_SEC, TimeUnit.SECONDS);
                    context.validateReleaseResources(Collections.singleton(worker));
                });
    }

    protected abstract Context createContext();

    /** This class provides a self-contained context for each test case. */
    protected abstract class Context {
        protected final Configuration flinkConfig = new Configuration();
        protected final TestingResourceEventHandler.Builder<WorkerType>
                resourceEventHandlerBuilder = TestingResourceEventHandler.builder();

        private ResourceManagerDriver<WorkerType> driver;
        private ScheduledExecutor mainThreadExecutor;
        private BlockedNodeRetriever blockedNodeRetriever = () -> Collections.emptySet();

        protected ResourceManagerDriver<WorkerType> getDriver() {
            return driver;
        }

        public void setBlockedNodeRetriever(BlockedNodeRetriever blockedNodeRetriever) {
            this.blockedNodeRetriever = blockedNodeRetriever;
        }

        protected final void runTest(RunnableWithException testMethod) throws Exception {
            prepareRunTest();

            driver = createResourceManagerDriver();
            mainThreadExecutor = MAIN_THREAD_EXECUTOR;

            driver.initialize(
                    resourceEventHandlerBuilder.build(),
                    mainThreadExecutor,
                    ForkJoinPool.commonPool(),
                    blockedNodeRetriever);

            testMethod.run();
        }

        protected final CompletableFuture<Void> runInMainThread(RunnableWithException command) {
            final CompletableFuture<Void> future = new CompletableFuture<>();
            mainThreadExecutor.execute(
                    () -> {
                        try {
                            command.run();
                            future.complete(null);
                        } catch (Throwable e) {
                            future.completeExceptionally(e);
                        }
                    });
            return future;
        }

        protected final <T> CompletableFuture<T> runInMainThread(Supplier<T> supplier) {
            return CompletableFuture.supplyAsync(supplier, mainThreadExecutor);
        }

        protected final void validateInMainThread() {
            assertThat(Thread.currentThread().getName()).isEqualTo(MAIN_THREAD_NAME);
        }

        protected abstract void prepareRunTest() throws Exception;

        protected abstract ResourceManagerDriver<WorkerType> createResourceManagerDriver();

        protected abstract void preparePreviousAttemptWorkers();

        protected abstract void validateInitialization() throws Exception;

        protected abstract void validateWorkersRecoveredFromPreviousAttempt(
                Collection<WorkerType> workers);

        protected abstract void validateTermination() throws Exception;

        protected abstract void validateDeregisterApplication() throws Exception;

        protected abstract void validateRequestedResources(
                Collection<TaskExecutorProcessSpec> taskExecutorProcessSpecs) throws Exception;

        protected abstract void validateReleaseResources(Collection<WorkerType> workerNodes)
                throws Exception;
    }
}
