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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateRequestHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateStore;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.sdk.fn.data.BeamFnDataInboundObserver;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.TimerReference;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BeamPythonFunctionRunnerTest {

    @Test
    void testFailureNotificationAfterResourceCloseIsIgnored() throws Exception {
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            final TestingBeamPythonFunctionRunner runner =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            setField(runner, "environment", environment);
            setField(runner, "resourceCloseCompletion", CompletableFuture.completedFuture(null));
            runner.onDataChannelFailure(new IOException("late cancellation callback"));
            assertThat(environment.getActualExternalFailureCause()).isEmpty();
        }
    }

    @Test
    void testDataRpcFailureUnblocksResultAndBeamCompletion() throws Exception {
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            environment.setExpectedExternalFailureCause(RuntimeException.class);
            final TestingBeamPythonFunctionRunner runner =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            setField(runner, "environment", environment);
            runner.resultBuffer = new LinkedBlockingQueue<>();
            final BeamFnDataInboundObserver inbound =
                    BeamFnDataInboundObserver.forConsumers(
                            Collections.emptyList(), Collections.emptyList());
            final CountDownLatch closing = new CountDownLatch(1);
            final RemoteBundle bundle =
                    new TestingRemoteBundle(
                            createStateRequestHandler(new AtomicBoolean()), new AtomicBoolean()) {
                        @Override
                        public void close() throws Exception {
                            closing.countDown();
                            inbound.awaitCompletion();
                        }
                    };
            setField(runner, "remoteBundle", bundle);
            setField(runner, "bundleStarted", true);
            final ExecutorService executor = Executors.newFixedThreadPool(2);
            try {
                final Future<?> flush =
                        executor.submit(
                                () -> {
                                    runner.flush();
                                    return null;
                                });
                final Future<?> result = executor.submit(runner::takeResult);
                assertThat(closing.await(5, TimeUnit.SECONDS)).isTrue();
                final IOException cause = new IOException("data RPC terminated");
                runner.onDataChannelFailure(cause);
                assertThatThrownBy(() -> flush.get(5, TimeUnit.SECONDS)).hasRootCause(cause);
                assertThatThrownBy(() -> result.get(5, TimeUnit.SECONDS)).hasRootCause(cause);
                assertThat(environment.getActualExternalFailureCause())
                        .hasValueSatisfying(error -> assertThat(error).hasCause(cause));
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @Test
    void testDataRpcFailureBeforeFlushPreservesFirstCause() throws Exception {
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            environment.setExpectedExternalFailureCause(RuntimeException.class);
            final TestingBeamPythonFunctionRunner runner =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            setField(runner, "environment", environment);
            runner.resultBuffer = new LinkedBlockingQueue<>();
            final AtomicBoolean bundleClosed = new AtomicBoolean();
            final RemoteBundle bundle =
                    new TestingRemoteBundle(
                            createStateRequestHandler(new AtomicBoolean()), bundleClosed);
            setField(runner, "remoteBundle", bundle);
            setField(runner, "bundleStarted", true);
            final IOException first = new IOException("first failure");
            runner.onDataChannelFailure(first);
            runner.onDataChannelFailure(new IOException("secondary failure"));
            assertThatThrownBy(runner::flush).hasCause(first);
            assertThatThrownBy(runner::takeResult).hasCause(first);
            assertThatThrownBy(runner::pollResult).hasCause(first);
            assertThat(bundleClosed).isFalse();
            assertThat(environment.getActualExternalFailureCause())
                    .hasValueSatisfying(error -> assertThat(error).hasCause(first));
        }
    }

    @Test
    void testDataRpcFailureFailsIdleRunnersSharingWorker() throws Exception {
        try (MockEnvironment firstEnvironment = MockEnvironment.builder().build();
                MockEnvironment secondEnvironment = MockEnvironment.builder().build()) {
            firstEnvironment.setExpectedExternalFailureCause(RuntimeException.class);
            secondEnvironment.setExpectedExternalFailureCause(RuntimeException.class);
            final PythonJobBundleFactory.DataChannelFailureHandler handler =
                    new PythonJobBundleFactory.DataChannelFailureHandler();
            final TestingBeamPythonFunctionRunner first =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            final TestingBeamPythonFunctionRunner second =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            setField(first, "environment", firstEnvironment);
            setField(second, "environment", secondEnvironment);
            first.resultBuffer = new LinkedBlockingQueue<>();
            second.resultBuffer = new LinkedBlockingQueue<>();
            handler.register(first::onDataChannelFailure);
            handler.register(second::onDataChannelFailure);
            final IOException cause = new IOException("shared data RPC terminated");

            handler.fail(cause);
            first.onDataChannelFailure(new IOException("secondary failure"));
            second.onDataChannelFailure(new IOException("secondary failure"));

            assertThat(firstEnvironment.getActualExternalFailureCause())
                    .hasValueSatisfying(error -> assertThat(error).hasCause(cause));
            assertThat(secondEnvironment.getActualExternalFailureCause())
                    .hasValueSatisfying(error -> assertThat(error).hasCause(cause));
        }
    }

    @Test
    void testCloseDrainsStateHandlerAfterStoppingOwnedRequestProduction() throws Exception {
        final AtomicBoolean stateAccessedDuringFactoryClose = new AtomicBoolean();
        final AtomicBoolean remoteBundleClosed = new AtomicBoolean();
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringFactoryClose);
        final JobBundleFactory jobBundleFactory = new TestingJobBundleFactory(stateRequestHandler);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "jobBundleFactory", jobBundleFactory);
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new TestingRemoteBundle(stateRequestHandler, remoteBundleClosed));
        setField(runner, "bundleStarted", true);

        runner.close();

        assertThat(stateAccessedDuringFactoryClose).isTrue();
        assertThat(remoteBundleClosed).isTrue();
        assertStateHandlerClosed(stateRequestHandler);
    }

    @Test
    void testCloseDrainsStateHandlerForNonFinalManagedMemoryLease() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean remoteBundleClosed = new AtomicBoolean();
        final AtomicBoolean sharedFactoryClosed = new AtomicBoolean();
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TrackingJobBundleFactory(sharedFactoryClosed),
                        RunnerApi.Environment.getDefaultInstance());
        final AtomicInteger sharedResourceLeases = new AtomicInteger(2);
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final OpaqueMemoryResource<PythonSharedResources> remainingSharedResourceLease =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new TestingRemoteBundle(stateRequestHandler, remoteBundleClosed));
        setField(runner, "bundleStarted", true);
        setField(runner, "sharedResources", sharedResources);

        runner.close();

        assertThat(stateAccessedDuringBundleClose).isTrue();
        assertThat(remoteBundleClosed).isTrue();
        assertThat(sharedFactoryClosed).isFalse();
        assertStateHandlerClosed(stateRequestHandler);

        remainingSharedResourceLease.close();

        assertThat(sharedFactoryClosed).isTrue();
    }

    @Test
    void testCloseDrainsStateHandlerForFinalManagedMemoryLease() throws Exception {
        final AtomicBoolean stateAccessedDuringFactoryClose = new AtomicBoolean();
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringFactoryClose);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TestingJobBundleFactory(stateRequestHandler),
                        RunnerApi.Environment.getDefaultInstance());
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                new OpaqueMemoryResource<>(pythonSharedResources, 1L, pythonSharedResources::close);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(runner, "sharedResources", sharedResources);

        runner.close();

        assertThat(stateAccessedDuringFactoryClose).isTrue();
        assertStateHandlerClosed(stateRequestHandler);
    }

    @Test
    void testCloseWaitsForConcurrentOwnedBundleFlush() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean ownedFactoryClosed = new AtomicBoolean();
        final AtomicInteger remoteBundleCloseCalls = new AtomicInteger();
        final AtomicReference<Thread> closeThread = new AtomicReference<>();
        final CountDownLatch firstBundleCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstBundleClose = new CountDownLatch(1);
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "jobBundleFactory", new TrackingJobBundleFactory(ownedFactoryClosed));
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new BlockingTestingRemoteBundle(
                        stateRequestHandler,
                        remoteBundleCloseCalls,
                        firstBundleCloseStarted,
                        releaseFirstBundleClose));
        setField(runner, "bundleStarted", true);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> flushFuture =
                    executor.submit(
                            () -> {
                                runner.flush();
                                return null;
                            });
            assertThat(firstBundleCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

            final Future<?> closeFuture =
                    executor.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closeStarted.countDown();
                                runner.close();
                                return null;
                            });
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThreadIsWaitingForBundleClose(
                    closeThread.get(), remoteBundleCloseCalls, closeFuture);
            assertThat(closeFuture.isDone()).isFalse();
            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(ownedFactoryClosed).isFalse();

            releaseFirstBundleClose.countDown();
            flushFuture.get(10, TimeUnit.SECONDS);
            closeFuture.get(10, TimeUnit.SECONDS);

            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(ownedFactoryClosed).isTrue();
            assertThat(stateAccessedDuringBundleClose).isTrue();
            assertStateHandlerClosed(stateRequestHandler);
        } finally {
            releaseFirstBundleClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testCloseWaitsForConcurrentManagedBundleFlush() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean sharedFactoryClosed = new AtomicBoolean();
        final AtomicInteger remoteBundleCloseCalls = new AtomicInteger();
        final AtomicReference<Thread> closeThread = new AtomicReference<>();
        final CountDownLatch firstBundleCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstBundleClose = new CountDownLatch(1);
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TrackingJobBundleFactory(sharedFactoryClosed),
                        RunnerApi.Environment.getDefaultInstance());
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                new OpaqueMemoryResource<>(pythonSharedResources, 1L, pythonSharedResources::close);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new BlockingTestingRemoteBundle(
                        stateRequestHandler,
                        remoteBundleCloseCalls,
                        firstBundleCloseStarted,
                        releaseFirstBundleClose));
        setField(runner, "bundleStarted", true);
        setField(runner, "sharedResources", sharedResources);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> flushFuture =
                    executor.submit(
                            () -> {
                                runner.flush();
                                return null;
                            });
            assertThat(firstBundleCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

            final Future<?> closeFuture =
                    executor.submit(
                            () -> {
                                closeThread.set(Thread.currentThread());
                                closeStarted.countDown();
                                runner.close();
                                return null;
                            });
            assertThat(closeStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThreadIsWaitingForBundleClose(
                    closeThread.get(), remoteBundleCloseCalls, closeFuture);
            assertThat(closeFuture.isDone()).isFalse();
            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(sharedFactoryClosed).isFalse();

            releaseFirstBundleClose.countDown();
            flushFuture.get(10, TimeUnit.SECONDS);
            closeFuture.get(10, TimeUnit.SECONDS);

            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(sharedFactoryClosed).isTrue();
            assertThat(stateAccessedDuringBundleClose).isTrue();
            assertStateHandlerClosed(stateRequestHandler);
        } finally {
            releaseFirstBundleClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testCancelDoesNotWaitForConcurrentManagedBundleFlush() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean sharedFactoryClosed = new AtomicBoolean();
        final AtomicInteger remoteBundleCloseCalls = new AtomicInteger();
        final CountDownLatch firstBundleCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstBundleClose = new CountDownLatch(1);
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TrackingJobBundleFactory(sharedFactoryClosed),
                        RunnerApi.Environment.getDefaultInstance());
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                new OpaqueMemoryResource<>(pythonSharedResources, 1L, pythonSharedResources::close);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new BlockingTestingRemoteBundle(
                        stateRequestHandler,
                        remoteBundleCloseCalls,
                        firstBundleCloseStarted,
                        releaseFirstBundleClose));
        setField(runner, "bundleStarted", true);
        setField(runner, "sharedResources", sharedResources);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> flushFuture =
                    executor.submit(
                            () -> {
                                runner.flush();
                                return null;
                            });
            assertThat(firstBundleCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

            final Future<?> cancelFuture =
                    executor.submit(
                            () -> {
                                runner.cancel();
                                return null;
                            });
            cancelFuture.get(10, TimeUnit.SECONDS);

            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(sharedFactoryClosed).isTrue();
            assertThat(stateAccessedDuringBundleClose).isFalse();
            assertStateHandlerClosed(stateRequestHandler);

            releaseFirstBundleClose.countDown();
            assertThatThrownBy(() -> flushFuture.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(sharedFactoryClosed).isTrue();
            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(stateAccessedDuringBundleClose).isFalse();
        } finally {
            releaseFirstBundleClose.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void testCancelRetainsUnclaimedManagedBundleCleanupOwnership() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean sharedFactoryClosed = new AtomicBoolean();
        final AtomicInteger remoteBundleCloseCalls = new AtomicInteger();
        final CountDownLatch bundleCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseBundleClose = new CountDownLatch(1);
        final CountDownLatch bundleCloseFinished = new CountDownLatch(1);
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TrackingJobBundleFactory(sharedFactoryClosed),
                        RunnerApi.Environment.getDefaultInstance());
        final AtomicInteger sharedResourceLeases = new AtomicInteger(2);
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final OpaqueMemoryResource<PythonSharedResources> remainingSharedResourceLease =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new BlockingTestingRemoteBundle(
                        stateRequestHandler,
                        remoteBundleCloseCalls,
                        bundleCloseStarted,
                        releaseBundleClose,
                        bundleCloseFinished));
        setField(runner, "bundleStarted", true);
        setField(runner, "sharedResources", sharedResources);

        try {
            runner.cancel();
            assertThat(bundleCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();
            runner.flush();

            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(sharedFactoryClosed).isFalse();
            assertStateHandlerClosed(stateRequestHandler);

            releaseBundleClose.countDown();
            assertThat(bundleCloseFinished.await(10, TimeUnit.SECONDS)).isTrue();
            assertRemainingSharedResourceLeases(sharedResourceLeases, 1);
            assertThat(stateAccessedDuringBundleClose).isFalse();
            assertThat(sharedFactoryClosed).isFalse();

            remainingSharedResourceLease.close();

            assertThat(sharedFactoryClosed).isTrue();
        } finally {
            releaseBundleClose.countDown();
            remainingSharedResourceLease.close();
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testCancelWaitsForConcurrentFinalLeaseDisposal(
            boolean failDisposal, boolean interruptCancel) throws Exception {
        final AtomicInteger factoryCloseCalls = new AtomicInteger();
        final AtomicBoolean cancelInterrupted = new AtomicBoolean();
        final AtomicReference<Thread> cancelThread = new AtomicReference<>();
        final CountDownLatch resourceCleanupStarted = new CountDownLatch(1);
        final CountDownLatch releaseResourceCleanup = new CountDownLatch(1);
        final RuntimeException disposalFailure = new RuntimeException("Resource disposal failed.");
        final JobBundleFactory jobBundleFactory =
                new TrackingJobBundleFactory(new AtomicBoolean()) {
                    @Override
                    public void close() {
                        factoryCloseCalls.incrementAndGet();
                        resourceCleanupStarted.countDown();
                        try {
                            assertThat(releaseResourceCleanup.await(10, TimeUnit.SECONDS)).isTrue();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        if (failDisposal) {
                            throw disposalFailure;
                        }
                    }
                };
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        jobBundleFactory, RunnerApi.Environment.getDefaultInstance());
        final SharedResources registry = new SharedResources();
        final Object leaseHolder = new Object();
        final SharedResources.ResourceAndSize<PythonSharedResources> resource =
                registry.getOrAllocateSharedResource(
                        "python", leaseHolder, ignored -> pythonSharedResources, 1L);
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                new OpaqueMemoryResource<>(
                        resource.resourceHandle(),
                        resource.size(),
                        () -> registry.release("python", leaseHolder, ignored -> {}));
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(new AtomicBoolean());
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        final ExecutorService cancellationExecutor =
                new CleanupAwaitingExecutor(resourceCleanupStarted, interruptCancel);
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new TestingRemoteBundle(stateRequestHandler, new AtomicBoolean()));
        setField(runner, "bundleStarted", true);
        setField(runner, "sharedResources", sharedResources);
        setField(runner, "cancellationBundleCloseExecutor", cancellationExecutor);
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        try {
            final Future<?> cancelFuture =
                    executor.submit(
                            () -> {
                                cancelThread.set(Thread.currentThread());
                                try {
                                    runner.cancel();
                                } finally {
                                    cancelInterrupted.set(Thread.currentThread().isInterrupted());
                                }
                                return null;
                            });
            assertThat(resourceCleanupStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThreadIsWaitingForBundleClose(
                    cancelThread.get(), factoryCloseCalls, cancelFuture);
            assertThat(cancelFuture.isDone()).isFalse();
            assertThat(factoryCloseCalls).hasValue(1);

            releaseResourceCleanup.countDown();
            if (failDisposal) {
                assertThatThrownBy(() -> cancelFuture.get(10, TimeUnit.SECONDS))
                        .hasCause(disposalFailure);
            } else {
                cancelFuture.get(10, TimeUnit.SECONDS);
            }
            assertStateHandlerClosed(stateRequestHandler);
            assertThat(factoryCloseCalls).hasValue(1);
            assertThat(cancelInterrupted.get()).isEqualTo(interruptCancel);

            final AtomicBoolean replacementFactoryClosed = new AtomicBoolean();
            executor.submit(
                            () -> {
                                final Object replacementLeaseHolder = new Object();
                                registry.getOrAllocateSharedResource(
                                        "python",
                                        replacementLeaseHolder,
                                        ignored ->
                                                new PythonSharedResources(
                                                        new TrackingJobBundleFactory(
                                                                replacementFactoryClosed),
                                                        RunnerApi.Environment.getDefaultInstance()),
                                        1L);
                                registry.release("python", replacementLeaseHolder, ignored -> {});
                                return null;
                            })
                    .get(10, TimeUnit.SECONDS);
            assertThat(replacementFactoryClosed).isTrue();
            assertThat(factoryCloseCalls).hasValue(1);
        } finally {
            releaseResourceCleanup.countDown();
            cancellationExecutor.shutdownNow();
            executor.shutdownNow();
            assertThat(cancellationExecutor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void testConcurrentCloseIsIdempotent() throws Exception {
        final AtomicInteger stateHandlerCloseCalls = new AtomicInteger();
        final AtomicReference<Thread> secondCloseThread = new AtomicReference<>();
        final CountDownLatch firstStateHandlerCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstStateHandlerClose = new CountDownLatch(1);
        final CountDownLatch secondCloseStarted = new CountDownLatch(1);
        final ProcessPythonEnvironmentManager environmentManager = createEnvironmentManager();
        environmentManager.open();
        final Path environmentDirectory = Paths.get(environmentManager.getBaseDirectory());
        final BlockingCloseBeamStateRequestHandler stateRequestHandler =
                new BlockingCloseBeamStateRequestHandler(
                        stateHandlerCloseCalls,
                        firstStateHandlerCloseStarted,
                        releaseFirstStateHandlerClose);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        new TrackingJobBundleFactory(new AtomicBoolean()),
                        RunnerApi.Environment.getDefaultInstance());
        pythonSharedResources.addPythonEnvironmentManager(environmentManager);
        final AtomicInteger sharedResourceLeases = new AtomicInteger(2);
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final OpaqueMemoryResource<PythonSharedResources> remainingSharedResourceLease =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(environmentManager);
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(runner, "sharedResources", sharedResources);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> firstCloseFuture =
                    executor.submit(
                            () -> {
                                runner.close();
                                return null;
                            });
            assertThat(firstStateHandlerCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();

            final Future<?> secondCloseFuture =
                    executor.submit(
                            () -> {
                                secondCloseThread.set(Thread.currentThread());
                                secondCloseStarted.countDown();
                                runner.close();
                                return null;
                            });
            assertThat(secondCloseStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThreadIsWaitingForBundleClose(
                    secondCloseThread.get(), stateHandlerCloseCalls, secondCloseFuture);
            assertThat(secondCloseFuture.isDone()).isFalse();
            assertThat(stateHandlerCloseCalls).hasValue(1);
            assertThat(Files.exists(environmentDirectory)).isTrue();

            releaseFirstStateHandlerClose.countDown();
            firstCloseFuture.get(10, TimeUnit.SECONDS);
            secondCloseFuture.get(10, TimeUnit.SECONDS);

            assertThat(stateHandlerCloseCalls).hasValue(1);
            assertThat(Files.exists(environmentDirectory)).isTrue();
        } finally {
            releaseFirstStateHandlerClose.countDown();
            executor.shutdownNow();
            remainingSharedResourceLease.close();
            environmentManager.close();
        }
    }

    private static void assertThreadIsWaitingForBundleClose(
            Thread closeThread, AtomicInteger concurrentCloseCalls, Future<?> closeFuture) {
        final long timeoutNanos = TimeUnit.SECONDS.toNanos(10);
        final long deadlineNanos = System.nanoTime() + timeoutNanos;
        while (System.nanoTime() < deadlineNanos) {
            if (closeFuture.isDone()) {
                assertThat(closeFuture.isDone()).isFalse();
            }
            if (concurrentCloseCalls.get() > 1) {
                assertThat(concurrentCloseCalls).hasValue(1);
            }
            if (closeThread.getState() == Thread.State.WAITING) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertThat(closeThread.getState()).isEqualTo(Thread.State.WAITING);
    }

    private static void assertRemainingSharedResourceLeases(
            AtomicInteger remainingLeases, int expectedLeases) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadlineNanos) {
            if (remainingLeases.get() == expectedLeases) {
                return;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        assertThat(remainingLeases).hasValue(expectedLeases);
    }

    private static OpaqueMemoryResource<PythonSharedResources> createSharedResourceLease(
            PythonSharedResources pythonSharedResources, AtomicInteger remainingLeases) {
        return new OpaqueMemoryResource<>(
                pythonSharedResources,
                1L,
                () -> {
                    if (remainingLeases.decrementAndGet() == 0) {
                        pythonSharedResources.close();
                    }
                });
    }

    private static BeamStateRequestHandler createStateRequestHandler(AtomicBoolean stateAccessed) {
        final BeamStateStore keyedStateStore =
                new BeamStateStore() {
                    @Override
                    public ListState<byte[]> getListState(BeamFnApi.StateRequest request) {
                        stateAccessed.set(true);
                        return null;
                    }

                    @Override
                    public MapState<ByteArrayWrapper, byte[]> getMapState(
                            BeamFnApi.StateRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };
        return new BeamStateRequestHandler(
                keyedStateStore,
                BeamStateStore.unsupported(),
                new NoOpBeamStateHandler<>(),
                new NoOpBeamStateHandler<>());
    }

    private static void assertStateHandlerClosed(BeamStateRequestHandler stateRequestHandler) {
        assertThatThrownBy(() -> stateRequestHandler.handle(createBagUserStateRequest()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Beam state request handler is closed.");
    }

    private static ProcessPythonEnvironmentManager createEnvironmentManager() {
        return new ProcessPythonEnvironmentManager(
                new PythonDependencyInfo(
                        Collections.emptyMap(), null, null, Collections.emptyMap(), "python"),
                new String[] {System.getProperty("java.io.tmpdir")},
                Collections.emptyMap(),
                new JobID());
    }

    private static void setField(Object target, String fieldName, Object value)
            throws ReflectiveOperationException {
        final Field field = BeamPythonFunctionRunner.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static BeamFnApi.StateRequest createBagUserStateRequest() {
        return BeamFnApi.StateRequest.newBuilder()
                .setStateKey(
                        BeamFnApi.StateKey.newBuilder()
                                .setBagUserState(
                                        BeamFnApi.StateKey.BagUserState.getDefaultInstance()))
                .setGet(BeamFnApi.StateGetRequest.getDefaultInstance())
                .build();
    }

    private static class TestingJobBundleFactory implements JobBundleFactory {

        private final BeamStateRequestHandler stateRequestHandler;

        private TestingJobBundleFactory(BeamStateRequestHandler stateRequestHandler) {
            this.stateRequestHandler = stateRequestHandler;
        }

        @Override
        public StageBundleFactory forStage(ExecutableStage executableStage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() throws Exception {
            stateRequestHandler.handle(createBagUserStateRequest());
        }
    }

    private static class TestingRemoteBundle implements RemoteBundle {

        private final BeamStateRequestHandler stateRequestHandler;
        private final AtomicBoolean closed;

        private TestingRemoteBundle(
                BeamStateRequestHandler stateRequestHandler, AtomicBoolean closed) {
            this.stateRequestHandler = stateRequestHandler;
            this.closed = closed;
        }

        @Override
        public String getId() {
            return "test-bundle";
        }

        @Override
        public Map<String, FnDataReceiver> getInputReceivers() {
            return Collections.emptyMap();
        }

        @Override
        public Map<KV<String, String>, FnDataReceiver<Timer>> getTimerReceivers() {
            return Collections.emptyMap();
        }

        @Override
        public void requestProgress() {}

        @Override
        public void split(double fractionOfRemainder) {}

        @Override
        public void close() throws Exception {
            closed.set(true);
            stateRequestHandler.handle(createBagUserStateRequest());
        }
    }

    private static class BlockingTestingRemoteBundle extends TestingRemoteBundle {

        private final BeamStateRequestHandler stateRequestHandler;
        private final AtomicInteger closeCalls;
        private final CountDownLatch firstCloseStarted;
        private final CountDownLatch releaseFirstClose;
        private final CountDownLatch closeFinished;

        private BlockingTestingRemoteBundle(
                BeamStateRequestHandler stateRequestHandler,
                AtomicInteger closeCalls,
                CountDownLatch firstCloseStarted,
                CountDownLatch releaseFirstClose) {
            this(
                    stateRequestHandler,
                    closeCalls,
                    firstCloseStarted,
                    releaseFirstClose,
                    new CountDownLatch(0));
        }

        private BlockingTestingRemoteBundle(
                BeamStateRequestHandler stateRequestHandler,
                AtomicInteger closeCalls,
                CountDownLatch firstCloseStarted,
                CountDownLatch releaseFirstClose,
                CountDownLatch closeFinished) {
            super(stateRequestHandler, new AtomicBoolean());
            this.stateRequestHandler = stateRequestHandler;
            this.closeCalls = closeCalls;
            this.firstCloseStarted = firstCloseStarted;
            this.releaseFirstClose = releaseFirstClose;
            this.closeFinished = closeFinished;
        }

        @Override
        public void close() throws Exception {
            try {
                if (closeCalls.incrementAndGet() == 1) {
                    firstCloseStarted.countDown();
                    awaitUninterruptibly(releaseFirstClose);
                }
                stateRequestHandler.handle(createBagUserStateRequest());
            } finally {
                closeFinished.countDown();
            }
        }

        private static void awaitUninterruptibly(CountDownLatch latch) {
            boolean interrupted = false;
            while (true) {
                try {
                    latch.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class BlockingCloseBeamStateRequestHandler extends BeamStateRequestHandler {

        private final AtomicInteger closeCalls;
        private final CountDownLatch firstCloseStarted;
        private final CountDownLatch releaseFirstClose;

        private BlockingCloseBeamStateRequestHandler(
                AtomicInteger closeCalls,
                CountDownLatch firstCloseStarted,
                CountDownLatch releaseFirstClose) {
            super(
                    BeamStateStore.unsupported(),
                    BeamStateStore.unsupported(),
                    new NoOpBeamStateHandler<>(),
                    new NoOpBeamStateHandler<>());
            this.closeCalls = closeCalls;
            this.firstCloseStarted = firstCloseStarted;
            this.releaseFirstClose = releaseFirstClose;
        }

        @Override
        public void close() {
            if (closeCalls.incrementAndGet() == 1) {
                firstCloseStarted.countDown();
                try {
                    releaseFirstClose.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while closing state handler.", e);
                }
            }
            super.close();
        }
    }

    private static class TrackingJobBundleFactory implements JobBundleFactory {

        private final AtomicBoolean closed;

        private TrackingJobBundleFactory(AtomicBoolean closed) {
            this.closed = closed;
        }

        @Override
        public StageBundleFactory forStage(ExecutableStage executableStage) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static class CleanupAwaitingExecutor extends ThreadPoolExecutor {

        private final CountDownLatch resourceCleanupStarted;
        private final boolean interruptCancel;

        private CleanupAwaitingExecutor(
                CountDownLatch resourceCleanupStarted, boolean interruptCancel) {
            super(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
            this.resourceCleanupStarted = resourceCleanupStarted;
            this.interruptCancel = interruptCancel;
        }

        @Override
        public void execute(Runnable command) {
            super.execute(command);
            try {
                assertThat(resourceCleanupStarted.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (interruptCancel) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static class NoOpBeamStateHandler<S> implements BeamStateHandler<S> {

        @Override
        public BeamFnApi.StateResponse.Builder handle(BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleGet(BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleAppend(
                BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }

        @Override
        public BeamFnApi.StateResponse.Builder handleClear(
                BeamFnApi.StateRequest request, S state) {
            return BeamFnApi.StateResponse.newBuilder();
        }
    }

    private static class TestingBeamPythonFunctionRunner extends BeamPythonFunctionRunner {

        private TestingBeamPythonFunctionRunner(
                ProcessPythonEnvironmentManager environmentManager) {
            super(
                    null,
                    "test-task",
                    environmentManager,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    0.0,
                    FlinkFnApi.CoderInfoDescriptor.getDefaultInstance(),
                    FlinkFnApi.CoderInfoDescriptor.getDefaultInstance(),
                    Collections.emptyMap());
        }

        @Override
        protected void buildTransforms(RunnerApi.Components.Builder componentsBuilder) {}

        @Override
        protected List<TimerReference> getTimers(RunnerApi.Components components) {
            return Collections.emptyList();
        }

        @Override
        protected Optional<RunnerApi.Coder> getOptionalTimerCoderProto() {
            return Optional.empty();
        }
    }
}
