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
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateRequestHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateStore;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.TimerReference;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BeamPythonFunctionRunnerTest {

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

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testCancelDoesNotWaitForConcurrentManagedBundleFlush(int otherLeases) throws Exception {
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
        final AtomicInteger sharedResourceLeases = new AtomicInteger(otherLeases + 1);
        final OpaqueMemoryResource<PythonSharedResources> sharedResources =
                createSharedResourceLease(pythonSharedResources, sharedResourceLeases);
        final OpaqueMemoryResource<PythonSharedResources> remainingSharedResourceLease =
                otherLeases > 0
                        ? createSharedResourceLease(pythonSharedResources, sharedResourceLeases)
                        : null;
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
            assertThat(sharedResourceLeases).hasValue(otherLeases);
            assertThat(sharedFactoryClosed.get()).isEqualTo(otherLeases == 0);
            assertThat(stateAccessedDuringBundleClose).isFalse();
            assertStateHandlerClosed(stateRequestHandler);

            releaseFirstBundleClose.countDown();
            assertThatThrownBy(() -> flushFuture.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IllegalStateException.class);
            assertThat(sharedFactoryClosed.get()).isEqualTo(otherLeases == 0);
            assertThat(remoteBundleCloseCalls).hasValue(1);
            assertThat(stateAccessedDuringBundleClose).isFalse();
        } finally {
            releaseFirstBundleClose.countDown();
            executor.shutdownNow();
            if (remainingSharedResourceLease != null) {
                remainingSharedResourceLease.close();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "true, 1"})
    void testCancelDoesNotCloseUnclaimedBundle(boolean sharedFactory, int otherLeases)
            throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean factoryClosed = new AtomicBoolean();
        final AtomicBoolean remoteBundleClosed = new AtomicBoolean();
        final BeamStateRequestHandler stateRequestHandler =
                createStateRequestHandler(stateAccessedDuringBundleClose);
        final JobBundleFactory jobBundleFactory = new TrackingJobBundleFactory(factoryClosed);
        final PythonSharedResources pythonSharedResources =
                new PythonSharedResources(
                        jobBundleFactory, RunnerApi.Environment.getDefaultInstance());
        final AtomicInteger sharedResourceLeases =
                new AtomicInteger(sharedFactory ? otherLeases + 1 : 0);
        final OpaqueMemoryResource<PythonSharedResources> remainingSharedResourceLease =
                otherLeases > 0
                        ? createSharedResourceLease(pythonSharedResources, sharedResourceLeases)
                        : null;
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(
                runner,
                "remoteBundle",
                new TestingRemoteBundle(stateRequestHandler, remoteBundleClosed));
        setField(runner, "bundleStarted", true);
        if (sharedFactory) {
            setField(
                    runner,
                    "sharedResources",
                    createSharedResourceLease(pythonSharedResources, sharedResourceLeases));
        } else {
            setField(runner, "jobBundleFactory", jobBundleFactory);
        }

        try {
            runner.cancel();
            runner.flush();
            runner.close();
            runner.cancel();

            assertThat(remoteBundleClosed).isFalse();
            assertThat(sharedResourceLeases).hasValue(otherLeases);
            assertThat(factoryClosed.get()).isEqualTo(otherLeases == 0);
            assertThat(stateAccessedDuringBundleClose).isFalse();
            assertStateHandlerClosed(stateRequestHandler);
        } finally {
            if (remainingSharedResourceLease != null) {
                remainingSharedResourceLease.close();
            }
        }
        assertThat(factoryClosed).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"false", "true"})
    void testCancelWaitsForConcurrentFinalLeaseDisposal(boolean failDisposal) throws Exception {
        final AtomicInteger factoryCloseCalls = new AtomicInteger();
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
        setField(runner, "stateRequestHandler", stateRequestHandler);
        setField(runner, "sharedResources", sharedResources);
        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final Future<?> firstCancelFuture =
                    executor.submit(
                            () -> {
                                runner.cancel();
                                return null;
                            });
            assertThat(resourceCleanupStarted.await(10, TimeUnit.SECONDS)).isTrue();
            final CountDownLatch cancelStarted = new CountDownLatch(1);
            final Future<?> cancelFuture =
                    executor.submit(
                            () -> {
                                cancelThread.set(Thread.currentThread());
                                cancelStarted.countDown();
                                runner.cancel();
                                return null;
                            });
            assertThat(cancelStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThreadIsWaitingForBundleClose(
                    cancelThread.get(), factoryCloseCalls, cancelFuture);
            assertThat(firstCancelFuture.isDone()).isFalse();
            assertThat(cancelFuture.isDone()).isFalse();
            assertThat(factoryCloseCalls).hasValue(1);

            releaseResourceCleanup.countDown();
            if (failDisposal) {
                assertThatThrownBy(() -> firstCancelFuture.get(10, TimeUnit.SECONDS))
                        .hasCause(disposalFailure);
                assertThatThrownBy(() -> cancelFuture.get(10, TimeUnit.SECONDS))
                        .hasCause(disposalFailure);
            } else {
                firstCancelFuture.get(10, TimeUnit.SECONDS);
                cancelFuture.get(10, TimeUnit.SECONDS);
            }
            assertStateHandlerClosed(stateRequestHandler);
            assertThat(factoryCloseCalls).hasValue(1);

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
            executor.shutdownNow();
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

        private BlockingTestingRemoteBundle(
                BeamStateRequestHandler stateRequestHandler,
                AtomicInteger closeCalls,
                CountDownLatch firstCloseStarted,
                CountDownLatch releaseFirstClose) {
            super(stateRequestHandler, new AtomicBoolean());
            this.stateRequestHandler = stateRequestHandler;
            this.closeCalls = closeCalls;
            this.firstCloseStarted = firstCloseStarted;
            this.releaseFirstClose = releaseFirstClose;
        }

        @Override
        public void close() throws Exception {
            if (closeCalls.incrementAndGet() == 1) {
                firstCloseStarted.countDown();
                awaitUninterruptibly(releaseFirstClose);
            }
            stateRequestHandler.handle(createBagUserStateRequest());
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
