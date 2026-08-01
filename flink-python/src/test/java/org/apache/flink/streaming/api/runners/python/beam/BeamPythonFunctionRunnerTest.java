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
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateRequestHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateStore;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
        assertThat(remoteBundleClosed).isFalse();
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
    void testCloseWaitsForConcurrentManagedBundleFlush() throws Exception {
        final AtomicBoolean stateAccessedDuringBundleClose = new AtomicBoolean();
        final AtomicBoolean sharedFactoryClosed = new AtomicBoolean();
        final AtomicInteger remoteBundleCloseCalls = new AtomicInteger();
        final CountDownLatch firstBundleCloseStarted = new CountDownLatch(1);
        final CountDownLatch releaseFirstBundleClose = new CountDownLatch(1);
        final CountDownLatch closeFlushStarted = new CountDownLatch(1);
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
        runner.notifyWhenCloseFlushStarts(closeFlushStarted);
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
                                runner.close();
                                return null;
                            });
            assertThat(closeFlushStarted.await(10, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> closeFuture.get(100, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
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
                releaseFirstClose.await();
            }
            stateRequestHandler.handle(createBagUserStateRequest());
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

        private volatile Thread closingThread;
        private volatile CountDownLatch closeFlushStarted;

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

        private void notifyWhenCloseFlushStarts(CountDownLatch closeFlushStarted) {
            this.closeFlushStarted = closeFlushStarted;
        }

        @Override
        public void close() throws Exception {
            closingThread = Thread.currentThread();
            try {
                super.close();
            } finally {
                closingThread = null;
            }
        }

        @Override
        public void flush() throws Exception {
            if (Thread.currentThread() == closingThread && closeFlushStarted != null) {
                closeFlushStarted.countDown();
            }
            super.flush();
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
