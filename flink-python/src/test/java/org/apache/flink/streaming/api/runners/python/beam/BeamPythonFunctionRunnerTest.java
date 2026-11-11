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
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.Timer;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.values.KV;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BeamPythonFunctionRunnerTest {

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
            assertThatThrownBy(first::takeResult).hasCause(cause);
            assertThatThrownBy(second::pollResult).hasCause(cause);
            first.onDataChannelFailure(new IOException("secondary failure"));
            second.onDataChannelFailure(new IOException("secondary failure"));

            assertThat(firstEnvironment.getActualExternalFailureCause())
                    .hasValueSatisfying(error -> assertThat(error).hasCause(cause));
            assertThat(secondEnvironment.getActualExternalFailureCause())
                    .hasValueSatisfying(error -> assertThat(error).hasCause(cause));
        }
    }

    @Test
    void testFailureInterruptsBundleClose() throws Exception {
        try (MockEnvironment environment = MockEnvironment.builder().build()) {
            environment.setExpectedExternalFailureCause(RuntimeException.class);
            final TestingBeamPythonFunctionRunner runner =
                    new TestingBeamPythonFunctionRunner(createEnvironmentManager());
            setField(runner, "environment", environment);
            runner.resultBuffer = new LinkedBlockingQueue<>();
            final BlockingBundle bundle = new BlockingBundle();
            setField(runner, "remoteBundle", bundle);
            setField(runner, "bundleStarted", true);
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                final Future<?> flush =
                        executor.submit(
                                () -> {
                                    runner.flush();
                                    return null;
                                });
                assertThat(bundle.entered.await(10, TimeUnit.SECONDS)).isTrue();
                final IOException cause = new IOException("data RPC terminated");
                runner.onDataChannelFailure(cause);
                assertThatThrownBy(() -> flush.get(10, TimeUnit.SECONDS)).hasRootCause(cause);
            } finally {
                bundle.release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    private static final class BlockingBundle implements RemoteBundle {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);

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
            entered.countDown();
            release.await();
        }
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
