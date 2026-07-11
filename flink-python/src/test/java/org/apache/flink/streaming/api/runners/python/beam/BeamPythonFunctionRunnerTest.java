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
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateRequestHandler;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateStore;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.core.construction.graph.TimerReference;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BeamPythonFunctionRunnerTest {

    @Test
    void testCloseDrainsStateHandlerAfterStoppingRequestProduction() throws Exception {
        final AtomicBoolean stateAccessedDuringFactoryClose = new AtomicBoolean();
        final BeamStateStore keyedStateStore =
                new BeamStateStore() {
                    @Override
                    public ListState<byte[]> getListState(BeamFnApi.StateRequest request) {
                        stateAccessedDuringFactoryClose.set(true);
                        return null;
                    }

                    @Override
                    public MapState<ByteArrayWrapper, byte[]> getMapState(
                            BeamFnApi.StateRequest request) {
                        throw new UnsupportedOperationException();
                    }
                };
        final BeamStateRequestHandler stateRequestHandler =
                new BeamStateRequestHandler(
                        keyedStateStore,
                        BeamStateStore.unsupported(),
                        new NoOpBeamStateHandler<>(),
                        new NoOpBeamStateHandler<>());
        final JobBundleFactory jobBundleFactory = new TestingJobBundleFactory(stateRequestHandler);
        final TestingBeamPythonFunctionRunner runner =
                new TestingBeamPythonFunctionRunner(createEnvironmentManager());
        setField(runner, "jobBundleFactory", jobBundleFactory);
        setField(runner, "stateRequestHandler", stateRequestHandler);

        runner.close();

        assertThat(stateAccessedDuringFactoryClose).isTrue();
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
