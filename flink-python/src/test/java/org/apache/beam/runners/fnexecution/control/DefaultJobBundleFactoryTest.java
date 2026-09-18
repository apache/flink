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

package org.apache.beam.runners.fnexecution.control;

import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.ModelCoders;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the copy of {@link DefaultJobBundleFactory} maintained in flink-python. */
class DefaultJobBundleFactoryTest {

    private static final String SECRET_VALUE = "canary-value-that-must-not-be-logged";

    @RegisterExtension
    private final LoggerAuditingExtension logging =
            new LoggerAuditingExtension(DefaultJobBundleFactory.class, Level.INFO);

    @Test
    void closingAnEnvironmentDoesNotLogItsPayload() throws Exception {
        final Environment environment =
                Environments.createProcessEnvironment(
                        "",
                        "",
                        "pyflink-udf-runner.sh",
                        Collections.singletonMap("MY_SECRET", SECRET_VALUE));
        final EnvironmentFactory environmentFactory =
                (env, workerId) ->
                        RemoteEnvironment.forHandler(env, new AcknowledgingRequestHandler());
        final EnvironmentFactory.Provider environmentFactoryProvider =
                (controlServer,
                        loggingServer,
                        retrievalServer,
                        provisioningServer,
                        clientPool,
                        idGenerator) -> environmentFactory;

        try (DefaultJobBundleFactory bundleFactory =
                new DefaultJobBundleFactory(
                        JobInfo.create("testJob", "testJob", "token", Struct.getDefaultInstance()),
                        Collections.singletonMap(
                                environment.getUrn(), environmentFactoryProvider))) {
            // the stage holds a reference on the environment; releasing both closes it
            bundleFactory.forStage(getExecutableStage(environment)).close();
        }

        assertThat(logging.getMessages())
                .anySatisfy(
                        message ->
                                assertThat(message)
                                        .contains("Closing environment")
                                        .contains(environment.getUrn()))
                .noneSatisfy(message -> assertThat(message).contains(SECRET_VALUE));
    }

    /** Answers every instruction immediately, standing in for an SDK harness. */
    private static final class AcknowledgingRequestHandler implements InstructionRequestHandler {

        @Override
        public void registerProcessBundleDescriptor(ProcessBundleDescriptor descriptor) {}

        @Override
        public CompletionStage<InstructionResponse> handle(InstructionRequest request) {
            return CompletableFuture.completedFuture(
                    InstructionResponse.newBuilder()
                            .setInstructionId(request.getInstructionId())
                            .build());
        }

        @Override
        public void close() {}
    }

    private static ExecutableStage getExecutableStage(Environment environment) {
        return ExecutableStage.fromPayload(
                ExecutableStagePayload.newBuilder()
                        .setInput("input-pc")
                        .setEnvironment(environment)
                        .setComponents(
                                Components.newBuilder()
                                        .putPcollections(
                                                "input-pc",
                                                PCollection.newBuilder()
                                                        .setWindowingStrategyId(
                                                                "windowing-strategy")
                                                        .setCoderId("coder-id")
                                                        .build())
                                        .putWindowingStrategies(
                                                "windowing-strategy",
                                                WindowingStrategy.newBuilder()
                                                        .setWindowCoderId("coder-id")
                                                        .build())
                                        .putCoders(
                                                "coder-id",
                                                Coder.newBuilder()
                                                        .setSpec(
                                                                FunctionSpec.newBuilder()
                                                                        .setUrn(
                                                                                ModelCoders
                                                                                        .INTERVAL_WINDOW_CODER_URN)
                                                                        .build())
                                                        .build())
                                        .build())
                        .build());
    }
}
