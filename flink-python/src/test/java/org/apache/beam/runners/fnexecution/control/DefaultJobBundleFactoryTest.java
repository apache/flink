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

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogControl;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.LogEntry;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.ProcessBundleDescriptor;
import org.apache.beam.model.fnexecution.v1.BeamFnControlGrpc;
import org.apache.beam.model.fnexecution.v1.BeamFnLoggingGrpc;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.fnexecution.environment.EnvironmentFactory;
import org.apache.beam.runners.fnexecution.environment.RemoteEnvironment;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.ModelCoders;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Metadata;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.MetadataUtils;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the copy of {@link DefaultJobBundleFactory} maintained in flink-python. */
class DefaultJobBundleFactoryTest {

    private static final String SECRET_VALUE = "canary-value-that-must-not-be-logged";

    private static final String FIRST_LOG = "sent when the logging stream opens";

    private static final String LAST_LOG = "sent after the control stream ended";

    @RegisterExtension
    private final LoggerAuditingExtension logging =
            new LoggerAuditingExtension(DefaultJobBundleFactory.class, Level.INFO);

    @RegisterExtension
    private final LoggerAuditingExtension workerLogging =
            new LoggerAuditingExtension("PythonWorker", Level.INFO);

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

    @Test
    void closingAnEnvironmentKeepsLogsTheHarnessSendsWhileShuttingDown() throws Exception {
        closeEnvironmentOf(new FakeHarness(true));

        assertThat(workerLogging.getMessages())
                .anySatisfy(message -> assertThat(message).contains(LAST_LOG));
    }

    @Test
    void closingAnEnvironmentEndsALoggingStreamTheHarnessKeepsOpen() throws Exception {
        // the runner stops waiting for the harness and ends the logging stream itself
        closeEnvironmentOf(new FakeHarness(false));
    }

    private void closeEnvironmentOf(FakeHarness harness) throws Exception {
        final Environment environment =
                Environments.createProcessEnvironment(
                        "", "", "pyflink-udf-runner.sh", Collections.emptyMap());
        final EnvironmentFactory.Provider environmentFactoryProvider =
                (controlServer,
                        loggingServer,
                        retrievalServer,
                        provisioningServer,
                        clientPool,
                        idGenerator) ->
                        (env, workerId) -> {
                            harness.start(
                                    controlServer.getApiServiceDescriptor().getUrl(),
                                    loggingServer.getApiServiceDescriptor().getUrl(),
                                    workerId);
                            CommonTestUtils.waitUtil(
                                    () ->
                                            workerLogging.getMessages().stream()
                                                    .anyMatch(m -> m.contains(FIRST_LOG)),
                                    Duration.ofSeconds(10),
                                    "The logging stream did not reach the runner.");
                            return RemoteEnvironment.forHandler(
                                    env,
                                    clientPool.getSource().take(workerId, Duration.ofSeconds(10)));
                        };

        try {
            try (DefaultJobBundleFactory bundleFactory =
                    new DefaultJobBundleFactory(
                            JobInfo.create(
                                    "testJob", "testJob", "token", Struct.getDefaultInstance()),
                            Collections.singletonMap(
                                    environment.getUrn(), environmentFactoryProvider))) {
                bundleFactory.forStage(getExecutableStage(environment)).close();
            }
            // checked before the harness shuts down, which would also end the stream
            harness.loggingStreamEnded.get(10, TimeUnit.SECONDS);
        } finally {
            harness.shutdown();
        }
    }

    /**
     * Connects a control stream and a logging stream like a Python SDK harness. When the runner
     * ends the control stream, it can send one more log entry and hang up, as the Python log
     * handler does when it flushes on exit.
     */
    private static final class FakeHarness {

        private final boolean hangUpAfterControlEnds;
        private final CompletableFuture<Void> loggingStreamEnded = new CompletableFuture<>();
        private final List<ManagedChannel> channels = new ArrayList<>();

        FakeHarness(boolean hangUpAfterControlEnds) {
            this.hangUpAfterControlEnds = hangUpAfterControlEnds;
        }

        void start(String controlTarget, String loggingTarget, String workerId) {
            final StreamObserver<LogEntry.List> logs =
                    BeamFnLoggingGrpc.newStub(channel(loggingTarget, workerId))
                            .logging(
                                    new StreamObserver<LogControl>() {
                                        @Override
                                        public void onNext(LogControl value) {}

                                        @Override
                                        public void onError(Throwable t) {
                                            loggingStreamEnded.complete(null);
                                        }

                                        @Override
                                        public void onCompleted() {
                                            loggingStreamEnded.complete(null);
                                        }
                                    });
            logs.onNext(logEntry(FIRST_LOG));

            final CompletableFuture<StreamObserver<InstructionResponse>> responses =
                    new CompletableFuture<>();
            responses.complete(
                    BeamFnControlGrpc.newStub(channel(controlTarget, workerId))
                            .control(
                                    new StreamObserver<InstructionRequest>() {
                                        @Override
                                        public void onNext(InstructionRequest request) {
                                            responses
                                                    .join()
                                                    .onNext(
                                                            InstructionResponse.newBuilder()
                                                                    .setInstructionId(
                                                                            request
                                                                                    .getInstructionId())
                                                                    .build());
                                        }

                                        @Override
                                        public void onError(Throwable t) {}

                                        @Override
                                        public void onCompleted() {
                                            if (hangUpAfterControlEnds) {
                                                logs.onNext(logEntry(LAST_LOG));
                                                logs.onCompleted();
                                            }
                                            responses.join().onCompleted();
                                        }
                                    }));
        }

        void shutdown() throws InterruptedException {
            for (ManagedChannel channel : channels) {
                channel.shutdownNow().awaitTermination(10, TimeUnit.SECONDS);
            }
        }

        private ManagedChannel channel(String target, String workerId) {
            final Metadata headers = new Metadata();
            headers.put(Metadata.Key.of("worker_id", Metadata.ASCII_STRING_MARSHALLER), workerId);
            final ManagedChannel channel =
                    ManagedChannelBuilder.forTarget(target)
                            .usePlaintext()
                            .intercept(MetadataUtils.newAttachHeadersInterceptor(headers))
                            .build();
            channels.add(channel);
            return channel;
        }

        private static LogEntry.List logEntry(String message) {
            return LogEntry.List.newBuilder()
                    .addLogEntries(
                            LogEntry.newBuilder()
                                    .setSeverity(LogEntry.Severity.Enum.INFO)
                                    .setMessage(message))
                    .build();
        }
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
