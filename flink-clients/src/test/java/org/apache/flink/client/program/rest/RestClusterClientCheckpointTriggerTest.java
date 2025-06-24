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

package org.apache.flink.client.program.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.FunctionWithException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@link RestClusterClient} for operations that trigger checkpoints.
 *
 * <p>These tests verify that the client uses the appropriate headers for each request, properly
 * constructs the request bodies/parameters and processes the responses correctly.
 */
class RestClusterClientCheckpointTriggerTest {

    private static final DispatcherGateway mockRestfulGateway =
            TestingDispatcherGateway.newBuilder().build();

    private static final GatewayRetriever<DispatcherGateway> mockGatewayRetriever =
            () -> CompletableFuture.completedFuture(mockRestfulGateway);

    private static ExecutorService executor;

    private static final Configuration REST_CONFIG;

    static {
        final Configuration config = new Configuration();
        config.set(JobManagerOptions.ADDRESS, "localhost");
        config.set(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        config.set(RestOptions.RETRY_DELAY, Duration.ofMillis(0L));
        config.set(RestOptions.PORT, 0);

        REST_CONFIG = new UnmodifiableConfiguration(config);
    }

    @BeforeAll
    static void setUp() {
        executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                RestClusterClientCheckpointTriggerTest.class.getSimpleName()));
    }

    @AfterAll
    static void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @ParameterizedTest
    @EnumSource(CheckpointType.class)
    void testTriggerCheckpointWithType(CheckpointType type) throws Exception {
        final TriggerId triggerId = new TriggerId();
        final long expectedCheckpointId = 5L;

        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        request -> {
                            assertThat(request.getCheckpointType()).isEqualTo(type);
                            return triggerId;
                        },
                        trigger -> {
                            assertThat(triggerId).isEqualTo(trigger);
                            return new CheckpointInfo(expectedCheckpointId, null);
                        })) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            final long checkpointId = restClusterClient.triggerCheckpoint(new JobID(), type).get();
            assertThat(checkpointId).isEqualTo(expectedCheckpointId);
        }
    }

    private static RestServerEndpoint createRestServerEndpoint(
            final FunctionWithException<
                            CheckpointTriggerRequestBody, TriggerId, RestHandlerException>
                    triggerHandlerLogic,
            final FunctionWithException<TriggerId, CheckpointInfo, RestHandlerException>
                    checkpointHandlerLogic)
            throws Exception {
        return TestRestServerEndpoint.builder(REST_CONFIG)
                .withHandler(new TestCheckpointTriggerHandler(triggerHandlerLogic))
                .withHandler(new TestCheckpointHandler(checkpointHandlerLogic))
                .buildAndStart();
    }

    private static final class TestCheckpointTriggerHandler
            extends TestHandler<
                    CheckpointTriggerRequestBody,
                    TriggerResponse,
                    CheckpointTriggerMessageParameters> {

        private final FunctionWithException<
                        CheckpointTriggerRequestBody, TriggerId, RestHandlerException>
                triggerHandlerLogic;

        TestCheckpointTriggerHandler(
                final FunctionWithException<
                                CheckpointTriggerRequestBody, TriggerId, RestHandlerException>
                        triggerHandlerLogic) {
            super(CheckpointTriggerHeaders.getInstance());
            this.triggerHandlerLogic = triggerHandlerLogic;
        }

        @Override
        protected CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<CheckpointTriggerRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {

            return CompletableFuture.completedFuture(
                    new TriggerResponse(triggerHandlerLogic.apply(request.getRequestBody())));
        }
    }

    private static class TestCheckpointHandler
            extends TestHandler<
                    EmptyRequestBody,
                    AsynchronousOperationResult<CheckpointInfo>,
                    CheckpointStatusMessageParameters> {

        private final FunctionWithException<TriggerId, CheckpointInfo, RestHandlerException>
                checkpointHandlerLogic;

        TestCheckpointHandler(
                final FunctionWithException<TriggerId, CheckpointInfo, RestHandlerException>
                        checkpointHandlerLogic) {
            super(CheckpointStatusHeaders.getInstance());
            this.checkpointHandlerLogic = checkpointHandlerLogic;
        }

        @Override
        protected CompletableFuture<AsynchronousOperationResult<CheckpointInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {

            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            return CompletableFuture.completedFuture(
                    AsynchronousOperationResult.completed(checkpointHandlerLogic.apply(triggerId)));
        }
    }

    private abstract static class TestHandler<
                    R extends RequestBody, P extends ResponseBody, M extends MessageParameters>
            extends AbstractRestHandler<DispatcherGateway, R, P, M> {

        private TestHandler(MessageHeaders<R, P, M> headers) {
            super(mockGatewayRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), headers);
        }
    }

    private RestClusterClient<StandaloneClusterId> createRestClusterClient(final int port)
            throws Exception {
        final Configuration clientConfig = new Configuration(REST_CONFIG);
        clientConfig.set(RestOptions.PORT, port);
        return new RestClusterClient<>(
                clientConfig,
                new RestClient(REST_CONFIG, executor),
                StandaloneClusterId.getInstance(),
                (attempt) -> 0);
    }
}
