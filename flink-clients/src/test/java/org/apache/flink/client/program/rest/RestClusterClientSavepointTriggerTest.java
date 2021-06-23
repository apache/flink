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
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointInfo;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.job.savepoints.SavepointTriggerRequestBody;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link RestClusterClient} for operations that trigger savepoints.
 *
 * <p>These tests verify that the client uses the appropriate headers for each request, properly
 * constructs the request bodies/parameters and processes the responses correctly.
 */
public class RestClusterClientSavepointTriggerTest extends TestLogger {

    private static final DispatcherGateway mockRestfulGateway =
            new TestingDispatcherGateway.Builder().build();

    private static final GatewayRetriever<DispatcherGateway> mockGatewayRetriever =
            () -> CompletableFuture.completedFuture(mockRestfulGateway);

    private static ExecutorService executor;

    private static final Configuration REST_CONFIG;

    static {
        final Configuration config = new Configuration();
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(RestOptions.RETRY_MAX_ATTEMPTS, 10);
        config.setLong(RestOptions.RETRY_DELAY, 0);
        config.setInteger(RestOptions.PORT, 0);

        REST_CONFIG = new UnmodifiableConfiguration(config);
    }

    @BeforeClass
    public static void setUp() throws ConfigurationException {
        executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                RestClusterClientSavepointTriggerTest.class.getSimpleName()));
    }

    @AfterClass
    public static void tearDown() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void testTriggerSavepointDefaultDirectory() throws Exception {
        final TriggerId triggerId = new TriggerId();
        final String expectedReturnedSavepointDir = "hello";

        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        request -> {
                            assertNull(request.getTargetDirectory());
                            assertFalse(request.isCancelJob());
                            return triggerId;
                        },
                        trigger -> {
                            assertEquals(triggerId, trigger);
                            return new SavepointInfo(expectedReturnedSavepointDir, null);
                        })) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            final String savepointPath =
                    restClusterClient.triggerSavepoint(new JobID(), null).get();
            assertEquals(expectedReturnedSavepointDir, savepointPath);
        }
    }

    @Test
    public void testTriggerSavepointTargetDirectory() throws Exception {
        final TriggerId triggerId = new TriggerId();
        final String expectedSubmittedSavepointDir = "world";
        final String expectedReturnedSavepointDir = "hello";

        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        triggerRequestBody -> {
                            assertEquals(
                                    expectedSubmittedSavepointDir,
                                    triggerRequestBody.getTargetDirectory());
                            assertFalse(triggerRequestBody.isCancelJob());
                            return triggerId;
                        },
                        statusRequestTriggerId -> {
                            assertEquals(triggerId, statusRequestTriggerId);
                            return new SavepointInfo(expectedReturnedSavepointDir, null);
                        })) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            final String savepointPath =
                    restClusterClient
                            .triggerSavepoint(new JobID(), expectedSubmittedSavepointDir)
                            .get();
            assertEquals(expectedReturnedSavepointDir, savepointPath);
        }
    }

    @Test
    public void testTriggerSavepointCancelJob() throws Exception {
        final TriggerId triggerId = new TriggerId();
        final String expectedSavepointDir = "hello";

        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        request -> {
                            assertTrue(request.isCancelJob());
                            return triggerId;
                        },
                        trigger -> {
                            assertEquals(triggerId, trigger);
                            return new SavepointInfo(expectedSavepointDir, null);
                        })) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            final String savepointPath =
                    restClusterClient.cancelWithSavepoint(new JobID(), null).get();
            assertEquals(expectedSavepointDir, savepointPath);
        }
    }

    @Test
    public void testTriggerSavepointFailure() throws Exception {
        final TriggerId triggerId = new TriggerId();

        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        request -> triggerId,
                        trigger ->
                                new SavepointInfo(
                                        null,
                                        new SerializedThrowable(
                                                new RuntimeException("expected"))))) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            try {
                restClusterClient.triggerSavepoint(new JobID(), null).get();
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                assertThat(cause, instanceOf(SerializedThrowable.class));
                assertThat(
                        ((SerializedThrowable) cause)
                                .deserializeError(ClassLoader.getSystemClassLoader())
                                .getMessage(),
                        equalTo("expected"));
            }
        }
    }

    @Test
    public void testTriggerSavepointRetry() throws Exception {
        final TriggerId triggerId = new TriggerId();
        final String expectedSavepointDir = "hello";

        final AtomicBoolean failRequest = new AtomicBoolean(true);
        try (final RestServerEndpoint restServerEndpoint =
                createRestServerEndpoint(
                        request -> triggerId,
                        trigger -> {
                            if (failRequest.compareAndSet(true, false)) {
                                throw new RestHandlerException(
                                        "expected", HttpResponseStatus.SERVICE_UNAVAILABLE);
                            } else {
                                return new SavepointInfo(expectedSavepointDir, null);
                            }
                        })) {

            final RestClusterClient<?> restClusterClient =
                    createRestClusterClient(restServerEndpoint.getServerAddress().getPort());

            final String savepointPath =
                    restClusterClient.triggerSavepoint(new JobID(), null).get();
            assertEquals(expectedSavepointDir, savepointPath);
        }
    }

    private static RestServerEndpoint createRestServerEndpoint(
            final FunctionWithException<
                            SavepointTriggerRequestBody, TriggerId, RestHandlerException>
                    triggerHandlerLogic,
            final FunctionWithException<TriggerId, SavepointInfo, RestHandlerException>
                    savepointHandlerLogic)
            throws Exception {
        return TestRestServerEndpoint.builder(REST_CONFIG)
                .withHandler(new TestSavepointTriggerHandler(triggerHandlerLogic))
                .withHandler(new TestSavepointHandler(savepointHandlerLogic))
                .buildAndStart();
    }

    private static final class TestSavepointTriggerHandler
            extends TestHandler<
                    SavepointTriggerRequestBody,
                    TriggerResponse,
                    SavepointTriggerMessageParameters> {

        private final FunctionWithException<
                        SavepointTriggerRequestBody, TriggerId, RestHandlerException>
                triggerHandlerLogic;

        TestSavepointTriggerHandler(
                final FunctionWithException<
                                SavepointTriggerRequestBody, TriggerId, RestHandlerException>
                        triggerHandlerLogic) {
            super(SavepointTriggerHeaders.getInstance());
            this.triggerHandlerLogic = triggerHandlerLogic;
        }

        @Override
        protected CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull
                        HandlerRequest<
                                        SavepointTriggerRequestBody,
                                        SavepointTriggerMessageParameters>
                                request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {

            return CompletableFuture.completedFuture(
                    new TriggerResponse(triggerHandlerLogic.apply(request.getRequestBody())));
        }
    }

    private static class TestSavepointHandler
            extends TestHandler<
                    EmptyRequestBody,
                    AsynchronousOperationResult<SavepointInfo>,
                    SavepointStatusMessageParameters> {

        private final FunctionWithException<TriggerId, SavepointInfo, RestHandlerException>
                savepointHandlerLogic;

        TestSavepointHandler(
                final FunctionWithException<TriggerId, SavepointInfo, RestHandlerException>
                        savepointHandlerLogic) {
            super(SavepointStatusHeaders.getInstance());
            this.savepointHandlerLogic = savepointHandlerLogic;
        }

        @Override
        protected CompletableFuture<AsynchronousOperationResult<SavepointInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody, SavepointStatusMessageParameters> request,
                @Nonnull DispatcherGateway gateway)
                throws RestHandlerException {

            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            return CompletableFuture.completedFuture(
                    AsynchronousOperationResult.completed(savepointHandlerLogic.apply(triggerId)));
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
        clientConfig.setInteger(RestOptions.PORT, port);
        return new RestClusterClient<>(
                clientConfig,
                new RestClient(REST_CONFIG, executor),
                StandaloneClusterId.getInstance(),
                (attempt) -> 0);
    }
}
