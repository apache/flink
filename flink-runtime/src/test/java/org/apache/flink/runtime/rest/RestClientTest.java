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

package org.apache.flink.runtime.rest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.CheckedSupplier;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ConnectTimeoutException;
import org.apache.flink.shaded.netty4.io.netty.channel.DefaultSelectStrategyFactory;
import org.apache.flink.shaded.netty4.io.netty.channel.SelectStrategy;
import org.apache.flink.shaded.netty4.io.netty.channel.SelectStrategyFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.THROWABLE;

/** Tests for {@link RestClient}. */
class RestClientTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static final String unroutableIp = "240.0.0.0";

    private static final long TIMEOUT = 10L;

    @Test
    void testConnectionTimeout() throws Exception {
        final Configuration config = new Configuration();
        config.setLong(RestOptions.CONNECTION_TIMEOUT, 1);
        try (final RestClient restClient = new RestClient(config, Executors.directExecutor())) {
            CompletableFuture<?> future =
                    restClient.sendRequest(
                            unroutableIp,
                            80,
                            new TestMessageHeaders(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance());

            FlinkAssertions.assertThatFuture(future)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(ConnectTimeoutException.class)
                    .extracting(Throwable::getCause, as(InstanceOfAssertFactories.THROWABLE))
                    .hasMessageContaining(unroutableIp);
        }
    }

    @Test
    void testInvalidVersionRejection() throws Exception {
        try (final RestClient restClient =
                new RestClient(new Configuration(), Executors.directExecutor())) {
            assertThatThrownBy(
                            () ->
                                    restClient.sendRequest(
                                            unroutableIp,
                                            80,
                                            new TestMessageHeaders(),
                                            EmptyMessageParameters.getInstance(),
                                            EmptyRequestBody.getInstance(),
                                            Collections.emptyList(),
                                            RuntimeRestAPIVersion.V0))
                    .as("The request should have been rejected due to a version mismatch.")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    /** Tests that we fail the operation if the remote connection closes. */
    @Test
    void testConnectionClosedHandling() throws Exception {
        final Configuration config = new Configuration();
        config.setLong(RestOptions.IDLENESS_TIMEOUT, 5000L);
        try (final ServerSocket serverSocket = new ServerSocket(0);
                final RestClient restClient =
                        new RestClient(config, EXECUTOR_EXTENSION.getExecutor())) {

            final String targetAddress = "localhost";
            final int targetPort = serverSocket.getLocalPort();

            // start server
            final CompletableFuture<Socket> socketCompletableFuture =
                    CompletableFuture.supplyAsync(
                            CheckedSupplier.unchecked(
                                    () -> NetUtils.acceptWithoutTimeout(serverSocket)));

            final CompletableFuture<EmptyResponseBody> responseFuture =
                    restClient.sendRequest(
                            targetAddress,
                            targetPort,
                            new TestMessageHeaders(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance(),
                            Collections.emptyList());

            Socket connectionSocket = null;

            try {
                connectionSocket = socketCompletableFuture.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (TimeoutException ignored) {
                // could not establish a server connection --> see that the response failed
                socketCompletableFuture.cancel(true);
            }

            if (connectionSocket != null) {
                // close connection
                connectionSocket.close();
            }

            FlinkAssertions.assertThatFuture(responseFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(IOException.class);
        }
    }

    /** Tests that we fail the operation if the client closes. */
    @Test
    void testRestClientClosedHandling() throws Exception {
        final Configuration config = new Configuration();
        config.setLong(RestOptions.IDLENESS_TIMEOUT, 5000L);

        Socket connectionSocket = null;

        try (final ServerSocket serverSocket = new ServerSocket(0);
                final RestClient restClient =
                        new RestClient(config, EXECUTOR_EXTENSION.getExecutor())) {

            final String targetAddress = "localhost";
            final int targetPort = serverSocket.getLocalPort();

            // start server
            final CompletableFuture<Socket> socketCompletableFuture =
                    CompletableFuture.supplyAsync(
                            CheckedSupplier.unchecked(
                                    () -> NetUtils.acceptWithoutTimeout(serverSocket)));

            final CompletableFuture<EmptyResponseBody> responseFuture =
                    restClient.sendRequest(
                            targetAddress,
                            targetPort,
                            new TestMessageHeaders(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance(),
                            Collections.emptyList());

            try {
                connectionSocket = socketCompletableFuture.get(TIMEOUT, TimeUnit.SECONDS);
            } catch (TimeoutException ignored) {
                // could not establish a server connection --> see that the response failed
                socketCompletableFuture.cancel(true);
            }

            restClient.close();

            FlinkAssertions.assertThatFuture(responseFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(IOException.class);
        } finally {
            if (connectionSocket != null) {
                connectionSocket.close();
            }
        }
    }

    /**
     * Tests that the futures returned by {@link RestClient} fail immediately if the client is
     * already closed.
     *
     * <p>See FLINK-32583
     */
    @Test
    void testCloseClientBeforeRequest() throws Exception {
        try (final RestClient restClient =
                new RestClient(new Configuration(), Executors.directExecutor())) {
            restClient.close(); // Intentionally close the client prior to the request

            CompletableFuture<?> future =
                    restClient.sendRequest(
                            unroutableIp,
                            80,
                            new TestMessageHeaders(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance());

            FlinkAssertions.assertThatFuture(future)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(IllegalStateException.class)
                    .extracting(Throwable::getCause, as(THROWABLE))
                    .hasMessage("RestClient is already closed");
        }
    }

    @Test
    void testCloseClientWhileProcessingRequest() throws Exception {
        // Set up a Netty SelectStrategy with latches that allow us to step forward through Netty's
        // request state machine, closing the client at a particular moment
        final OneShotLatch connectTriggered = new OneShotLatch();
        final OneShotLatch closeTriggered = new OneShotLatch();
        final SelectStrategy fallbackSelectStrategy =
                DefaultSelectStrategyFactory.INSTANCE.newSelectStrategy();
        final SelectStrategyFactory selectStrategyFactory =
                () ->
                        (selectSupplier, hasTasks) -> {
                            connectTriggered.trigger();
                            closeTriggered.awaitQuietly();

                            return fallbackSelectStrategy.calculateStrategy(
                                    selectSupplier, hasTasks);
                        };

        try (final RestClient restClient =
                new RestClient(
                        new Configuration(), Executors.directExecutor(), selectStrategyFactory)) {
            // Check that client's internal collection of pending response futures is empty prior to
            // the request
            assertThat(restClient.getResponseChannelFutures()).isEmpty();

            final CompletableFuture<?> requestFuture =
                    restClient.sendRequest(
                            unroutableIp,
                            80,
                            new TestMessageHeaders(),
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance());

            // Check that client's internal collection of pending response futures now has one
            // entry, presumably due to the call to sendRequest
            assertThat(restClient.getResponseChannelFutures()).hasSize(1);

            // Wait for Netty to start connecting, then while it's paused in the SelectStrategy,
            // close the client before unpausing Netty
            connectTriggered.await();
            final CompletableFuture<Void> closeFuture = restClient.closeAsync();
            closeTriggered.trigger();

            FlinkAssertions.assertThatFuture(closeFuture)
                    .as("Close should have had completed.")
                    .eventuallySucceeds();

            FlinkAssertions.assertThatFuture(requestFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(IllegalStateException.class)
                    .extracting(Throwable::getCause, as(THROWABLE))
                    .hasMessage("executor not accepting a task");
        }
    }

    @Test
    void testResponseChannelFuturesResolvedExceptionallyOnClose() throws Exception {
        try (final RestClient restClient =
                new RestClient(new Configuration(), Executors.directExecutor())) {
            CompletableFuture<Channel> responseChannelFuture = new CompletableFuture<>();

            // Add the future to the client's internal collection of pending response futures
            restClient.getResponseChannelFutures().add(responseChannelFuture);

            // Close the client, which should resolve all pending response futures exceptionally and
            // clear the collection
            restClient.close();

            // Ensure the client's internal collection of pending response futures was cleared after
            // close
            assertThat(restClient.getResponseChannelFutures()).isEmpty();

            FlinkAssertions.assertThatFuture(responseChannelFuture)
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(IllegalStateException.class)
                    .extracting(Throwable::getCause, as(THROWABLE))
                    .hasMessage("RestClient closed before request completed");
        }
    }

    private static class TestMessageHeaders
            implements RuntimeMessageHeaders<
                    EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters> {

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public Class<EmptyResponseBody> getResponseClass() {
            return EmptyResponseBody.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return "";
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/";
        }
    }
}
