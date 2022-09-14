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

package org.apache.flink.table.gateway.rest;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestServerEndpoint;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rpc.exceptions.EndpointNotStartedException;
import org.apache.flink.table.gateway.api.SqlGatewayService;
import org.apache.flink.table.gateway.rest.handler.AbstractSqlGatewayRestHandler;
import org.apache.flink.table.gateway.rest.header.SqlGatewayMessageHeaders;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestAPIVersion;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.table.gateway.rest.util.RestConfigUtils.getBaseConfig;
import static org.apache.flink.table.gateway.rest.util.RestConfigUtils.getFlinkConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link SqlGatewayRestEndpoint}. */
class SqlGatewayRestEndpointITCase {

    private static final SqlGatewayService service = null;

    private static RestServerEndpoint serverEndpoint;
    private static RestClient restClient;
    private static InetSocketAddress serverAddress;

    private static TestBadCaseHandler testHandler;
    private static TestVersionSelectionHeaders1 header1;
    private static TestVersionSelectionHeaders2 header2;
    private static TestBadCaseHeaders badCaseHeader;
    private static TestVersionHandler testVersionHandler1;
    private static TestVersionHandler testVersionHandler2;

    private static Configuration config;
    private static final Time timeout = Time.seconds(10L);

    @BeforeEach
    void setup() throws Exception {
        // Test version cases
        header1 = new TestVersionSelectionHeaders1();
        header2 = new TestVersionSelectionHeaders2();
        testVersionHandler1 = new TestVersionHandler(service, header1);
        testVersionHandler2 = new TestVersionHandler(service, header2);

        // Test exception cases
        badCaseHeader = new TestBadCaseHeaders();
        testHandler = new TestBadCaseHandler(service);

        // Init
        final String address = InetAddress.getLoopbackAddress().getHostAddress();
        config = getBaseConfig(getFlinkConfig(address, address, "0"));
        serverEndpoint =
                TestingSqlGatewayRestEndpoint.builder(config, service)
                        .withHandler(badCaseHeader, testHandler)
                        .withHandler(header1, testVersionHandler1)
                        .withHandler(header2, testVersionHandler2)
                        .buildAndStart();

        restClient =
                new RestClient(
                        config,
                        Executors.newFixedThreadPool(
                                1, new ExecutorThreadFactory("rest-client-thread-pool")));
        serverAddress = serverEndpoint.getServerAddress();
    }

    @AfterEach
    void stop() throws Exception {

        if (restClient != null) {
            restClient.shutdown(timeout);
            restClient = null;
        }

        if (serverEndpoint != null) {
            serverEndpoint.closeAsync().get(timeout.getSize(), timeout.getUnit());
            serverEndpoint = null;
        }
    }

    /** Test that {@link SqlGatewayMessageHeaders} can identify the version correctly. */
    @Test
    void testSqlGatewayMessageHeaders() throws Exception {
        // The header only support V1, but send request by V0
        assertThatThrownBy(
                        () ->
                                restClient.sendRequest(
                                        serverAddress.getHostName(),
                                        serverAddress.getPort(),
                                        header2,
                                        EmptyMessageParameters.getInstance(),
                                        EmptyRequestBody.getInstance(),
                                        Collections.emptyList(),
                                        SqlGatewayRestAPIVersion.V0))
                .isInstanceOf(IllegalArgumentException.class);

        // The header only support V1, send request by V1
        CompletableFuture<TestResponse> specifiedVersionResponse =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        header2,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        SqlGatewayRestAPIVersion.V1);

        TestResponse testResponse1 = specifiedVersionResponse.get(5, TimeUnit.SECONDS);
        assertThat(testResponse1.getStatus()).isEqualTo("V1");

        // The header only support V1, send request by latest version V1
        CompletableFuture<TestResponse> unspecifiedVersionResponse =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        header2,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList());

        TestResponse testResponse2 = unspecifiedVersionResponse.get(5, TimeUnit.SECONDS);
        assertThat(testResponse2.getStatus()).isEqualTo("V1");
    }

    /** Test that requests of different version are routed to correct handlers. */
    @Test
    void testVersionSelection() throws Exception {
        CompletableFuture<TestResponse> version1Response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        header1,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        SqlGatewayRestAPIVersion.V0);

        TestResponse testResponse = version1Response.get(5, TimeUnit.SECONDS);
        assertThat(testResponse.getStatus()).isEqualTo("V0");

        CompletableFuture<TestResponse> version2Response =
                restClient.sendRequest(
                        serverAddress.getHostName(),
                        serverAddress.getPort(),
                        header2,
                        EmptyMessageParameters.getInstance(),
                        EmptyRequestBody.getInstance(),
                        Collections.emptyList(),
                        SqlGatewayRestAPIVersion.V1);
        TestResponse testResponse2 = version2Response.get(5, TimeUnit.SECONDS);
        assertThat(testResponse2.getStatus()).isEqualTo("V1");
    }

    /**
     * Test that {@link AbstractSqlGatewayRestHandler} will use the default endpoint version when
     * the url does not contain version.
     */
    @Test
    void testDefaultVersionRouting() throws Exception {
        assertThat(config.getBoolean(SecurityOptions.SSL_REST_ENABLED)).isFalse();

        OkHttpClient client = new OkHttpClient();
        final Request request =
                new Request.Builder()
                        .url(serverEndpoint.getRestBaseUrl() + header1.getTargetRestEndpointURL())
                        .build();

        final Response response = client.newCall(request).execute();
        assert response.body() != null;
        assertThat(response.body().string()).contains("V1");
    }

    /**
     * Tests that request are handled as individual units which don't interfere with each other.
     * This means that request responses can overtake each other.
     */
    @Test
    void testRequestInterleaving() throws Exception {
        final BlockerSync sync = new BlockerSync();
        testHandler.handlerBody =
                id -> {
                    if (id == 1) {
                        try {
                            sync.block();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return CompletableFuture.completedFuture(new TestResponse(id.toString()));
                };

        // send first request and wait until the handler blocks
        final CompletableFuture<TestResponse> response1 =
                sendRequestToTestHandler(new TestRequest(1));
        sync.awaitBlocker();

        // send second request and verify response
        final CompletableFuture<TestResponse> response2 =
                sendRequestToTestHandler(new TestRequest(2));
        assertThat(response2.get().status).isEqualTo("2");

        // wake up blocked handler
        sync.releaseBlocker();

        // verify response to first request
        assertThat(response1.get().status).isEqualTo("1");
    }

    @Test
    void testDuplicateHandlerRegistrationIsForbidden() {
        assertThatThrownBy(
                        () -> {
                            try (TestingSqlGatewayRestEndpoint restServerEndpoint =
                                    TestingSqlGatewayRestEndpoint.builder(config, service)
                                            .withHandler(header1, testHandler)
                                            .withHandler(badCaseHeader, testHandler)
                                            .build()) {
                                restServerEndpoint.start();
                            }
                        })
                .isInstanceOf(FlinkRuntimeException.class);
    }

    @Test
    void testEndpointsMustBeUnique() {
        assertThatThrownBy(
                        () -> {
                            try (TestingSqlGatewayRestEndpoint restServerEndpoint =
                                    TestingSqlGatewayRestEndpoint.builder(config, service)
                                            .withHandler(badCaseHeader, testHandler)
                                            .withHandler(badCaseHeader, testVersionHandler1)
                                            .build()) {
                                restServerEndpoint.start();
                            }
                        })
                .isInstanceOf(FlinkRuntimeException.class);
    }

    /**
     * Tests that after calling {@link SqlGatewayRestEndpoint#closeAsync()}, the handlers are closed
     * first, and we wait for in-flight requests to finish. As long as not all handlers are closed,
     * HTTP requests should be served.
     */
    @Test
    void testShouldWaitForHandlersWhenClosing() throws Exception {
        testHandler.closeFuture = new CompletableFuture<>();
        final BlockerSync sync = new BlockerSync();
        testHandler.handlerBody =
                id -> {
                    // Intentionally schedule the work on a different thread. This is to simulate
                    // handlers where the CompletableFuture is finished by the RPC framework.
                    return CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    sync.block();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return new TestResponse(id.toString());
                            });
                };

        // Initiate closing RestServerEndpoint but the test handler should block.
        final CompletableFuture<Void> closeRestServerEndpointFuture = serverEndpoint.closeAsync();
        assertThat(closeRestServerEndpointFuture).isNotDone();

        // create an in-flight request
        final CompletableFuture<TestResponse> request =
                sendRequestToTestHandler(new TestRequest(1));
        sync.awaitBlocker();

        // Allow handler to close but there is still one in-flight request which should prevent
        // the RestServerEndpoint from closing.
        testHandler.closeFuture.complete(null);
        assertThat(closeRestServerEndpointFuture).isNotDone();

        // Finish the in-flight request.
        sync.releaseBlocker();

        request.get(timeout.getSize(), timeout.getUnit());
        closeRestServerEndpointFuture.get(timeout.getSize(), timeout.getUnit());
    }

    @Test
    void testRestServerBindPort() throws Exception {
        final int portRangeStart = 52300;
        final int portRangeEnd = 52400;
        final String address = InetAddress.getLoopbackAddress().getHostAddress();
        final Configuration sqlGatewayRestEndpointConfig =
                getBaseConfig(
                        getFlinkConfig(address, address, portRangeStart + "-" + portRangeEnd));

        try (RestServerEndpoint serverEndpoint1 =
                        TestingSqlGatewayRestEndpoint.builder(sqlGatewayRestEndpointConfig, service)
                                .build();
                RestServerEndpoint serverEndpoint2 =
                        TestingSqlGatewayRestEndpoint.builder(sqlGatewayRestEndpointConfig, service)
                                .build()) {

            serverEndpoint1.start();
            serverEndpoint2.start();

            assertThat(Objects.requireNonNull(serverEndpoint1.getServerAddress()).getPort())
                    .isNotEqualTo(
                            Objects.requireNonNull(serverEndpoint2.getServerAddress()).getPort());

            assertThat(serverEndpoint1.getServerAddress().getPort())
                    .isGreaterThanOrEqualTo(portRangeStart);
            assertThat(serverEndpoint1.getServerAddress().getPort())
                    .isLessThanOrEqualTo(portRangeEnd);
            assertThat(serverEndpoint2.getServerAddress().getPort())
                    .isGreaterThanOrEqualTo(portRangeStart);
            assertThat(serverEndpoint2.getServerAddress().getPort())
                    .isLessThanOrEqualTo(portRangeEnd);
        }
    }

    @Test
    void testOnUnavailableRpcEndpointReturns503() {
        CompletableFuture<TestResponse> response = sendRequestToTestHandler(new TestRequest(3));

        assertThatThrownBy(response::get)
                .extracting(x -> ExceptionUtils.findThrowable(x, RestClientException.class))
                .extracting(Optional::get)
                .extracting(RestClientException::getHttpResponseStatus)
                .isEqualTo(HttpResponseStatus.SERVICE_UNAVAILABLE);
    }

    private static class TestBadCaseHandler
            extends AbstractSqlGatewayRestHandler<
                    TestRequest, TestResponse, EmptyMessageParameters> {

        private final OneShotLatch closeLatch = new OneShotLatch();

        private CompletableFuture<Void> closeFuture = CompletableFuture.completedFuture(null);

        private Function<Integer, CompletableFuture<TestResponse>> handlerBody;

        TestBadCaseHandler(SqlGatewayService sqlGatewayService) {
            super(sqlGatewayService, Collections.emptyMap(), badCaseHeader);
        }

        @Override
        public CompletableFuture<Void> closeHandlerAsync() {
            closeLatch.trigger();
            return closeFuture;
        }

        @Override
        protected CompletableFuture<TestResponse> handleRequest(
                @Nullable SqlGatewayRestAPIVersion version,
                @NotNull HandlerRequest<TestRequest> request) {
            final int id = request.getRequestBody().id;
            if (id == 3) {
                return FutureUtils.completedExceptionally(
                        new EndpointNotStartedException("test exception"));
            }
            return handlerBody.apply(id);
        }
    }

    private CompletableFuture<TestResponse> sendRequestToTestHandler(
            final TestRequest testRequest) {
        try {
            return restClient.sendRequest(
                    serverAddress.getHostName(),
                    serverAddress.getPort(),
                    badCaseHeader,
                    EmptyMessageParameters.getInstance(),
                    testRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestRequest implements RequestBody {
        public final int id;

        @JsonCreator
        public TestRequest(@JsonProperty("id") int id) {
            this.id = id;
        }
    }

    private static class TestResponse implements ResponseBody {

        public final String status;

        @JsonCreator
        public TestResponse(@JsonProperty("status") String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }
    }

    private static class TestBadCaseHeaders
            implements SqlGatewayMessageHeaders<TestRequest, TestResponse, EmptyMessageParameters> {

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/";
        }

        @Override
        public Class<TestRequest> getRequestClass() {
            return TestRequest.class;
        }

        @Override
        public Class<TestResponse> getResponseClass() {
            return TestResponse.class;
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
    }

    private static class TestVersionSelectionHeadersBase
            implements SqlGatewayMessageHeaders<
                    EmptyRequestBody, TestResponse, EmptyMessageParameters> {

        @Override
        public Class<EmptyRequestBody> getRequestClass() {
            return EmptyRequestBody.class;
        }

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.GET;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return "/test/select-version";
        }

        @Override
        public Class<TestResponse> getResponseClass() {
            return TestResponse.class;
        }

        @Override
        public HttpResponseStatus getResponseStatusCode() {
            return HttpResponseStatus.OK;
        }

        @Override
        public String getDescription() {
            return null;
        }

        @Override
        public EmptyMessageParameters getUnresolvedMessageParameters() {
            return EmptyMessageParameters.getInstance();
        }
    }

    private static class TestVersionSelectionHeaders1 extends TestVersionSelectionHeadersBase {
        @Override
        public Collection<SqlGatewayRestAPIVersion> getSupportedAPIVersions() {
            return Collections.singleton(SqlGatewayRestAPIVersion.V0);
        }
    }

    private static class TestVersionSelectionHeaders2 extends TestVersionSelectionHeadersBase {
        @Override
        public Collection<SqlGatewayRestAPIVersion> getSupportedAPIVersions() {
            return Collections.singleton(SqlGatewayRestAPIVersion.V1);
        }
    }

    private static class TestVersionHandler
            extends AbstractSqlGatewayRestHandler<
                    EmptyRequestBody, TestResponse, EmptyMessageParameters> {

        TestVersionHandler(
                final SqlGatewayService sqlGatewayService, TestVersionSelectionHeadersBase header) {
            super(sqlGatewayService, Collections.emptyMap(), header);
        }

        @Override
        protected CompletableFuture<TestResponse> handleRequest(
                @Nullable SqlGatewayRestAPIVersion version,
                @NotNull HandlerRequest<EmptyRequestBody> request) {
            assert version != null;
            return CompletableFuture.completedFuture(new TestResponse(version.name()));
        }
    }
}
