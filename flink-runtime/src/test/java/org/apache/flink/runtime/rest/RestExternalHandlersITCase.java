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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.io.network.netty.InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.OutboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio0InboundChannelHandlerFactory;
import org.apache.flink.runtime.io.network.netty.Prio1InboundChannelHandlerFactory;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.RuntimeMessageHeaders;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.ContextClassLoaderExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** IT cases for {@link RestClient} and {@link RestServerEndpoint}. */
@ExtendWith(TestLoggerExtension.class)
class RestExternalHandlersITCase {

    private static final Time timeout = Time.seconds(10L);
    private static final String REQUEST_URL = "/nonExisting1";
    private static final String REDIRECT1_URL = "/nonExisting2";
    private static final String REDIRECT2_URL = "/nonExisting3";

    private RestServerEndpoint serverEndpoint;
    private RestClient restClient;
    private InetSocketAddress serverAddress;

    @RegisterExtension
    static final Extension CONTEXT_CLASS_LOADER_EXTENSION =
            ContextClassLoaderExtension.builder()
                    .withServiceEntry(
                            InboundChannelHandlerFactory.class,
                            Prio0InboundChannelHandlerFactory.class.getCanonicalName(),
                            Prio1InboundChannelHandlerFactory.class.getCanonicalName())
                    .withServiceEntry(
                            OutboundChannelHandlerFactory.class,
                            Prio0OutboundChannelHandlerFactory.class.getCanonicalName(),
                            Prio1OutboundChannelHandlerFactory.class.getCanonicalName())
                    .build();

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private final Configuration config;

    public RestExternalHandlersITCase() {
        this.config = getBaseConfig();
    }

    private static Configuration getBaseConfig() {
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "0");
        config.setString(RestOptions.BIND_ADDRESS, loopbackAddress);
        config.setString(RestOptions.ADDRESS, loopbackAddress);
        config.setString(Prio0OutboundChannelHandlerFactory.REDIRECT_TO_URL, REDIRECT1_URL);
        config.setString(Prio0InboundChannelHandlerFactory.REDIRECT_FROM_URL, REDIRECT1_URL);
        config.setString(Prio0InboundChannelHandlerFactory.REDIRECT_TO_URL, REDIRECT2_URL);
        return config;
    }

    @BeforeEach
    void setup() throws Exception {
        serverEndpoint = TestRestServerEndpoint.builder(config).buildAndStart();
        restClient = new RestClient(config, EXECUTOR_RESOURCE.getExecutor());
        serverAddress = serverEndpoint.getServerAddress();
    }

    @AfterEach
    void teardown() throws Exception {
        if (restClient != null) {
            restClient.shutdown(timeout);
            restClient = null;
        }

        if (serverEndpoint != null) {
            serverEndpoint.closeAsync().get(timeout.getSize(), timeout.getUnit());
            serverEndpoint = null;
        }
    }

    @Test
    void testHandlersMustBeLoaded() {
        final List<InboundChannelHandlerFactory> inboundChannelHandlerFactories =
                serverEndpoint.getInboundChannelHandlerFactories();
        assertEquals(inboundChannelHandlerFactories.size(), 2);
        assertTrue(
                inboundChannelHandlerFactories.get(0) instanceof Prio1InboundChannelHandlerFactory);
        assertTrue(
                inboundChannelHandlerFactories.get(1) instanceof Prio0InboundChannelHandlerFactory);

        final List<OutboundChannelHandlerFactory> outboundChannelHandlerFactories =
                restClient.getOutboundChannelHandlerFactories();
        assertEquals(outboundChannelHandlerFactories.size(), 2);
        assertTrue(
                outboundChannelHandlerFactories.get(0)
                        instanceof Prio1OutboundChannelHandlerFactory);
        assertTrue(
                outboundChannelHandlerFactories.get(1)
                        instanceof Prio0OutboundChannelHandlerFactory);

        try {
            final CompletableFuture<TestResponse> response =
                    sendRequestToTestHandler(new TestRequest());
            response.get();
            fail("Request must fail with 2 times redirected URL");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(REDIRECT2_URL));
        }
    }

    private CompletableFuture<TestResponse> sendRequestToTestHandler(
            final TestRequest testRequest) {
        try {
            return restClient.sendRequest(
                    serverAddress.getHostName(),
                    serverAddress.getPort(),
                    new TestHeaders(),
                    EmptyMessageParameters.getInstance(),
                    testRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestRequest implements RequestBody {}

    private static class TestResponse implements ResponseBody {}

    private static class TestHeaders
            implements RuntimeMessageHeaders<TestRequest, TestResponse, EmptyMessageParameters> {

        @Override
        public HttpMethodWrapper getHttpMethod() {
            return HttpMethodWrapper.POST;
        }

        @Override
        public String getTargetRestEndpointURL() {
            return REQUEST_URL;
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
}
