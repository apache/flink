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

package org.apache.flink.runtime.rest.handler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.FlinkHttpObjectAggregator;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.handler.router.RouteResult;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.util.TestMessageHeaders;
import org.apache.flink.runtime.rest.util.TestRestHandler;
import org.apache.flink.runtime.rest.util.TestRestServerEndpoint;
import org.apache.flink.runtime.rest.versioning.RuntimeRestAPIVersion;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ConfigurationException;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPipeline;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpStatusClass;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.util.Attribute;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nonnull;

import java.io.File;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link AbstractHandler}. */
class AbstractHandlerTest {

    private static final RestfulGateway mockRestfulGateway =
            TestingDispatcherGateway.newBuilder().build();

    private static final GatewayRetriever<RestfulGateway> mockGatewayRetriever =
            () -> CompletableFuture.completedFuture(mockRestfulGateway);

    private static final Configuration REST_BASE_CONFIG;

    static {
        final String loopbackAddress = InetAddress.getLoopbackAddress().getHostAddress();

        final Configuration config = new Configuration();
        config.set(RestOptions.BIND_PORT, "0");
        config.set(RestOptions.BIND_ADDRESS, loopbackAddress);
        config.set(RestOptions.ADDRESS, loopbackAddress);

        REST_BASE_CONFIG = config;
    }

    private RestClient createRestClient(int serverPort) throws ConfigurationException {
        Configuration config = new Configuration(REST_BASE_CONFIG);
        config.set(RestOptions.PORT, serverPort);

        return new RestClient(config, Executors.directExecutor());
    }

    @Test
    void testOOMErrorMessageEnrichment() throws Exception {
        final TestMessageHeaders<EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
                messageHeaders =
                        TestMessageHeaders.emptyBuilder()
                                .setTargetRestEndpointURL("/test-handler")
                                .build();

        final TestRestHandler<
                        RestfulGateway, EmptyRequestBody, EmptyResponseBody, EmptyMessageParameters>
                testRestHandler =
                        new TestRestHandler<>(
                                mockGatewayRetriever,
                                messageHeaders,
                                FutureUtils.completedExceptionally(
                                        new OutOfMemoryError("Metaspace")));

        try (final TestRestServerEndpoint server =
                        TestRestServerEndpoint.builder(REST_BASE_CONFIG)
                                .withHandler(messageHeaders, testRestHandler)
                                .buildAndStart();
                final RestClient restClient =
                        createRestClient(server.getServerAddress().getPort())) {
            CompletableFuture<EmptyResponseBody> response =
                    restClient.sendRequest(
                            server.getServerAddress().getHostName(),
                            server.getServerAddress().getPort(),
                            messageHeaders,
                            EmptyMessageParameters.getInstance(),
                            EmptyRequestBody.getInstance());

            assertThatThrownBy(response::get)
                    .as(
                            "An ExecutionException was expected here being caused by the OutOfMemoryError.")
                    .isInstanceOf(ExecutionException.class)
                    .hasMessageContaining(
                            "Metaspace. The metaspace out-of-memory error has occurred. ");
        }
    }

    @Test
    void testFileCleanup(@TempDir File temporaryFolder) throws Exception {
        final Path dir = temporaryFolder.toPath();
        final Path file = dir.resolve("file");
        Files.createFile(file);

        RestfulGateway mockRestfulGateway = new TestingRestfulGateway.Builder().build();

        final GatewayRetriever<RestfulGateway> mockGatewayRetriever =
                () -> CompletableFuture.completedFuture(mockRestfulGateway);

        CompletableFuture<Void> requestProcessingCompleteFuture = new CompletableFuture<>();
        TestHandler handler =
                new TestHandler(requestProcessingCompleteFuture, mockGatewayRetriever);

        RouteResult<?> routeResult =
                new RouteResult<>("", "", Collections.emptyMap(), Collections.emptyMap(), "");
        HttpRequest request =
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.GET,
                        TestHandler.TestHeaders.INSTANCE.getTargetRestEndpointURL(),
                        Unpooled.wrappedBuffer(new byte[0]));
        RoutedRequest<?> routerRequest = new RoutedRequest<>(routeResult, request);

        Attribute<FileUploads> attribute = new SimpleAttribute();
        attribute.set(new FileUploads(dir));
        Channel channel = mock(Channel.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(attribute);

        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.channel()).thenReturn(channel);

        handler.respondAsLeader(context, routerRequest, mockRestfulGateway);

        // the (asynchronous) request processing is not yet complete so the files should still exist
        assertThat(Files.exists(file)).isTrue();
        requestProcessingCompleteFuture.complete(null);
        assertThat(Files.exists(file)).isFalse();
    }

    @Test
    void testIgnoringUnknownFields() {
        RestfulGateway mockRestfulGateway = new TestingRestfulGateway.Builder().build();

        CompletableFuture<Void> requestProcessingCompleteFuture = new CompletableFuture<>();
        TestHandler handler =
                new TestHandler(requestProcessingCompleteFuture, mockGatewayRetriever);

        RouteResult<?> routeResult =
                new RouteResult<>("", "", Collections.emptyMap(), Collections.emptyMap(), "");
        String requestBody = "{\"unknown_field_should_be_ignore\": true}";
        HttpRequest request =
                new DefaultFullHttpRequest(
                        HttpVersion.HTTP_1_1,
                        HttpMethod.POST,
                        TestHandler.TestHeaders.INSTANCE.getTargetRestEndpointURL(),
                        Unpooled.wrappedBuffer(requestBody.getBytes(StandardCharsets.UTF_8)));
        RoutedRequest<?> routerRequest = new RoutedRequest<>(routeResult, request);

        AtomicReference<HttpResponse> response = new AtomicReference<>();

        FlinkHttpObjectAggregator aggregator =
                new FlinkHttpObjectAggregator(
                        RestOptions.SERVER_MAX_CONTENT_LENGTH.defaultValue(),
                        Collections.emptyMap());
        ChannelPipeline pipeline = mock(ChannelPipeline.class);
        when(pipeline.get(eq(FlinkHttpObjectAggregator.class))).thenReturn(aggregator);

        ChannelFuture succeededFuture = mock(ChannelFuture.class);
        when(succeededFuture.isSuccess()).thenReturn(true);

        Attribute<FileUploads> attribute = new SimpleAttribute();
        Channel channel = mock(Channel.class);
        when(channel.attr(any(AttributeKey.class))).thenReturn(attribute);

        ChannelHandlerContext context = mock(ChannelHandlerContext.class);
        when(context.pipeline()).thenReturn(pipeline);
        when(context.channel()).thenReturn(channel);
        when(context.write(any()))
                .thenAnswer(
                        invocation -> {
                            if (invocation.getArguments().length > 0
                                    && invocation.getArgument(0) instanceof HttpResponse) {
                                response.set(invocation.getArgument(0));
                            }
                            return succeededFuture;
                        });
        when(context.writeAndFlush(any())).thenReturn(succeededFuture);

        handler.respondAsLeader(context, routerRequest, mockRestfulGateway);
        assertThat(
                        response.get() == null
                                || response.get().status().codeClass() == HttpStatusClass.SUCCESS)
                .isTrue();
    }

    private static class SimpleAttribute implements Attribute<FileUploads> {

        private static final AttributeKey<FileUploads> KEY = AttributeKey.valueOf("test");

        private final AtomicReference<FileUploads> container = new AtomicReference<>();

        @Override
        public AttributeKey<FileUploads> key() {
            return KEY;
        }

        @Override
        public FileUploads get() {
            return container.get();
        }

        @Override
        public void set(FileUploads value) {
            container.set(value);
        }

        @Override
        public FileUploads getAndSet(FileUploads value) {
            return container.getAndSet(value);
        }

        @Override
        public FileUploads setIfAbsent(FileUploads value) {
            if (container.compareAndSet(null, value)) {
                return value;
            } else {
                return container.get();
            }
        }

        @Override
        public FileUploads getAndRemove() {
            return container.getAndSet(null);
        }

        @Override
        public boolean compareAndSet(FileUploads oldValue, FileUploads newValue) {
            return container.compareAndSet(oldValue, newValue);
        }

        @Override
        public void remove() {
            set(null);
        }
    }

    private static class TestHandler
            extends AbstractHandler<RestfulGateway, EmptyRequestBody, EmptyMessageParameters> {
        private final CompletableFuture<Void> completionFuture;

        protected TestHandler(
                CompletableFuture<Void> completionFuture,
                @Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever) {
            super(
                    leaderRetriever,
                    RpcUtils.INF_TIMEOUT,
                    Collections.emptyMap(),
                    TestHeaders.INSTANCE);
            this.completionFuture = completionFuture;
        }

        @Override
        protected CompletableFuture<Void> respondToRequest(
                ChannelHandlerContext ctx,
                HttpRequest httpRequest,
                HandlerRequest<EmptyRequestBody> handlerRequest,
                RestfulGateway gateway)
                throws RestHandlerException {
            return completionFuture;
        }

        private enum TestHeaders
                implements UntypedResponseMessageHeaders<EmptyRequestBody, EmptyMessageParameters> {
            INSTANCE;

            @Override
            public Class<EmptyRequestBody> getRequestClass() {
                return EmptyRequestBody.class;
            }

            @Override
            public EmptyMessageParameters getUnresolvedMessageParameters() {
                return EmptyMessageParameters.getInstance();
            }

            @Override
            public HttpMethodWrapper getHttpMethod() {
                return HttpMethodWrapper.POST;
            }

            @Override
            public String getTargetRestEndpointURL() {
                return "/test";
            }

            @Override
            public boolean acceptsFileUploads() {
                return true;
            }

            @Override
            public Collection<RuntimeRestAPIVersion> getSupportedAPIVersions() {
                return Collections.singleton(RuntimeRestAPIVersion.V1);
            }
        }
    }
}
