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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ParameterMapper;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * This client is the counter-part to the {@link RestServerEndpoint}.
 */
public class RestClientEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RestClientEndpoint.class);

	private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

	private final String configuredTargetAddress;
	private final int configuredTargetPort;
	private final SSLEngine sslEngine;

	private Bootstrap bootstrap;

	private final ClientHandler handler = new ClientHandler();

	private CompletableFuture<?> lastFuture = CompletableFuture.completedFuture(null);

	private final Executor directExecutor = Executors.directExecutor();

	public RestClientEndpoint(RestClientEndpointConfiguration configuration) {
		this.configuredTargetAddress = configuration.getTargetRestEndpointAddress();
		this.configuredTargetPort = configuration.getTargetRestEndpointPort();
		this.sslEngine = configuration.getSslEngine();
	}

	public void start() {
		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) {

				// SSL should be the first handler in the pipeline
				if (sslEngine != null) {
					ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				ch.pipeline()
					.addLast(new HttpClientCodec())
					.addLast(new HttpObjectAggregator(1024 * 1024))
					.addLast(handler)
					.addLast(new PipelineErrorHandler(LOG));
			}
		};

		NioEventLoopGroup group = new NioEventLoopGroup(1);

		bootstrap = new Bootstrap();
		bootstrap
			.group(group)
			.channel(NioSocketChannel.class)
			.handler(initializer);

		LOG.info("Rest client endpoint started.");
	}

	public void shutdown() {
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully();
			}
		}
	}

	public <M extends MessageHeaders<R, P, U>, U extends ParameterMapper, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(M messageHeaders, U urlResolver, R request) throws IOException {
		Preconditions.checkNotNull(messageHeaders);
		Preconditions.checkNotNull(request);

		String targetUrl = ParameterMapper.resolveUrl(
			messageHeaders.getTargetRestEndpointURL(),
			urlResolver.mapPathParameters(messageHeaders.getPathParameters()),
			urlResolver.mapQueryParameters(messageHeaders.getQueryParameters())
		);

		LOG.debug("Sending request of class {} to {}", request.getClass(), targetUrl);
		// serialize payload
		StringWriter sw = new StringWriter();
		objectMapper.writeValue(sw, request);
		ByteBuf payload = Unpooled.wrappedBuffer(sw.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

		// create request and set headers
		FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, messageHeaders.getHttpMethod().getNettyHttpMethod(), targetUrl, payload);
		httpRequest.headers()
			.add(HttpHeaders.Names.CONTENT_LENGTH, payload.capacity())
			.add(HttpHeaders.Names.CONTENT_TYPE, "application/json; charset=" + ConfigConstants.DEFAULT_CHARSET.name())
			.set(HttpHeaders.Names.HOST, configuredTargetAddress + ":" + configuredTargetPort)
			.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

		synchronized (this) {
			// This ensures strict sequential processing of requests.
			// If we send new requests immediately we can no longer make assumptions about the order in which responses
			// arrive, due to which the handler cannot know which future he should complete (not to mention what response
			// type to read).
			CompletableFuture<P> nextFuture = lastFuture
				.handleAsync((f, e) -> submitRequest(httpRequest, messageHeaders), directExecutor)
				.thenCompose((future) -> future);

			lastFuture = nextFuture;
			return nextFuture;
		}
	}

	private <M extends MessageHeaders<R, P, U>, U extends ParameterMapper, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> submitRequest(FullHttpRequest httpRequest, M messageHeaders) {
		CompletableFuture<P> responseFuture = handler.expectResponse(messageHeaders.getResponseClass());

		try {
			// write request
			Channel channel = bootstrap.connect(configuredTargetAddress, configuredTargetPort).sync().channel();
			channel.writeAndFlush(httpRequest);
			channel.closeFuture();
		} catch (InterruptedException e) {
			return FutureUtils.completedExceptionally(e);
		}
		return responseFuture;
	}

	@ChannelHandler.Sharable
	private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

		private volatile ExpectedResponse<? extends ResponseBody> expectedResponse = null;

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (msg instanceof FullHttpResponse) {
				completeFuture((FullHttpResponse) msg);
			} else {
				LOG.error("Implementation error: Received a response that wasn't a FullHttpResponse.");
			}
		}

		private <J extends ResponseBody> void completeFuture(FullHttpResponse msg) throws IOException {
			ByteBuf content = msg.content();

			JsonNode rawResponse;
			try {
				InputStream in = new ByteBufInputStream(content);
				rawResponse = objectMapper.readTree(in);
				LOG.debug("Received response {}.", rawResponse);
			} catch (JsonMappingException | JsonParseException je) {
				LOG.error("Response was not valid JSON.", je);
				return;
			}

			final ExpectedResponse<J> expectedResponse = (ExpectedResponse<J>) this.expectedResponse;
			this.expectedResponse = null;

			if (expectedResponse == null) {
				LOG.error("Received response, but we didn't expect any. Response={}", rawResponse);
				return;
			}

			try {
				try {
					J response = objectMapper.treeToValue(rawResponse, expectedResponse.responseClass);
					expectedResponse.responseFuture.complete(response);
				} catch (JsonProcessingException jpe) {
					// the received response did not matched the expected response type

					// lets see if it is an ErrorResponse instead
					try {
						ErrorResponseBody error = objectMapper.treeToValue(rawResponse, ErrorResponseBody.class);
						expectedResponse.responseFuture.completeExceptionally(new RuntimeException(error.errors.toString()));
					} catch (JsonMappingException jme) {
						// if this fails it is either the expected type or response type was wrong, most likely caused
						// by a client/search MessageHeaders mismatch
						LOG.error("Received response was neither of the expected type ({}) nor an error. Response={}", expectedResponse.responseClass, rawResponse, jme);
						expectedResponse.responseFuture.completeExceptionally(new RuntimeException("Response was neither of the expected type(" + expectedResponse.responseClass + ") nor an error.", jme));
					}
				}
			} catch (Exception e) {
				LOG.error("Critical error while handling response {}.", rawResponse, e);
				expectedResponse.responseFuture.completeExceptionally(e);
			}
		}

		<T extends ResponseBody> CompletableFuture<T> expectResponse(Class<T> jsonResponse) {
			LOG.debug("Expected response of type {}.", jsonResponse);
			CompletableFuture<T> responseFuture = new CompletableFuture<>();
			expectedResponse = new ExpectedResponse<>(jsonResponse, responseFuture);

			return responseFuture;
		}

		private static class ExpectedResponse<J extends ResponseBody> {
			final Class<J> responseClass;
			final CompletableFuture<J> responseFuture;

			private ExpectedResponse(Class<J> extectedResponse, CompletableFuture<J> responseFuture) {
				this.responseClass = extectedResponse;
				this.responseFuture = responseFuture;
			}
		}
	}
}
