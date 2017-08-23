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
import org.apache.flink.runtime.rest.handler.PipelineErrorHandler;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
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

/**
 * This client is the counter-part to the {@link RestServerEndpoint}.
 */
public class RestClientEndpoint {
	private static final Logger LOG = LoggerFactory.getLogger(RestClientEndpoint.class);

	private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

	private final String configuredTargetAddress;
	private final int configuredTargetPort;

	private Bootstrap bootstrap;

	public RestClientEndpoint(RestClientEndpointConfiguration configuration) {
		Preconditions.checkNotNull(configuration);
		this.configuredTargetAddress = configuration.getTargetRestEndpointAddress();
		this.configuredTargetPort = configuration.getTargetRestEndpointPort();

		SSLEngine sslEngine = configuration.getSslEngine();
		ChannelInitializer initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				// SSL should be the first handler in the pipeline
				if (sslEngine != null) {
					ch.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				ch.pipeline()
					.addLast(new HttpClientCodec())
					.addLast(new HttpObjectAggregator(1024 * 1024))
					.addLast(new ClientHandler())
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

	public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(M messageHeaders, U messageParameters, R request) throws IOException {
		Preconditions.checkNotNull(messageHeaders);
		Preconditions.checkNotNull(request);
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkState(messageParameters.isResolved(), "Message parameters were not resolved.");

		String targetUrl = MessageParameters.resolveUrl(messageHeaders.getTargetRestEndpointURL(), messageParameters);

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

		return submitRequest(httpRequest, messageHeaders.getResponseClass());
	}

	private <P extends ResponseBody> CompletableFuture<P> submitRequest(FullHttpRequest httpRequest, Class<P> responseClass) {
		return CompletableFuture.supplyAsync(() -> bootstrap.connect(configuredTargetAddress, configuredTargetPort))
			.thenApply((channel) -> {
				try {
					return channel.sync();
				} catch (InterruptedException e) {
					throw new FlinkRuntimeException(e);
				}
			})
			.thenApply((ChannelFuture::channel))
			.thenCompose((channel -> {
				ClientHandler handler = channel.pipeline().get(ClientHandler.class);
				CompletableFuture<P> future = handler.setExpectedResponse(responseClass);
				channel.writeAndFlush(httpRequest);
				return future;
			}));
	}

	private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

		private ExpectedResponse<? extends ResponseBody> expectedResponse = null;

		<P extends ResponseBody> CompletableFuture<P> setExpectedResponse(Class<P> expectedResponse) {
			CompletableFuture<P> responseFuture = new CompletableFuture<>();
			this.expectedResponse = new ExpectedResponse<>(expectedResponse, responseFuture);

			return responseFuture;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (msg instanceof FullHttpResponse) {
				completeFuture((FullHttpResponse) msg);
			} else {
				LOG.error("Implementation error: Received a response that wasn't a FullHttpResponse.");
				expectedResponse.responseFuture.completeExceptionally(new RestClientException("Implementation error: Received a response that wasn't a FullHttpResponse."));
			}
		}

		private <P extends ResponseBody> void completeFuture(FullHttpResponse msg) throws IOException {
			ByteBuf content = msg.content();

			JsonNode rawResponse;
			try {
				InputStream in = new ByteBufInputStream(content);
				rawResponse = objectMapper.readTree(in);
				LOG.debug("Received response {}.", rawResponse);
			} catch (JsonMappingException | JsonParseException je) {
				LOG.error("Response was not valid JSON.", je);
				expectedResponse.responseFuture.completeExceptionally(new RestClientException("Response was not valid JSON.", je));
				return;
			}

			if (expectedResponse == null) {
				LOG.error("Received response, but we didn't expect any. Response={}", rawResponse);
				return;
			}

			try {
				ExpectedResponse<P> expectedResponse = (ExpectedResponse<P>) this.expectedResponse;
				P response = objectMapper.treeToValue(rawResponse, expectedResponse.responseClass);
				expectedResponse.responseFuture.complete(response);
			} catch (JsonProcessingException jpe) {
				// the received response did not matched the expected response type

				// lets see if it is an ErrorResponse instead
				try {
					ErrorResponseBody error = objectMapper.treeToValue(rawResponse, ErrorResponseBody.class);
					expectedResponse.responseFuture.completeExceptionally(new RestClientException(error.errors.toString()));
				} catch (JsonMappingException jme) {
					// if this fails it is either the expected type or response type was wrong, most likely caused
					// by a client/search MessageHeaders mismatch
					LOG.error("Received response was neither of the expected type ({}) nor an error. Response={}", expectedResponse.responseClass, rawResponse, jme);
					expectedResponse.responseFuture.completeExceptionally(new RestClientException("Response was neither of the expected type(" + expectedResponse.responseClass + ") nor an error.", jme));
				}
			}
		}
	}

	private static class ExpectedResponse<P extends ResponseBody> {
		final Class<P> responseClass;
		final CompletableFuture<P> responseFuture;

		private ExpectedResponse(Class<P> extectedResponse, CompletableFuture<P> responseFuture) {
			this.responseClass = extectedResponse;
			this.responseFuture = responseFuture;
		}
	}
}
