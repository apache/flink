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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.util.RestConstants;
import org.apache.flink.runtime.rest.util.RestMapperUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JavaType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufInputStream;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.TooLongFrameException;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpClientCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObjectAggregator;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.DefaultThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLEngine;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * This client is the counter-part to the {@link RestServerEndpoint}.
 */
public class RestClient {
	private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

	private static final ObjectMapper objectMapper = RestMapperUtils.getStrictObjectMapper();

	// used to open connections to a rest server endpoint
	private final Executor executor;

	private Bootstrap bootstrap;

	public RestClient(RestClientConfiguration configuration, Executor executor) {
		Preconditions.checkNotNull(configuration);
		this.executor = Preconditions.checkNotNull(executor);

		SSLEngine sslEngine = configuration.getSslEngine();
		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel socketChannel) throws Exception {
				// SSL should be the first handler in the pipeline
				if (sslEngine != null) {
					socketChannel.pipeline().addLast("ssl", new SslHandler(sslEngine));
				}

				socketChannel.pipeline()
					.addLast(new HttpClientCodec())
					.addLast(new HttpObjectAggregator(configuration.getMaxContentLength()))
					.addLast(new ClientHandler());
			}
		};
		NioEventLoopGroup group = new NioEventLoopGroup(1, new DefaultThreadFactory("flink-rest-client-netty"));

		bootstrap = new Bootstrap();
		bootstrap
			.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Math.toIntExact(configuration.getConnectionTimeout()))
			.group(group)
			.channel(NioSocketChannel.class)
			.handler(initializer);

		LOG.info("Rest client endpoint started.");
	}

	public void shutdown(Time timeout) {
		LOG.info("Shutting down rest endpoint.");
		CompletableFuture<?> groupFuture = new CompletableFuture<>();
		if (bootstrap != null) {
			if (bootstrap.group() != null) {
				bootstrap.group().shutdownGracefully(0L, timeout.toMilliseconds(), TimeUnit.MILLISECONDS)
					.addListener(finished -> {
						if (finished.isSuccess()) {
							groupFuture.complete(null);
						} else {
							groupFuture.completeExceptionally(finished.cause());
						}
					});
			}
		}

		try {
			groupFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			LOG.info("Rest endpoint shutdown complete.");
		} catch (Exception e) {
			LOG.warn("Rest endpoint shutdown failed.", e);
		}
	}

	public <M extends MessageHeaders<R, P, U>, U extends MessageParameters, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> sendRequest(String targetAddress, int targetPort, M messageHeaders, U messageParameters, R request) throws IOException {
		Preconditions.checkNotNull(targetAddress);
		Preconditions.checkArgument(0 <= targetPort && targetPort < 65536, "The target port " + targetPort + " is not in the range (0, 65536].");
		Preconditions.checkNotNull(messageHeaders);
		Preconditions.checkNotNull(request);
		Preconditions.checkNotNull(messageParameters);
		Preconditions.checkState(messageParameters.isResolved(), "Message parameters were not resolved.");

		String targetUrl = MessageParameters.resolveUrl(messageHeaders.getTargetRestEndpointURL(), messageParameters);

		LOG.debug("Sending request of class {} to {}:{}{}", request.getClass(), targetAddress, targetPort, targetUrl);
		// serialize payload
		StringWriter sw = new StringWriter();
		objectMapper.writeValue(sw, request);
		ByteBuf payload = Unpooled.wrappedBuffer(sw.toString().getBytes(ConfigConstants.DEFAULT_CHARSET));

		// create request and set headers
		FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, messageHeaders.getHttpMethod().getNettyHttpMethod(), targetUrl, payload);
		httpRequest.headers()
			.add(HttpHeaders.Names.CONTENT_LENGTH, payload.capacity())
			.add(HttpHeaders.Names.CONTENT_TYPE, RestConstants.REST_CONTENT_TYPE)
			.set(HttpHeaders.Names.HOST, targetAddress + ':' + targetPort)
			.set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

		final JavaType responseType;

		final Collection<Class<?>> typeParameters = messageHeaders.getResponseTypeParameters();

		if (typeParameters.isEmpty()) {
			responseType = objectMapper.constructType(messageHeaders.getResponseClass());
		} else {
			responseType = objectMapper.getTypeFactory().constructParametricType(
				messageHeaders.getResponseClass(),
				typeParameters.toArray(new Class<?>[typeParameters.size()]));
		}

		return submitRequest(targetAddress, targetPort, httpRequest, responseType);
	}

	private <P extends ResponseBody> CompletableFuture<P> submitRequest(String targetAddress, int targetPort, FullHttpRequest httpRequest, JavaType responseType) {
		final ChannelFuture connectFuture = bootstrap.connect(targetAddress, targetPort);

		final CompletableFuture<Channel> channelFuture = new CompletableFuture<>();

		connectFuture.addListener(
			(ChannelFuture future) -> {
				if (future.isSuccess()) {
					channelFuture.complete(future.channel());
				} else {
					channelFuture.completeExceptionally(future.cause());
				}
			});

		return channelFuture
			.thenComposeAsync(
				channel -> {
					ClientHandler handler = channel.pipeline().get(ClientHandler.class);
					CompletableFuture<JsonResponse> future = handler.getJsonFuture();
					channel.writeAndFlush(httpRequest);
					return future;
				},
				executor)
			.thenComposeAsync(
				(JsonResponse rawResponse) -> parseResponse(rawResponse, responseType),
				executor);
	}

	private static <P extends ResponseBody> CompletableFuture<P> parseResponse(JsonResponse rawResponse, JavaType responseType) {
		CompletableFuture<P> responseFuture = new CompletableFuture<>();
		final JsonParser jsonParser = objectMapper.treeAsTokens(rawResponse.json);
		try {
			P response = objectMapper.readValue(jsonParser, responseType);
			responseFuture.complete(response);
		} catch (IOException originalException) {
			// the received response did not matched the expected response type

			// lets see if it is an ErrorResponse instead
			try {
				ErrorResponseBody error = objectMapper.treeToValue(rawResponse.getJson(), ErrorResponseBody.class);
				responseFuture.completeExceptionally(new RestClientException(error.errors.toString(), rawResponse.getHttpResponseStatus()));
			} catch (JsonProcessingException jpe2) {
				// if this fails it is either the expected type or response type was wrong, most likely caused
				// by a client/search MessageHeaders mismatch
				LOG.error("Received response was neither of the expected type ({}) nor an error. Response={}", responseType, rawResponse, jpe2);
				responseFuture.completeExceptionally(
					new RestClientException(
						"Response was neither of the expected type(" + responseType + ") nor an error.",
						originalException,
						rawResponse.getHttpResponseStatus()));
			}
		}
		return responseFuture;
	}

	private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

		private final CompletableFuture<JsonResponse> jsonFuture = new CompletableFuture<>();

		CompletableFuture<JsonResponse> getJsonFuture() {
			return jsonFuture;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
			if (msg instanceof FullHttpResponse) {
				readRawResponse((FullHttpResponse) msg);
			} else {
				LOG.error("Implementation error: Received a response that wasn't a FullHttpResponse.");
				if (msg instanceof HttpResponse) {
					jsonFuture.completeExceptionally(
						new RestClientException(
							"Implementation error: Received a response that wasn't a FullHttpResponse.",
							((HttpResponse) msg).getStatus()));
				} else {
					jsonFuture.completeExceptionally(
						new RestClientException(
							"Implementation error: Received a response that wasn't a FullHttpResponse.",
							HttpResponseStatus.INTERNAL_SERVER_ERROR));
				}

			}
			ctx.close();
		}

		@Override
		public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
			if (cause instanceof TooLongFrameException) {
				jsonFuture.completeExceptionally(new TooLongFrameException(String.format(
					cause.getMessage() + " Try to raise [%s]",
					RestOptions.CLIENT_MAX_CONTENT_LENGTH.key())));
			} else {
				jsonFuture.completeExceptionally(cause);
			}
			ctx.close();
		}

		private void readRawResponse(FullHttpResponse msg) {
			ByteBuf content = msg.content();

			JsonNode rawResponse;
			try {
				InputStream in = new ByteBufInputStream(content);
				rawResponse = objectMapper.readTree(in);
				LOG.debug("Received response {}.", rawResponse);
			} catch (JsonParseException je) {
				LOG.error("Response was not valid JSON.", je);
				// let's see if it was a plain-text message instead
				content.readerIndex(0);
				try {
					ByteBufInputStream in = new ByteBufInputStream(content);
					byte[] data = new byte[in.available()];
					in.readFully(data);
					String message = new String(data);
					LOG.error("Unexpected plain-text response: {}", message);
					jsonFuture.completeExceptionally(new RestClientException("Response was not valid JSON, but plain-text: " + message, je, msg.getStatus()));
				} catch (IOException e) {
					jsonFuture.completeExceptionally(new RestClientException("Response was not valid JSON, nor plain-text.", je, msg.getStatus()));
				}
				return;
			} catch (IOException ioe) {
				LOG.error("Response could not be read.", ioe);
				jsonFuture.completeExceptionally(new RestClientException("Response could not be read.", ioe, msg.getStatus()));
				return;
			}
			jsonFuture.complete(new JsonResponse(rawResponse, msg.getStatus()));
		}
	}

	private static final class JsonResponse {
		private final JsonNode json;
		private final HttpResponseStatus httpResponseStatus;

		private JsonResponse(JsonNode json, HttpResponseStatus httpResponseStatus) {
			this.json = Preconditions.checkNotNull(json);
			this.httpResponseStatus = Preconditions.checkNotNull(httpResponseStatus);
		}

		public JsonNode getJson() {
			return json;
		}

		public HttpResponseStatus getHttpResponseStatus() {
			return httpResponseStatus;
		}

		@Override
		public String toString() {
			return "JsonResponse{" +
				"json=" + json +
				", httpResponseStatus=" + httpResponseStatus +
				'}';
		}
	}
}
