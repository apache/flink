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
	private final SSLEngine sslEngine;

	private Bootstrap bootstrap;

	private final Object lock = new Object();

	public RestClientEndpoint(RestClientEndpointConfiguration configuration) {
		this.configuredTargetAddress = configuration.getTargetRestEndpointAddress();
		this.configuredTargetPort = configuration.getTargetRestEndpointPort();
		this.sslEngine = configuration.getSslEngine();
	}

	public void start() {
		NioEventLoopGroup group = new NioEventLoopGroup(1);

		bootstrap = new Bootstrap();
		bootstrap
			.group(group)
			.channel(NioSocketChannel.class);

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

		return submitRequest(httpRequest, messageHeaders);
	}

	private <M extends MessageHeaders<R, P, U>, U extends ParameterMapper, R extends RequestBody, P extends ResponseBody> CompletableFuture<P> submitRequest(FullHttpRequest httpRequest, M messageHeaders) {
		synchronized (lock) {
			CompletableFuture<P> responseFuture = ClientHandler.addHandlerForResponse(bootstrap, sslEngine, messageHeaders.getResponseClass());

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
	}

	private static class RestChannelInitializer extends ChannelInitializer<SocketChannel> {

		private final SSLEngine sslEngine;
		private final ClientHandler handler;

		public RestChannelInitializer(SSLEngine sslEngine, ClientHandler handler) {
			this.sslEngine = sslEngine;
			this.handler = handler;
		}

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
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
	}

	private static class ClientHandler<P extends ResponseBody> extends SimpleChannelInboundHandler<Object> {

		private final ExpectedResponse<P> expectedResponse;

		private ClientHandler(ExpectedResponse<P> expectedResponse) {
			this.expectedResponse = expectedResponse;
		}

		static <P extends ResponseBody> CompletableFuture<P> addHandlerForResponse(Bootstrap bootStrap, SSLEngine sslEngine, Class<P> expectedResponse) {
			CompletableFuture<P> responseFuture = new CompletableFuture<>();

			ClientHandler handler = new ClientHandler<>(new ExpectedResponse<>(expectedResponse, responseFuture));
			bootStrap.handler(new RestChannelInitializer(sslEngine, handler));

			return responseFuture;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (msg instanceof FullHttpResponse) {
				completeFuture((FullHttpResponse) msg);
			} else {
				LOG.error("Implementation error: Received a response that wasn't a FullHttpResponse.");
			}
		}

		private void completeFuture(FullHttpResponse msg) throws IOException {
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

			if (expectedResponse == null) {
				LOG.error("Received response, but we didn't expect any. Response={}", rawResponse);
				return;
			}

			try {
				try {
					P response = objectMapper.treeToValue(rawResponse, expectedResponse.responseClass);
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
