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

package org.apache.flink.runtime.webmonitor.testutils;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * A simple HTTP client.
 *
 * <pre>
 * HttpTestClient client = new HttpTestClient("localhost", 8081, 5000);
 * client.get("/overview");
 * SimpleHttpResponse response = client.getNextResponse(5000);
 *
 * assertEquals(200, response.getStatus().code()); // OK
 * assertEquals("application/json", response.getType());
 * assertTrue(response.getContent().contains("\"jobs-running\":0"));
 * </pre>
 */
public class HttpTestClient implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(HttpTestClient.class);

	/** Target host */
	private final String host;

	/** Target port */
	private final int port;

	/** Netty's thread group for the client */
	private final EventLoopGroup group;

	/** TCP connection to the HTTP server */
	private final Channel channel;

	/** Responses received by the client */
	private final BlockingQueue<SimpleHttpResponse> responses = new LinkedBlockingQueue<>();

	/**
	 * Creates a client instance for the server at the target host and port.
	 *
	 * <p>This establishes a connection to the server.
	 *
	 * @param host Host of the HTTP server
	 * @param port Port of the HTTP server
	 * @throws InterruptedException
	 */
	public HttpTestClient(String host, int port, int connectTimoutMs) throws InterruptedException,
			TimeoutException {
		this.host = host;
		this.port = port;

		this.group = new NioEventLoopGroup();

		Bootstrap b = new Bootstrap();
		b.group(group)
				.channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {

					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ChannelPipeline p = ch.pipeline();
						p.addLast(new HttpClientCodec());
						p.addLast(new HttpContentDecompressor());
						p.addLast(new ClientHandler(responses));
					}
				});

		// Make the connection attempt.
		ChannelFuture connect = b.connect(host, port);

		if (connect.await(connectTimoutMs, TimeUnit.MILLISECONDS)) {
			this.channel = connect.channel();
		}
		else {
			throw new TimeoutException("Connection failed");
		}
	}

	/**
	 * Sends a request to to the server.
	 *
	 * <pre>
	 * HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/overview");
	 * request.headers().set(HttpHeaders.Names.HOST, host);
	 * request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
	 *
	 * sendRequest(request);
	 * </pre>
	 *
	 * @param request The {@link HttpRequest} to send to the server
	 */
	public void sendRequest(HttpRequest request) {
		LOG.debug("Writing {}.", request);

		channel.writeAndFlush(request);
	}

	/**
	 * Sends a simple GET request to the given path. You only specify the $path part of
	 * http://$host:$host/$path.
	 *
	 * @param path The $path to GET (http://$host:$host/$path)
	 */
	public void get(String path) {
		if (!path.startsWith("/")) {
			path = "/" + path;
		}

		HttpRequest getRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1,
				HttpMethod.GET, path);
		getRequest.headers().set(HttpHeaders.Names.HOST, host);
		getRequest.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);

		sendRequest(getRequest);
	}

	/**
	 * Returns the next available HTTP response. A call to this method blocks until a response
	 * becomes available.
	 *
	 * @return The next available {@link SimpleHttpResponse}
	 */
	public SimpleHttpResponse getNextResponse() throws InterruptedException {
		return responses.take();
	}

	/**
	 * Returns the next available HTTP response . A call to this method blocks until a response
	 * becomes available or throws an Exception if the timeout fires.
	 *
	 * @param timeoutMs Timeout in milliseconds for the next response to become available
	 * @return The next available {@link SimpleHttpResponse}
	 */
	public SimpleHttpResponse getNextResponse(int timeoutMs) throws InterruptedException,
			TimeoutException {

		checkArgument(timeoutMs >= 0, "Negative Timeout");

		SimpleHttpResponse response = responses.poll(timeoutMs, TimeUnit.MILLISECONDS);

		if (response == null) {
			throw new TimeoutException("No response within timeout of " + timeoutMs + " ms");
		}
		else {
			return response;
		}
	}

	/**
	 * Closes the client.
	 */
	@Override
	public void close() throws InterruptedException {
		if (channel != null) {
			channel.close().sync();
		}

		if (group != null) {
			group.shutdownGracefully();
		}

		LOG.debug("Closed");
	}

	/**
	 * A simple HTTP response.
	 */
	public static class SimpleHttpResponse {

		private final HttpResponseStatus status;

		private final String type;

		private final String content;

		public SimpleHttpResponse(HttpResponseStatus status, String type, String content) {
			this.status = status;
			this.type = type;
			this.content = content;
		}

		public HttpResponseStatus getStatus() {
			return status;
		}

		public String getType() {
			return type;
		}

		public String getContent() {
			return content;
		}

		@Override
		public String toString() {
			return "HttpResponse(status=" + status + ", type='" + type + "'" + ", content='" +
					content + "')";
		}
	}

	/**
	 * The response handler. Responses from the server are handled here.
	 */
	private static class ClientHandler extends SimpleChannelInboundHandler<HttpObject> {

		private final BlockingQueue<SimpleHttpResponse> responses;

		private HttpResponseStatus currentStatus;

		private String currentType;

		private String currentContent = "";

		public ClientHandler(BlockingQueue<SimpleHttpResponse> responses) {
			this.responses = responses;
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
			LOG.debug("Received {}", msg);

			if (msg instanceof HttpResponse) {
				HttpResponse response = (HttpResponse) msg;

				currentStatus = response.getStatus();
				currentType = response.headers().get(HttpHeaders.Names.CONTENT_TYPE);

				if (HttpHeaders.isTransferEncodingChunked(response)) {
					LOG.debug("Content is chunked");
				}
			}

			if (msg instanceof HttpContent) {
				HttpContent content = (HttpContent) msg;

				// Add the content
				currentContent += content.content().toString(CharsetUtil.UTF_8);

				// Finished with this
				if (content instanceof LastHttpContent) {
					responses.add(new SimpleHttpResponse(currentStatus, currentType,
							currentContent));

					currentStatus = null;
					currentType = null;
					currentContent = "";

					ctx.close();
				}
			}
		}
	}
}
