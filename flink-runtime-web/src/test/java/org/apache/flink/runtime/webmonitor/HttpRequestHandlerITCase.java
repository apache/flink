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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpServerCodec;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.DefaultHttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpDataFactory;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.stream.ChunkedWriteHandler;

import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders.Names.LOCATION;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link HttpRequestHandler} class.
 */
public class HttpRequestHandlerITCase {

	private static final String JAR_RUN_URI = "jars/jarID/run";

	private static final FiniteDuration TestTimeout = new FiniteDuration(1, TimeUnit.MINUTES);

	private static String runJarUriWithQueryParams;

	@Before
	public void setup() {
		StringBuffer sb = new StringBuffer(JAR_RUN_URI)
				.append("?")
				.append("program-args=args")
				.append("&")
				.append("entry-class=EntryClassName.class")
				.append("&")
				.append("parallelism=4")
				.append("&")
				.append("allowNonRestoredState=true");
		runJarUriWithQueryParams = sb.toString();

		NioEventLoopGroup parentGroup   = new NioEventLoopGroup(1);
		NioEventLoopGroup workerGroup = new NioEventLoopGroup();
		ChannelInitializer<SocketChannel> init = new ChannelInitializer<SocketChannel>() {

			@Override
			protected void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline()
						.addLast(new HttpServerCodec())
						.addLast(new ChunkedWriteHandler())
						.addLast(new HttpRequestHandler(null))
						.addLast(new TestRequestHandler());
			}
		};
		ServerBootstrap server = new ServerBootstrap();
		server.group(parentGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(init);
		server.bind(new InetSocketAddress(10000));
	}

	@Test
	public void testPostRequestForRunJarOperationWithRequestParameters() throws Exception {

		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, JAR_RUN_URI);
		HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
		HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request, true);
		bodyRequestEncoder.addBodyAttribute("program-args", "args");
		bodyRequestEncoder.addBodyAttribute("entry-class", "EntryClassName.class");
		bodyRequestEncoder.addBodyAttribute("parallelism", "4");
		bodyRequestEncoder.addBodyAttribute("allowNonRestoredState", "true");
		request = bodyRequestEncoder.finalizeRequest();

		HttpTestClient.SimpleHttpResponse response = sendPostRequest(request, bodyRequestEncoder);
		assertResponse(response, runJarUriWithQueryParams);
	}

	@Test
	public void testPostRequestForRunJarOperationWithMixedRequestAndQueryParameters() throws Exception {

		String requestUri = JAR_RUN_URI + "?program-args=args&entry-class=EntryClassName.class";
		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, requestUri);
		HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
		HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request, true);
		bodyRequestEncoder.addBodyAttribute("parallelism", "4");
		bodyRequestEncoder.addBodyAttribute("allowNonRestoredState", "true");
		request = bodyRequestEncoder.finalizeRequest();

		HttpTestClient.SimpleHttpResponse response = sendPostRequest(request, bodyRequestEncoder);
		assertResponse(response, runJarUriWithQueryParams);
	}

	@Test
	public void testPostRequestForRunJarOperationWithQueryParams() {

		ByteBuf byteBuf = Unpooled.copiedBuffer("ping", Charset.defaultCharset());
		HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, runJarUriWithQueryParams, byteBuf);
		request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, byteBuf.readableBytes());

		HttpTestClient.SimpleHttpResponse response = sendPostRequest(request);
		assertResponse(response, runJarUriWithQueryParams);
	}

	@Test
	public void testPostRequestForRunJarOperationWithLargeQueryString() {

		ByteBuf byteBuf = Unpooled.copiedBuffer("ping", Charset.defaultCharset());
		char[] chars = new char[4096];
		Arrays.fill(chars, "x".charAt(0));
		String runJarUrlWith4096Chars = new StringBuffer()
				.append(JAR_RUN_URI).append("?").append("program-args=").append(chars).toString();
		HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, runJarUrlWith4096Chars, byteBuf);
		request.headers().set(HttpHeaders.Names.CONTENT_LENGTH, byteBuf.readableBytes());

		HttpTestClient.SimpleHttpResponse response = sendPostRequest(request);
		assertResponse(response, "/bad-request");
	}

	@Test
	public void testPostRequestForRunJarOperationWithLargeParamSize() throws Exception {
		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, JAR_RUN_URI);
		HttpDataFactory factory = new DefaultHttpDataFactory(DefaultHttpDataFactory.MINSIZE);
		HttpPostRequestEncoder bodyRequestEncoder = new HttpPostRequestEncoder(factory, request, true);
		char[] chars = new char[4096];
		Arrays.fill(chars, "x".charAt(0));
		bodyRequestEncoder.addBodyAttribute("program-args", new String(chars));
		request = bodyRequestEncoder.finalizeRequest();

		HttpTestClient.SimpleHttpResponse response = sendPostRequest(request, bodyRequestEncoder);
		assertResponse(response, new StringBuffer(JAR_RUN_URI).append("?program-args=").append(chars).toString());
	}

	private HttpTestClient.SimpleHttpResponse sendPostRequest(HttpRequest request) {
		final Deadline deadline = TestTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("127.0.0.1", 10000)) {
			client.sendRequest(request, deadline.timeLeft());
			HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());
			return response;
		}
		catch (Exception e) {
			fail("Post request failed:" + e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private HttpTestClient.SimpleHttpResponse sendPostRequest(HttpRequest request, HttpPostRequestEncoder encoder) {
		final Deadline deadline = TestTimeout.fromNow();

		try (HttpTestClient client = new HttpTestClient("127.0.0.1", 10000)) {
			client.sendPostRequest(request, encoder, deadline.timeLeft());
			HttpTestClient.SimpleHttpResponse response = client.getNextResponse(deadline.timeLeft());
			return response;
		}
		catch (Exception e) {
			fail("Post request failed: " + e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private void assertResponse(HttpTestClient.SimpleHttpResponse response, String expectedUrl) {
		assertNotNull(response);
		assertEquals(OK, response.getStatus());
		assertEquals("text/plain; charset=UTF-8", response.getType());
		String content = response.getContent();
		assertNotNull(content);
		assertTrue(content.contains("pong"));
		assertEquals(expectedUrl, response.getLocation());
	}

	private static class TestRequestHandler extends SimpleChannelInboundHandler<HttpRequest> {

		@Override
		public void channelRead0(ChannelHandlerContext ctx, HttpRequest request) throws Exception {

			//Processing request
			ByteBuf byteBuf = Unpooled.copiedBuffer("pong", Charset.defaultCharset());

			//Writing response, wait till it is completely written and close channel after that
			HttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, byteBuf);
			response.headers().set(CONTENT_TYPE, "text/plain; charset=UTF-8");
			response.headers().set(CONTENT_LENGTH, byteBuf.readableBytes());
			response.headers().set(LOCATION, request.getUri());
			ctx.channel().writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
		}
	}
}
