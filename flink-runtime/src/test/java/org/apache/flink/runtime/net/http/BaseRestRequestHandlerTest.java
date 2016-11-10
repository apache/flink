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

package org.apache.flink.runtime.net.http;

import com.google.common.net.MediaType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests the {@link BaseRestRequestHandler}.
 */
public class BaseRestRequestHandlerTest {

	private static final int TIMEOUT = 1;

	class TestHandler extends BaseRestRequestHandler<TestUtils.TestEntity,TestUtils.TestEntity> {
		private Promise<RestResponse<TestUtils.TestEntity>> promise;
		public TestHandler() {
			super(TestUtils.TestEntity.class, TIMEOUT, TimeUnit.SECONDS);
		}
		public void setPromise(Promise<RestResponse<TestUtils.TestEntity>> promise) {
			this.promise = promise;
		}
		@Override
		protected Future<RestResponse<TestUtils.TestEntity>> handleRequest(
			ChannelHandlerContext ctx, RestRequest<TestUtils.TestEntity> request) {
			return promise;
		}
	}

	@Test
	public void testHandle() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(new TestHandler());
		Promise<RestResponse<TestUtils.TestEntity>> promise = channel.eventLoop().newPromise();
		channel.pipeline().get(TestHandler.class).setPromise(promise);

		DefaultRestRequest request = new DefaultRestRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		request.headers().add(HttpHeaders.Names.ACCEPT, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(request);
		promise.setSuccess(new DefaultRestResponse<TestUtils.TestEntity>(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, null));

		RestResponse response = (RestResponse) channel.outboundMessages().remove();
		assertEquals(HttpResponseStatus.OK, response.getStatus());

		// verify that the scheduled timeout has no unpleasant effect
		Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT + 1));
		channel.runScheduledPendingTasks();
		assertEquals(0, channel.outboundMessages().size());
	}

	@Test
	public void testHandleNotAcceptable() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(new TestHandler());

		DefaultRestRequest request = new DefaultRestRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		channel.writeInbound(request);

		RestResponse response1 = (RestResponse) channel.outboundMessages().remove();
		assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response1.getStatus());

		request.headers().set(HttpHeaders.Names.ACCEPT, MediaType.XML_UTF_8.toString());
		channel.writeInbound(request);

		RestResponse response2 = (RestResponse) channel.outboundMessages().remove();
		assertEquals(HttpResponseStatus.NOT_ACCEPTABLE, response2.getStatus());
	}

	@Test
	public void testHandleException() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(new TestHandler());
		Promise<RestResponse<TestUtils.TestEntity>> promise = channel.eventLoop().newPromise();
		channel.pipeline().get(TestHandler.class).setPromise(promise);

		DefaultRestRequest request = new DefaultRestRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		request.headers().add(HttpHeaders.Names.ACCEPT, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(request);
		promise.setFailure(new RuntimeException("expected"));

		RestResponse response = (RestResponse) channel.outboundMessages().remove();
		assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.getStatus());
	}

	@Test
	public void testTimeout() throws Exception {
		EmbeddedChannel channel = new EmbeddedChannel(new TestHandler());
		Promise<RestResponse<TestUtils.TestEntity>> promise = channel.eventLoop().newPromise();
		channel.pipeline().get(TestHandler.class).setPromise(promise);

		DefaultRestRequest request = new DefaultRestRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");
		request.headers().add(HttpHeaders.Names.ACCEPT, MediaType.JSON_UTF_8.toString());
		channel.writeInbound(request);

		// exercise the response timeout
		Thread.sleep(TimeUnit.SECONDS.toMillis(TIMEOUT + 1));
		channel.runScheduledPendingTasks();

		RestResponse response = (RestResponse) channel.outboundMessages().remove();
		assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.getStatus());

		// verify that a late completion of the response promise has no pleasant effect
		assertFalse(promise.tryFailure(new RuntimeException("expected")));
		assertEquals(0, channel.outboundMessages().size());
	}

	@Test
	public void testBlockQuote() {
		assertEquals("> \n", BaseRestRequestHandler.blockquote(""));
		assertEquals("> A\n", BaseRestRequestHandler.blockquote("A"));
		assertEquals("> A\n> B\n> C\n", BaseRestRequestHandler.blockquote("A\nB\nC\n"));
	}
}
