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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.RedirectHandler;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.router.Router;
import org.apache.flink.runtime.rest.handler.util.HandlerRedirectUtils;
import org.apache.flink.runtime.rest.handler.util.KeepAliveWrite;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.runtime.webmonitor.utils.WebFrontendBootstrap;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RedirectHandler}.
 */
public class RedirectHandlerTest extends TestLogger {

	private static final String RESPONSE_MESSAGE = "foobar";

	/**
	 * Tests the behaviour of the RedirectHandler under the following conditions.
	 *
	 * <p>1. No local address known --> service unavailable
	 * 2. Local address known but no gateway resolved --> service unavailable
	 * 3. Remote leader gateway --> redirection
	 * 4. Local leader gateway
	 * @throws Exception
	 */
	@Test
	public void testRedirectHandler() throws Exception {
		final String restPath = "/testing";
		final String correctAddress = "foobar:21345";
		final String redirectionAddress = "http://foobar:12345";
		final String expectedRedirection = redirectionAddress + restPath;

		final Configuration configuration = new Configuration();
		final Router router = new Router();
		final Time timeout = Time.seconds(10L);
		final CompletableFuture<String> localAddressFuture = new CompletableFuture<>();
		final GatewayRetriever<RestfulGateway> gatewayRetriever = mock(GatewayRetriever.class);

		final RestfulGateway redirectionGateway = mock(RestfulGateway.class);
		when(redirectionGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(redirectionAddress));

		final RestfulGateway localGateway = mock(RestfulGateway.class);
		when(localGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(correctAddress));

		when(gatewayRetriever.getNow()).thenReturn(Optional.empty(), Optional.of(redirectionGateway), Optional.of(localGateway));

		final TestingHandler testingHandler = new TestingHandler(
			localAddressFuture,
			gatewayRetriever,
			timeout);

		router.addGet(restPath, testingHandler);
		WebFrontendBootstrap bootstrap = new WebFrontendBootstrap(
			router,
			log,
			null,
			null,
			"localhost",
			0,
			configuration);

		try (HttpTestClient httpClient = new HttpTestClient("localhost", bootstrap.getServerPort())) {
			// 1. without completed local address future --> Internal server error
			httpClient.sendGetRequest(restPath, FutureUtils.toFiniteDuration(timeout));

			HttpTestClient.SimpleHttpResponse response = httpClient.getNextResponse(FutureUtils.toFiniteDuration(timeout));

			Assert.assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, response.getStatus());

			// 2. with completed local address future but no leader gateway available --> Service unavailable
			localAddressFuture.complete(correctAddress);

			httpClient.sendGetRequest(restPath, FutureUtils.toFiniteDuration(timeout));

			response = httpClient.getNextResponse(FutureUtils.toFiniteDuration(timeout));

			Assert.assertEquals(HttpResponseStatus.SERVICE_UNAVAILABLE, response.getStatus());

			// 3. with leader gateway which is not the one of this REST endpoints --> Redirection required
			httpClient.sendGetRequest(restPath, FutureUtils.toFiniteDuration(timeout));

			response = httpClient.getNextResponse(FutureUtils.toFiniteDuration(timeout));

			Assert.assertEquals(HttpResponseStatus.TEMPORARY_REDIRECT, response.getStatus());
			Assert.assertEquals(expectedRedirection, response.getLocation());

			// 4. with local REST endpoint
			httpClient.sendGetRequest(restPath, FutureUtils.toFiniteDuration(timeout));

			response = httpClient.getNextResponse(FutureUtils.toFiniteDuration(timeout));

			Assert.assertEquals(HttpResponseStatus.OK, response.getStatus());
			Assert.assertEquals(RESPONSE_MESSAGE, response.getContent());

		} finally {
			bootstrap.shutdown();
		}
	}

	private static class TestingHandler extends RedirectHandler<RestfulGateway> {

		protected TestingHandler(
				@Nonnull CompletableFuture<String> localAddressFuture,
				@Nonnull GatewayRetriever<RestfulGateway> leaderRetriever,
				@Nonnull Time timeout) {
			super(localAddressFuture, leaderRetriever, timeout, Collections.emptyMap());
		}

		@Override
		protected void respondAsLeader(ChannelHandlerContext channelHandlerContext, RoutedRequest routedRequest, RestfulGateway gateway) throws Exception {
			Assert.assertTrue(channelHandlerContext.channel().eventLoop().inEventLoop());
			HttpResponse response = HandlerRedirectUtils.getResponse(HttpResponseStatus.OK, RESPONSE_MESSAGE);
			KeepAliveWrite.flush(channelHandlerContext.channel(), routedRequest.getRequest(), response);
		}
	}

}
