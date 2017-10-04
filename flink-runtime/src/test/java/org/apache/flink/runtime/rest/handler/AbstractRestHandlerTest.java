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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.WebSocketUpgradeResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Routed;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link AbstractRestHandler}.
 */
public class AbstractRestHandlerTest {

	private static final String WS_SUBPROTOCOL = "test";
	private static final String REST_ADDRESS = "http://localhost:1234";

	private CompletableFuture<String> gatewayRestAddress;
	private GatewayRetriever<RestfulGateway> gatewayRetriever;

	@Before
	public void setup() throws Exception {
		// setup a mock gateway and retriever to satisfy the RedirectHandler
		RestfulGateway mockRestfulGateway = mock(RestfulGateway.class);
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(REST_ADDRESS));
		gatewayRetriever = mock(GatewayRetriever.class);
		when(gatewayRetriever.getNow()).thenReturn(Optional.of(mockRestfulGateway));
	}

	// ----- WebSocket support -----

	/**
	 * Tests the websocket upgrade logic of AbstractRestHandler.
	 */
	@Test
	public void testWebSocketUpgrade() {
		// write a handshake request
		EmbeddedChannel channel = channel(new TestWebSocketRestHandler());
		Routed request = handshakeRequest(WebSocketTestHandlerSpecification.INSTANCE, Collections.emptyMap(), Collections.emptyMap());
		channel.writeInbound(request);

		// check for a websocket handshaker and message handler in the pipeline
		Assert.assertNotNull(channel.pipeline().get(TestWebSocketMessageHandler.class));
		Assert.assertNotNull(channel.pipeline().get(WebSocketServerProtocolHandler.class));

		// check for a websocket handshake response
		FullHttpResponse response = (FullHttpResponse) channel.readOutbound();
		Assert.assertNotNull(response);
		Assert.assertEquals(HttpResponseStatus.SWITCHING_PROTOCOLS, response.getStatus());
	}

	/**
	 * A handler for testing websocket upgrade responses.
	 */
	private class TestWebSocketRestHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, WebSocketUpgradeResponseBody, EmptyMessageParameters> {

		public final ChannelHandler messageHandler = new TestWebSocketMessageHandler();

		public TestWebSocketRestHandler() {
			super(CompletableFuture.completedFuture(REST_ADDRESS), gatewayRetriever, RpcUtils.INF_TIMEOUT, WebSocketTestHandlerSpecification.INSTANCE);
		}

		@Override
		protected CompletableFuture<WebSocketUpgradeResponseBody> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
			WebSocketUpgradeResponseBody response = new WebSocketUpgradeResponseBody(messageHandler);
			return CompletableFuture.completedFuture(response);
		}
	}

	private class TestWebSocketMessageHandler extends ChannelDuplexHandler {
	}

	private static class WebSocketTestHandlerSpecification implements MessageHeaders<EmptyRequestBody, WebSocketUpgradeResponseBody, EmptyMessageParameters> {

		private static final WebSocketTestHandlerSpecification INSTANCE = new WebSocketTestHandlerSpecification();

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/";
		}

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public Class<WebSocketUpgradeResponseBody> getResponseClass() {
			return WebSocketUpgradeResponseBody.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		public WebSocketTestHandlerSpecification getInstance() {
			return INSTANCE;
		}
	}

	// ----- Utility -----

	/**
	 * Creates an embedded channel for HTTP/websocket test purposes.
	 */
	private EmbeddedChannel channel(ChannelHandler handler) {
		// the websocket handshaker looks for HTTP decoder/encoder (to know where to inject WS handlers).
		// For test purposes, use a passthru encoder so that tests may assert various aspects of the response.
		return new EmbeddedChannel(new HttpRequestDecoder(), new TestHttpResponseEncoder(), handler);
	}

	/**
	 * Creates an URI for the given REST spec.
	 */
	private static URI requestUri(MessageHeaders spec) {
		return URI.create(REST_ADDRESS).resolve(spec.getTargetRestEndpointURL());
	}

	/**
	 * Creates a handshake request for the given spec.
	 */
	private static Routed handshakeRequest(MessageHeaders spec, Map<String, String> pathParams, Map<String, List<String>> queryParams) {
		URI wsURL = URI.create(requestUri(spec).toString().replace("http:", "ws:"));
		TestHandshaker handshaker = new TestHandshaker(wsURL, HttpHeaders.EMPTY_HEADERS);
		FullHttpRequest httpRequest = handshaker.newHandshakeRequest();
		Routed routed = new Routed(null, false, httpRequest, spec.getTargetRestEndpointURL(), pathParams, queryParams);
		return routed;
	}

	/**
	 * Creates an HTTP request for the given spec.
	 */
	private static Routed request(MessageHeaders spec, Map<String, String> pathParams, Map<String, List<String>> queryParams) {
		FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, spec.getHttpMethod().getNettyHttpMethod(), spec.getTargetRestEndpointURL());
		Routed routed = new Routed(null, false, httpRequest, spec.getTargetRestEndpointURL(), pathParams, queryParams);
		return routed;
	}

	/**
	 * A helper class for using Netty's built-in handshaking class to construct valid handshake requests.
	 */
	private static class TestHandshaker extends WebSocketClientHandshaker13 {
		public TestHandshaker(URI webSocketURL, HttpHeaders customHeaders) {
			super(webSocketURL, WebSocketVersion.V13, WS_SUBPROTOCOL, false, customHeaders, Integer.MAX_VALUE);
		}

		@Override
		public FullHttpRequest newHandshakeRequest() {
			return super.newHandshakeRequest();
		}
	}

	/**
	 * A no-op encoder that must exist to satisfy the WS handshaker.
	 */
	private static class TestHttpResponseEncoder extends HttpResponseEncoder {
		@Override
		protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
			if (msg != null) {
				ReferenceCountUtil.retain(msg);
				out.add(msg);
			}
		}
	}

}
