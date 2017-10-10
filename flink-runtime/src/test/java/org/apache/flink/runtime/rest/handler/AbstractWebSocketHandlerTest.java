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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.WebSocketSpecification;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.FullHttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequestDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseEncoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Handler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.router.Router;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker13;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.websocketx.WebSocketVersion;
import org.apache.flink.shaded.netty4.io.netty.util.AbstractReferenceCounted;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the {@link AbstractWebSocketHandler}.
 */
public class AbstractWebSocketHandlerTest {

	private static final String TEST_SUBPROTOCOL = "test";
	private static final String REST_ADDRESS = "http://localhost:1234";
	private static final JobID TEST_JOB_ID = new JobID();

	private GatewayRetriever<RestfulGateway> gatewayRetriever;

	@Before
	public void setup() throws Exception {
		// setup a mock gateway and retriever to satisfy the RedirectHandler
		RestfulGateway mockRestfulGateway = mock(RestfulGateway.class);
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(REST_ADDRESS));
		gatewayRetriever = mock(GatewayRetriever.class);
		when(gatewayRetriever.getNow()).thenReturn(Optional.of(mockRestfulGateway));
	}

	/**
	 * Tests the parameter processing logic.
	 */
	@Test
	public void testInvalidMessageParameter() {

		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, new TestHandler());

		// write a handshake request that is lacking a mandatory parameter
		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		URI requestUri = URI.create(requestUri(TestSpecification.INSTANCE, params).toString()
			.replace("42", "NA"));
		FullHttpRequest request = handshakeRequest(requestUri, TEST_SUBPROTOCOL);
		channel.writeInbound(request);

		// verify that the request was rejected
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(0, request.refCnt());
		Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, response.getStatus());
		Assert.assertTrue(channel.isOpen());
	}

	/**
	 * Tests the parameter processing logic.
	 */
	@Test
	public void testMissingMessageParameter() {

		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, new TestHandler());

		// write a handshake request that is lacking a mandatory parameter
		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		URI requestUri = URI.create(requestUri(TestSpecification.INSTANCE, params).toString()
			.replace("q", "_"));
		FullHttpRequest request = handshakeRequest(requestUri, TEST_SUBPROTOCOL);
		channel.writeInbound(request);

		// verify that the request was rejected
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(0, request.refCnt());
		Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, response.getStatus());
		Assert.assertTrue(channel.isOpen());
	}

	/**
	 * Tests the parameter processing logic.
	 */
	@Test
	public void testValidMessageParameters() {

		TestHandler handler = new TestHandler();
		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, handler);

		// write a handshake request with the mandatory parameter
		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		FullHttpRequest request = handshakeRequest(requestUri(TestSpecification.INSTANCE, params), TEST_SUBPROTOCOL);
		channel.writeInbound(request);

		// verify that the request was accepted and that the message parameters were available in numerous callbacks
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(0, request.refCnt());
		Assert.assertEquals(HttpResponseStatus.SWITCHING_PROTOCOLS, response.getStatus());
		Assert.assertNotNull(handler.handshakeInitiated);
		Assert.assertEquals(Collections.singletonList(42), handler.handshakeInitiated.queryParameter.getValue());
	}

	/**
	 * Tests the websocket upgrade logic.
	 */
	@Test
	public void testWebSocketUpgrade() {

		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, new TestHandler());

		// write a handshake request
		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		FullHttpRequest request = handshakeRequest(requestUri(TestSpecification.INSTANCE, params), TEST_SUBPROTOCOL);
		channel.writeInbound(request);

		// check for a websocket handshaker and message handler in the pipeline
		Assert.assertNotNull(channel.pipeline().get(WebSocketServerProtocolHandler.class));
		Assert.assertNotNull(channel.pipeline().get(JsonWebSocketMessageCodec.class));

		// check for a websocket handshake response
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(0, request.refCnt());
		Assert.assertNotNull(response);
		Assert.assertEquals(HttpResponseStatus.SWITCHING_PROTOCOLS, response.getStatus());
	}

	/**
	 * Tests upgrade failure due to a handshaking issue.
	 */
	@Test
	public void testWebSocketUpgradeFailure() {

		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, new TestHandler());

		// write an invalid handshake request
		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		FullHttpRequest request = handshakeRequest(requestUri(TestSpecification.INSTANCE, params), TEST_SUBPROTOCOL);
		request.headers().remove("Sec-WebSocket-Key");
		channel.writeInbound(request);

		// check for a websocket handshake response
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(0, request.refCnt());
		Assert.assertNotNull(response);
		Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, response.getStatus());
		Assert.assertFalse(channel.isOpen());
	}

	/**
	 * Tests message reading and writing.
	 */
	@Test
	public void testMessageReadWrite() throws Exception {
		TestHandler handler = new TestHandler();
		EmbeddedChannel channel = channel(TestSpecification.INSTANCE, handler);

		TestParameters params = TestParameters.from(TEST_JOB_ID, 42);
		FullHttpRequest request = handshakeRequest(requestUri(TestSpecification.INSTANCE, params), TEST_SUBPROTOCOL);
		channel.writeInbound(request);
		HttpResponse response = (HttpResponse) channel.readOutbound();
		Assert.assertEquals(HttpResponseStatus.SWITCHING_PROTOCOLS, response.getStatus());

		TestMessage expected = new TestMessage(42);
		channel.writeInbound(expected);
		TestMessage actual = handler.messages.take();
		Assert.assertEquals(expected.sequenceNumber, actual.sequenceNumber);
		Assert.assertEquals(1, expected.refCnt());
		Assert.assertTrue(channel.isOpen());

		ByteBuf msg = (ByteBuf) channel.readOutbound();
		Assert.assertEquals(23, msg.readableBytes()); // contains an on-the-wire text frame
		ReferenceCountUtil.release(msg);
	}

	/**
	 * A path parameter for test purposes.
	 */
	static class TestPathParameter extends MessagePathParameter<JobID> {
		TestPathParameter() {
			super("jobid");
		}

		@Override
		public JobID convertFromString(String value) {
			return JobID.fromHexString(value);
		}

		@Override
		protected String convertToString(JobID value) {
			return value.toString();
		}
	}

	/**
	 * A query parameter for test purposes.
	 */
	private static class TestQueryParameter extends MessageQueryParameter<Integer> {

		TestQueryParameter() {
			super("q", MessageParameterRequisiteness.MANDATORY);
		}

		@Override
		public Integer convertValueFromString(String value) {
			return Integer.parseInt(value);
		}

		@Override
		public String convertStringToValue(Integer value) {
			return value.toString();
		}
	}

	/**
	 * Parameters for the test WebSocket resource.
	 */
	private static class TestParameters extends MessageParameters {
		private final TestPathParameter pathParameter = new TestPathParameter();
		private final TestQueryParameter queryParameter = new TestQueryParameter();

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.singleton(pathParameter);
		}

		@Override
		public Collection<MessageQueryParameter<?>> getQueryParameters() {
			return Collections.singleton(queryParameter);
		}

		public static TestParameters from(JobID jobID, int sequenceNumber) {
			TestParameters p = new TestParameters();
			p.pathParameter.resolve(jobID);
			p.queryParameter.resolve(Collections.singletonList(sequenceNumber));
			return p;
		}
	}

	/**
	 * The specification for the test WebSocket resource.
	 */
	static class TestSpecification implements WebSocketSpecification<TestParameters, TestMessage, TestMessage> {

		static final TestSpecification INSTANCE = new TestSpecification();

		@Override
		public String getTargetRestEndpointURL() {
			return "/jobs/:jobid/subscribe";
		}

		@Override
		public String getSubprotocol() {
			return "test";
		}

		@Override
		public Class<TestMessage> getOutboundClass() {
			return TestMessage.class;
		}

		@Override
		public Class<TestMessage> getInboundClass() {
			return TestMessage.class;
		}

		@Override
		public TestParameters getUnresolvedMessageParameters() {
			return new TestParameters();
		}
	}

	/**
	 * The channel handler for the test WebSocket resource.
	 */
	private class TestHandler extends AbstractWebSocketHandler<RestfulGateway, TestParameters, TestMessage, TestMessage> {

		TestParameters handshakeInitiated = null;

		TestParameters handshakeCompleted = null;

		final LinkedBlockingQueue<TestMessage> messages = new LinkedBlockingQueue<>();

		public TestHandler() {
			super(CompletableFuture.completedFuture(REST_ADDRESS), gatewayRetriever, RpcUtils.INF_TIMEOUT, TestSpecification.INSTANCE);
		}

		@Override
		protected CompletableFuture<Void> handshakeInitiated(ChannelHandlerContext ctx, TestParameters parameters) throws Exception {
			handshakeInitiated = parameters;
			return CompletableFuture.completedFuture(null);
		}

		@Override
		protected void handshakeCompleted(ChannelHandlerContext ctx, TestParameters parameters) throws Exception {
			handshakeCompleted = parameters;
		}

		@Override
		protected void messageReceived(ChannelHandlerContext ctx, TestParameters parameters, TestMessage msg) throws Exception {
			ReferenceCountUtil.retain(msg);
			messages.put(msg);

			ctx.channel().writeAndFlush(msg);
		}
	}

	/**
	 * A WebSocket message for test purposes.
	 */
	private static class TestMessage extends AbstractReferenceCounted implements ResponseBody, RequestBody {

		public final int sequenceNumber;

		public TestMessage(@JsonProperty("sequenceNumber") int sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}

		@Override
		protected void deallocate() {
		}
	}

	// ----- Utility -----

	/**
	 * Creates an embedded channel for HTTP/websocket test purposes.
	 */
	private EmbeddedChannel channel(RestHandlerSpecification spec, ChannelInboundHandler handler) {

		// the websocket handshaker looks for HTTP decoder/encoder (to know where to inject WS handlers).
		// For test purposes, use a passthru encoder so that tests may assert various aspects of the response.
		return new EmbeddedChannel(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel channel) throws Exception {
				Router router = new Router();
				router.GET(spec.getTargetRestEndpointURL(), handler);
				Handler routerHandler = new Handler(router);
				channel.pipeline()
					.addLast(new HttpRequestDecoder(), new MockHttpResponseEncoder())
					.addLast(routerHandler.name(), routerHandler);
			}
		});
	}

	/**
	 * Creates an URI for the given REST spec.
	 */
	private static URI requestUri(RestHandlerSpecification spec, MessageParameters parameters) {
		URI resolved = URI.create(MessageParameters.resolveUrl(spec.getTargetRestEndpointURL(), parameters));
		return URI.create(REST_ADDRESS).resolve(resolved);
	}

	/**
	 * Creates a handshake request for the given request URI.
	 */
	private static FullHttpRequest handshakeRequest(URI requestUri, String subprotocol) {
		URI wsURL = URI.create(requestUri.toString().replace("http:", "ws:").replace("https:", "wss:"));
		MockHandshaker handshaker = new MockHandshaker(wsURL, subprotocol, HttpHeaders.EMPTY_HEADERS);
		FullHttpRequest httpRequest = handshaker.newHandshakeRequest();
		return httpRequest;
	}

	/**
	 * A helper class for using Netty's built-in handshaking class to construct valid handshake requests.
	 */
	private static class MockHandshaker extends WebSocketClientHandshaker13 {
		public MockHandshaker(URI webSocketURL, String subprotocol, HttpHeaders customHeaders) {
			super(webSocketURL, WebSocketVersion.V13, subprotocol, false, customHeaders, Integer.MAX_VALUE);
		}

		@Override
		public FullHttpRequest newHandshakeRequest() {
			return super.newHandshakeRequest();
		}
	}

	/**
	 * A no-op encoder that must exist to satisfy the WS handshaker.
	 */
	private static class MockHttpResponseEncoder extends HttpResponseEncoder {
		@Override
		protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
			if (msg != null) {
				ReferenceCountUtil.retain(msg);
				out.add(msg);
			}
		}
	}
}
