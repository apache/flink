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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.AbstractWebSocketHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;
import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.WebSocketSpecification;
import org.apache.flink.runtime.rest.util.RestClientException;
import org.apache.flink.runtime.rest.websocket.KeyedChannelRouter;
import org.apache.flink.runtime.rest.websocket.WebSocket;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.group.ChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.group.DefaultChannelGroup;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.GlobalEventExecutor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * IT cases for {@link RestClient} and {@link RestServerEndpoint}.
 */
public class RestEndpointITCase extends TestLogger {

	private static final JobID PATH_JOB_ID = new JobID();
	private static final JobID QUERY_JOB_ID = new JobID();
	private static final String JOB_ID_KEY = "jobid";
	private static final Time timeout = Time.seconds(10L);

	private RestServerEndpoint serverEndpoint;
	private RestClient clientEndpoint;

	private TestEventProvider eventProvider;

	@Before
	public void setup() throws Exception {
		Configuration config = new Configuration();

		RestServerEndpointConfiguration serverConfig = RestServerEndpointConfiguration.fromConfiguration(config);
		RestClientConfiguration clientConfig = RestClientConfiguration.fromConfiguration(config);

		final String restAddress = "http://localhost:1234";
		RestfulGateway mockRestfulGateway = mock(RestfulGateway.class);
		when(mockRestfulGateway.requestRestAddress(any(Time.class))).thenReturn(CompletableFuture.completedFuture(restAddress));
		GatewayRetriever<RestfulGateway> mockGatewayRetriever = mock(GatewayRetriever.class);
		when(mockGatewayRetriever.getNow()).thenReturn(Optional.of(mockRestfulGateway));

		// a REST operation
		TestHandler testHandler = new TestHandler(
			CompletableFuture.completedFuture(restAddress),
			mockGatewayRetriever,
			RpcUtils.INF_TIMEOUT);

		// a WebSocket operation
		ChannelGroup eventChannelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
		eventProvider = new TestEventProvider(eventChannelGroup);
		TestWebSocketOperation.WsRestHandler testWebSocketHandler = new TestWebSocketOperation.WsRestHandler(
			CompletableFuture.completedFuture(restAddress),
			mockGatewayRetriever,
			eventProvider,
			RpcUtils.INF_TIMEOUT);

		serverEndpoint = new TestRestServerEndpoint(serverConfig, testHandler, testWebSocketHandler);
		clientEndpoint = new TestRestClient(clientConfig);

		serverEndpoint.start();
	}

	@After
	public void teardown() {
		if (clientEndpoint != null) {
			clientEndpoint.shutdown(timeout);
			clientEndpoint = null;
		}

		if (serverEndpoint != null) {
			serverEndpoint.shutdown(timeout);
			serverEndpoint = null;
		}
	}

	/**
	 * Tests that request are handled as individual units which don't interfere with each other.
	 * This means that request responses can overtake each other.
	 */
	@Test
	public void testRequestInterleaving() throws Exception {

		TestParameters parameters = new TestParameters();
		parameters.jobIDPathParameter.resolve(PATH_JOB_ID);
		parameters.jobIDQueryParameter.resolve(Collections.singletonList(QUERY_JOB_ID));

		// send first request and wait until the handler blocks
		CompletableFuture<TestResponse> response1;
		final InetSocketAddress serverAddress = serverEndpoint.getServerAddress();

		synchronized (TestHandler.LOCK) {
			response1 = clientEndpoint.sendRequest(
				serverAddress.getHostName(),
				serverAddress.getPort(),
				new TestHeaders(),
				parameters,
				new TestRequest(1));
			TestHandler.LOCK.wait();
		}

		// send second request and verify response
		CompletableFuture<TestResponse> response2 = clientEndpoint.sendRequest(
			serverAddress.getHostName(),
			serverAddress.getPort(),
			new TestHeaders(),
			parameters,
			new TestRequest(2));
		Assert.assertEquals(2, response2.get().id);

		// wake up blocked handler
		synchronized (TestHandler.LOCK) {
			TestHandler.LOCK.notifyAll();
		}
		// verify response to first request
		Assert.assertEquals(1, response1.get().id);
	}

	/**
	 * Tests that a bad handler request (HandlerRequest cannot be created) is reported as a BAD_REQUEST
	 * and not an internal server error.
	 *
	 * <p>See FLINK-7663
	 */
	@Test
	public void testBadHandlerRequest() throws Exception {
		final InetSocketAddress serverAddress = serverEndpoint.getServerAddress();

		final FaultyTestParameters parameters = new FaultyTestParameters();

		parameters.faultyJobIDPathParameter.resolve(PATH_JOB_ID);
		((TestParameters) parameters).jobIDQueryParameter.resolve(Collections.singletonList(QUERY_JOB_ID));

		CompletableFuture<TestResponse> response = clientEndpoint.sendRequest(
			serverAddress.getHostName(),
			serverAddress.getPort(),
			new TestHeaders(),
			parameters,
			new TestRequest(2));

		try {
			response.get();

			Assert.fail("The request should fail with a bad request return code.");
		} catch (ExecutionException ee) {
			Throwable t = ExceptionUtils.stripExecutionException(ee);

			Assert.assertTrue(t instanceof RestClientException);

			RestClientException rce = (RestClientException) t;

			Assert.assertEquals(HttpResponseStatus.BAD_REQUEST, rce.getHttpResponseStatus());
		}
	}

	/**
	 * Tests that a web socket operation works end-to-end.
	 */
	@Test
	public void testWebSocketEndToEnd() throws Exception {
		TestWebSocketOperation.WsParameters parameters = new TestWebSocketOperation.WsParameters();
		parameters.jobIDPathParameter.resolve(PATH_JOB_ID);

		final LinkedBlockingQueue<TestMessage> messageQueue = new LinkedBlockingQueue<>();
		final InetSocketAddress serverAddress = serverEndpoint.getServerAddress();

		// open a websocket connection with a listener that simply enqueues incoming messages
		CompletableFuture<WebSocket<TestMessage, TestMessage>> response = clientEndpoint.sendWebSocketRequest(
			serverAddress.getHostName(),
			serverAddress.getPort(),
			new TestWebSocketOperation.WsHeaders(),
			parameters,
			(event) -> {
				try {
					messageQueue.put(event);
				}
				catch (Exception e) {
					throw new RuntimeException(e);
				}
			});

		// wait for the connection to be established
		WebSocket<TestMessage, TestMessage> webSocket = response.get();
		try {
			// wait for the server to register the channel (happens asynchronously after handshake complete)
			TestWebSocketOperation.WsRestHandler.LATCH.await();

			// play ping-pong (server goes first)
			eventProvider.writeAndFlush(PATH_JOB_ID, new TestMessage(42));
			TestMessage received = messageQueue.take();
			Assert.assertEquals(42, received.sequenceNumber);
			webSocket.send(received.incr());
			received = messageQueue.take();
			Assert.assertEquals(44, received.sequenceNumber);
		}
		finally {
			webSocket.close();
		}
	}

	private static class TestRestServerEndpoint extends RestServerEndpoint {

		private final TestHandler testHandler;
		private final TestWebSocketOperation.WsRestHandler testWebSocketHandler;

		TestRestServerEndpoint(RestServerEndpointConfiguration configuration, TestHandler testHandler, TestWebSocketOperation.WsRestHandler testWebSocketHandler) {
			super(configuration);

			this.testHandler = Preconditions.checkNotNull(testHandler);
			this.testWebSocketHandler = Preconditions.checkNotNull(testWebSocketHandler);
		}

		@Override
		protected Collection<Tuple2<RestHandlerSpecification, ChannelInboundHandler>> initializeHandlers(CompletableFuture<String> restAddressFuture) {
			return Arrays.asList(
				Tuple2.of(new TestHeaders(), testHandler),
				Tuple2.of(new TestWebSocketOperation.WsHeaders(), testWebSocketHandler));
		}
	}

	private static class TestEventProvider extends KeyedChannelRouter<JobID> {
		public TestEventProvider(ChannelGroup channelGroup) {
			super(AttributeKey.valueOf("jobID"), channelGroup);
		}
	}

	private static class TestHandler extends AbstractRestHandler<RestfulGateway, TestRequest, TestResponse, TestParameters> {

		public static final Object LOCK = new Object();

		TestHandler(
			CompletableFuture<String> localAddressFuture,
			GatewayRetriever<RestfulGateway> leaderRetriever,
			Time timeout) {
			super(
				localAddressFuture,
				leaderRetriever,
				timeout,
				new TestHeaders());
		}

		@Override
		protected CompletableFuture<TestResponse> handleRequest(@Nonnull HandlerRequest<TestRequest, TestParameters> request, RestfulGateway gateway) throws RestHandlerException {
			Assert.assertEquals(request.getPathParameter(JobIDPathParameter.class), PATH_JOB_ID);
			Assert.assertEquals(request.getQueryParameter(JobIDQueryParameter.class).get(0), QUERY_JOB_ID);

			if (request.getRequestBody().id == 1) {
				synchronized (LOCK) {
					try {
						LOCK.notifyAll();
						LOCK.wait();
					} catch (InterruptedException ignored) {
					}
				}
			}
			return CompletableFuture.completedFuture(new TestResponse(request.getRequestBody().id));
		}
	}

	private static class TestRestClient extends RestClient {

		TestRestClient(RestClientConfiguration configuration) {
			super(configuration, TestingUtils.defaultExecutor());
		}
	}

	private static class TestRequest implements RequestBody {
		public final int id;

		@JsonCreator
		public TestRequest(@JsonProperty("id") int id) {
			this.id = id;
		}
	}

	private static class TestResponse implements ResponseBody {
		public final int id;

		@JsonCreator
		public TestResponse(@JsonProperty("id") int id) {
			this.id = id;
		}
	}

	private static class TestHeaders implements MessageHeaders<TestRequest, TestResponse, TestParameters> {

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.POST;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "/test/:jobid";
		}

		@Override
		public Class<TestRequest> getRequestClass() {
			return TestRequest.class;
		}

		@Override
		public Class<TestResponse> getResponseClass() {
			return TestResponse.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public TestParameters getUnresolvedMessageParameters() {
			return new TestParameters();
		}
	}

	private static class TestParameters extends MessageParameters {
		private final JobIDPathParameter jobIDPathParameter = new JobIDPathParameter();
		private final JobIDQueryParameter jobIDQueryParameter = new JobIDQueryParameter();

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.singleton(jobIDPathParameter);
		}

		@Override
		public Collection<MessageQueryParameter<?>> getQueryParameters() {
			return Collections.singleton(jobIDQueryParameter);
		}
	}

	private static class FaultyTestParameters extends TestParameters {
		private final FaultyJobIDPathParameter faultyJobIDPathParameter = new FaultyJobIDPathParameter();

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.singleton(faultyJobIDPathParameter);
		}
	}

	private static class TestWebSocketOperation {

		private static class WsParameters extends MessageParameters {
			private final JobIDPathParameter jobIDPathParameter = new JobIDPathParameter();

			@Override
			public Collection<MessagePathParameter<?>> getPathParameters() {
				return Collections.singleton(jobIDPathParameter);
			}

			@Override
			public Collection<MessageQueryParameter<?>> getQueryParameters() {
				return Collections.emptyList();
			}
		}

		static class WsHeaders implements WebSocketSpecification<WsParameters, TestMessage, TestMessage> {

			@Override
			public String getTargetRestEndpointURL() {
				return "/test/:jobid/subscribe";
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
			public WsParameters getUnresolvedMessageParameters() {
				return new WsParameters();
			}
		}

		static class WsRestHandler extends AbstractWebSocketHandler<RestfulGateway, WsParameters, TestMessage, TestMessage> {

			public static final CountDownLatch LATCH = new CountDownLatch(1);

			private final TestEventProvider eventProvider;

			WsRestHandler(
				CompletableFuture<String> localAddressFuture,
				GatewayRetriever<RestfulGateway> leaderRetriever,
				TestEventProvider eventProvider,
				Time timeout) {
				super(localAddressFuture, leaderRetriever, timeout, new WsHeaders());
				this.eventProvider = eventProvider;
			}

			@Override
			protected CompletableFuture<Void> handshakeInitiated(ChannelHandlerContext ctx, WsParameters parameters) throws Exception {
				// validate request before completing the handshake
				JobID jobID = parameters.getPathParameter(JobIDPathParameter.class);
				Assert.assertEquals(PATH_JOB_ID, jobID);
				return CompletableFuture.completedFuture(null);
			}

			@Override
			protected void handshakeCompleted(ChannelHandlerContext ctx, WsParameters parameters) throws Exception {
				// handshake complete; register for server events
				JobID jobID = parameters.getPathParameter(JobIDPathParameter.class);
				eventProvider.register(ctx.channel(), jobID);
				LATCH.countDown();
			}

			@Override
			protected void messageReceived(ChannelHandlerContext ctx, WsParameters parameters, TestMessage msg) throws Exception {
				ctx.channel().writeAndFlush(msg.incr()).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
			}
		}
	}

	static class JobIDPathParameter extends MessagePathParameter<JobID> {
		JobIDPathParameter() {
			super(JOB_ID_KEY);
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

	static class FaultyJobIDPathParameter extends MessagePathParameter<JobID> {

		FaultyJobIDPathParameter() {
			super(JOB_ID_KEY);
		}

		@Override
		protected JobID convertFromString(String value) throws ConversionException {
			return JobID.fromHexString(value);
		}

		@Override
		protected String convertToString(JobID value) {
			return "foobar";
		}
	}

	static class JobIDQueryParameter extends MessageQueryParameter<JobID> {
		JobIDQueryParameter() {
			super(JOB_ID_KEY, MessageParameterRequisiteness.MANDATORY);
		}

		@Override
		public JobID convertValueFromString(String value) {
			return JobID.fromHexString(value);
		}

		@Override
		public String convertStringToValue(JobID value) {
			return value.toString();
		}
	}

	static class TestMessage implements ResponseBody, RequestBody {
		public final int sequenceNumber;

		@JsonCreator
		public TestMessage(@JsonProperty("sequenceNumber") int sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}

		public TestMessage incr() {
			return new TestMessage(sequenceNumber + 1);
		}
	}
}
