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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.queue.QueueStatus;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.TestingRestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link AbstractAsynchronousOperationHandlers}.
 */
public class AbstractAsynchronousOperationHandlersTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private TestingAsynchronousOperationHandlers testingAsynchronousOperationHandlers;

	private TestingAsynchronousOperationHandlers.TestingTriggerHandler testingTriggerHandler;

	private TestingAsynchronousOperationHandlers.TestingStatusHandler testingStatusHandler;

	@Before
	public void setup() {
		testingAsynchronousOperationHandlers = new TestingAsynchronousOperationHandlers();

		testingTriggerHandler = testingAsynchronousOperationHandlers.new TestingTriggerHandler(
			() -> null,
			TIMEOUT,
			Collections.emptyMap(),
			TestingTriggerMessageHeaders.INSTANCE);

		testingStatusHandler = testingAsynchronousOperationHandlers.new TestingStatusHandler(
			() -> null,
			TIMEOUT,
			Collections.emptyMap(),
			TestingStatusMessageHeaders.INSTANCE);
	}

	/**
	 * Tests the triggering and successful completion of an asynchronous operation.
	 */
	@Test
	public void testOperationCompletion() throws Exception {
		final CompletableFuture<String> savepointFuture = new CompletableFuture<>();
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder()
			.setTriggerSavepointFunction((JobID jobId, String directory) -> savepointFuture)
			.build();

		// trigger the operation
		final TriggerId triggerId = testingTriggerHandler.handleRequest(
			triggerOperationRequest(),
			testingRestfulGateway).get().getTriggerId();

		AsynchronousOperationResult<OperationResult> operationResult = testingStatusHandler.handleRequest(
			statusOperationRequest(triggerId),
			testingRestfulGateway).get();

		assertThat(operationResult.queueStatus().getId(), is(QueueStatus.inProgress().getId()));

		// complete the operation
		final String savepointPath = "foobar";
		savepointFuture.complete(savepointPath);

		operationResult = testingStatusHandler.handleRequest(
			statusOperationRequest(triggerId),
			testingRestfulGateway).get();

		assertThat(operationResult.queueStatus().getId(), is(QueueStatus.completed().getId()));

		assertThat(operationResult.resource().value, is(savepointPath));
	}

	/**
	 * Tests the triggering and exceptional completion of an asynchronous operation.
	 */
	@Test
	public void testOperationFailure() throws Exception {
		final FlinkException testException = new FlinkException("Test exception");
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder()
			.setTriggerSavepointFunction((JobID jobId, String directory) -> FutureUtils.completedExceptionally(testException))
			.build();

		// trigger the operation
		final TriggerId triggerId = testingTriggerHandler.handleRequest(
			triggerOperationRequest(),
			testingRestfulGateway).get().getTriggerId();

		AsynchronousOperationResult<OperationResult> operationResult = testingStatusHandler.handleRequest(
			statusOperationRequest(triggerId),
			testingRestfulGateway).get();

		assertThat(operationResult.queueStatus().getId(), is(QueueStatus.completed().getId()));

		final OperationResult resource = operationResult.resource();
		assertThat(resource.throwable, is(testException));
	}

	/**
	 * Tests that an querying an unknown trigger id will return an exceptionally completed
	 * future.
	 */
	@Test
	public void testUnknownTriggerId() throws Exception {
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder().build();

		try {
			testingStatusHandler.handleRequest(
				statusOperationRequest(new TriggerId()),
				testingRestfulGateway).get();

			fail("This should have failed with a RestHandlerException.");
		} catch (ExecutionException ee) {
			final Optional<RestHandlerException> optionalRestHandlerException = ExceptionUtils.findThrowable(ee, RestHandlerException.class);

			assertThat(optionalRestHandlerException.isPresent(), is(true));

			final RestHandlerException restHandlerException = optionalRestHandlerException.get();

			assertThat(restHandlerException.getMessage(), containsString("Operation not found"));
			assertThat(restHandlerException.getHttpResponseStatus(), is(HttpResponseStatus.NOT_FOUND));
		}
	}

	/**
	 * Tests that the future returned by {@link AbstractAsynchronousOperationHandlers.StatusHandler#closeAsync()}
	 * completes when the result of the asynchronous operation is served.
	 */
	@Test
	public void testCloseShouldFinishOnFirstServedResult() throws Exception {
		final CompletableFuture<String> savepointFuture = new CompletableFuture<>();
		final TestingRestfulGateway testingRestfulGateway = new TestingRestfulGateway.Builder()
			.setTriggerSavepointFunction((JobID jobId, String directory) -> savepointFuture)
			.build();

		final TriggerId triggerId = testingTriggerHandler.handleRequest(
			triggerOperationRequest(),
			testingRestfulGateway).get().getTriggerId();
		final CompletableFuture<Void> closeFuture = testingStatusHandler.closeAsync();

		testingStatusHandler.handleRequest(statusOperationRequest(triggerId), testingRestfulGateway).get();

		assertThat(closeFuture.isDone(), is(false));

		savepointFuture.complete("foobar");
		testingStatusHandler.handleRequest(statusOperationRequest(triggerId), testingRestfulGateway).get();

		assertThat(closeFuture.isDone(), is(true));
	}

	private static HandlerRequest<EmptyRequestBody, EmptyMessageParameters> triggerOperationRequest() throws HandlerRequestException {
		return new HandlerRequest<>(EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance());
	}

	private static HandlerRequest<EmptyRequestBody, TriggerMessageParameters> statusOperationRequest(TriggerId triggerId) throws HandlerRequestException {
		return new HandlerRequest<>(
			EmptyRequestBody.getInstance(),
			new TriggerMessageParameters(),
			Collections.singletonMap(TriggerIdPathParameter.KEY, triggerId.toString()),
			Collections.emptyMap());
	}

	private static final class TestOperationKey extends OperationKey {

		protected TestOperationKey(TriggerId triggerId) {
			super(triggerId);
		}
	}

	private static final class TriggerMessageParameters extends MessageParameters {

		private final TriggerIdPathParameter triggerIdPathParameter = new TriggerIdPathParameter();

		@Override
		public Collection<MessagePathParameter<?>> getPathParameters() {
			return Collections.singleton(triggerIdPathParameter);
		}

		@Override
		public Collection<MessageQueryParameter<?>> getQueryParameters() {
			return Collections.emptyList();
		}
	}

	private static final class OperationResult {
		@Nullable
		private final Throwable throwable;

		@Nullable
		private final String value;

		OperationResult(@Nullable String value, @Nullable Throwable throwable) {
			this.value = value;
			this.throwable = throwable;
		}
	}

	private static final class TestingTriggerMessageHeaders extends AsynchronousOperationTriggerMessageHeaders<EmptyRequestBody, EmptyMessageParameters> {

		static final TestingTriggerMessageHeaders INSTANCE = new TestingTriggerMessageHeaders();

		private TestingTriggerMessageHeaders() {}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public String getDescription() {
			return "";
		}

		@Override
		protected String getAsyncOperationDescription() {
			return "";
		}

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public EmptyMessageParameters getUnresolvedMessageParameters() {
			return EmptyMessageParameters.getInstance();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.POST;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "barfoo";
		}
	}

	private static final class TestingStatusMessageHeaders extends AsynchronousOperationStatusMessageHeaders<OperationResult, TriggerMessageParameters> {

		private static final TestingStatusMessageHeaders INSTANCE = new TestingStatusMessageHeaders();

		private TestingStatusMessageHeaders() {}

		@Override
		public Class<OperationResult> getValueClass() {
			return OperationResult.class;
		}

		@Override
		public HttpResponseStatus getResponseStatusCode() {
			return HttpResponseStatus.OK;
		}

		@Override
		public Class<EmptyRequestBody> getRequestClass() {
			return EmptyRequestBody.class;
		}

		@Override
		public TriggerMessageParameters getUnresolvedMessageParameters() {
			return new TriggerMessageParameters();
		}

		@Override
		public HttpMethodWrapper getHttpMethod() {
			return HttpMethodWrapper.GET;
		}

		@Override
		public String getTargetRestEndpointURL() {
			return "foobar";
		}

		@Override
		public String getDescription() {
			return "";
		}
	}

	private static final class TestingAsynchronousOperationHandlers extends AbstractAsynchronousOperationHandlers<TestOperationKey, String> {

		class TestingTriggerHandler extends TriggerHandler<RestfulGateway, EmptyRequestBody, EmptyMessageParameters> {

			protected TestingTriggerHandler(GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, MessageHeaders<EmptyRequestBody, TriggerResponse, EmptyMessageParameters> messageHeaders) {
				super(leaderRetriever, timeout, responseHeaders, messageHeaders);
			}

			@Override
			protected CompletableFuture<String> triggerOperation(HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, RestfulGateway gateway) throws RestHandlerException {
				return gateway.triggerSavepoint(new JobID(), null, false, timeout);
			}

			@Override
			protected TestOperationKey createOperationKey(HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request) {
				return new TestOperationKey(new TriggerId());
			}
		}

		class TestingStatusHandler extends StatusHandler<RestfulGateway, OperationResult, TriggerMessageParameters> {

			protected TestingStatusHandler(GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, MessageHeaders<EmptyRequestBody, AsynchronousOperationResult<OperationResult>, TriggerMessageParameters> messageHeaders) {
				super(leaderRetriever, timeout, responseHeaders, messageHeaders);
			}

			@Override
			protected TestOperationKey getOperationKey(HandlerRequest<EmptyRequestBody, TriggerMessageParameters> request) {
				final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);

				return new TestOperationKey(triggerId);
			}

			@Override
			protected OperationResult exceptionalOperationResultResponse(Throwable throwable) {
				return new OperationResult(null, throwable);
			}

			@Override
			protected OperationResult operationResultResponse(String operationResult) {
				return new OperationResult(operationResult, null);
			}
		}
	}

}
