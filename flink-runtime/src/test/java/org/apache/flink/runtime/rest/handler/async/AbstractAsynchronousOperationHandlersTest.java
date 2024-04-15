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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.messages.Acknowledge;
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
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link AbstractAsynchronousOperationHandlers}. */
class AbstractAsynchronousOperationHandlersTest {

    private static final Time TIMEOUT = Time.seconds(10L);

    // Not actually used by the tests in this class, but required as a parameter
    private static final TestingRestfulGateway DUMMY_GATEWAY = new TestingRestfulGateway();

    private TestingAsynchronousOperationHandlers.TestingTriggerHandler testingTriggerHandler;

    private TestingAsynchronousOperationHandlers.TestingStatusHandler testingStatusHandler;

    @BeforeEach
    void setup() {
        TestingAsynchronousOperationHandlers testingAsynchronousOperationHandlers =
                new TestingAsynchronousOperationHandlers();

        testingTriggerHandler =
                testingAsynchronousOperationHandlers
                .new TestingTriggerHandler(
                        () -> null,
                        TIMEOUT,
                        Collections.emptyMap(),
                        TestingTriggerMessageHeaders.INSTANCE);

        testingStatusHandler =
                testingAsynchronousOperationHandlers
                .new TestingStatusHandler(
                        () -> null,
                        TIMEOUT,
                        Collections.emptyMap(),
                        TestingStatusMessageHeaders.INSTANCE);
    }

    /** Tests the triggering and successful completion of an asynchronous operation. */
    @Test
    void testOperationCompletion() throws Exception {
        final CompletableFuture<Acknowledge> acknowledgeFuture = new CompletableFuture<>();
        testingTriggerHandler.setGatewayCallback((request, gateway) -> acknowledgeFuture);

        // trigger the operation
        final TriggerId triggerId =
                testingTriggerHandler
                        .handleRequest(triggerOperationRequest(), DUMMY_GATEWAY)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<OperationResult> operationResult =
                testingStatusHandler
                        .handleRequest(statusOperationRequest(triggerId), DUMMY_GATEWAY)
                        .get();

        assertThat(operationResult.queueStatus().getId())
                .isEqualTo(QueueStatus.inProgress().getId());

        // complete the operation
        acknowledgeFuture.complete(Acknowledge.get());

        operationResult =
                testingStatusHandler
                        .handleRequest(statusOperationRequest(triggerId), DUMMY_GATEWAY)
                        .get();

        assertThat(operationResult.queueStatus().getId())
                .isEqualTo(QueueStatus.completed().getId());

        assertThat(operationResult.resource().value).isEqualTo(Acknowledge.get());
    }

    /** Tests the triggering and exceptional completion of an asynchronous operation. */
    @Test
    void testOperationFailure() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        testingTriggerHandler.setGatewayCallback(
                (request, gateway) -> FutureUtils.completedExceptionally(testException));

        // trigger the operation
        final TriggerId triggerId =
                testingTriggerHandler
                        .handleRequest(triggerOperationRequest(), DUMMY_GATEWAY)
                        .get()
                        .getTriggerId();

        AsynchronousOperationResult<OperationResult> operationResult =
                testingStatusHandler
                        .handleRequest(statusOperationRequest(triggerId), DUMMY_GATEWAY)
                        .get();

        assertThat(operationResult.queueStatus().getId())
                .isEqualTo(QueueStatus.completed().getId());

        final OperationResult resource = operationResult.resource();
        assertThat(resource.throwable).isEqualTo(testException);
    }

    /**
     * Tests that an querying an unknown trigger id will return an exceptionally completed future.
     */
    @Test
    void testUnknownTriggerId() throws Exception {
        try {
            testingStatusHandler
                    .handleRequest(statusOperationRequest(new TriggerId()), DUMMY_GATEWAY)
                    .get();

            fail("This should have failed with a RestHandlerException.");
        } catch (ExecutionException ee) {
            final Optional<RestHandlerException> optionalRestHandlerException =
                    ExceptionUtils.findThrowable(ee, RestHandlerException.class);

            assertThat(optionalRestHandlerException).isPresent();

            final RestHandlerException restHandlerException = optionalRestHandlerException.get();

            assertThat(restHandlerException.getMessage()).contains("Operation not found");
            assertThat(restHandlerException.getHttpResponseStatus())
                    .isEqualTo(HttpResponseStatus.NOT_FOUND);
        }
    }

    /**
     * Tests that the future returned by {@link
     * AbstractAsynchronousOperationHandlers.StatusHandler#closeAsync()} completes when the result
     * of the asynchronous operation is served.
     */
    @Test
    void testCloseShouldFinishOnFirstServedResult() throws Exception {
        final CompletableFuture<Acknowledge> acknowledgeFuture = new CompletableFuture<>();
        testingTriggerHandler.setGatewayCallback((request, gateway) -> acknowledgeFuture);

        final TriggerId triggerId =
                testingTriggerHandler
                        .handleRequest(triggerOperationRequest(), DUMMY_GATEWAY)
                        .get()
                        .getTriggerId();
        final CompletableFuture<Void> closeFuture = testingStatusHandler.closeAsync();

        testingStatusHandler.handleRequest(statusOperationRequest(triggerId), DUMMY_GATEWAY).get();

        assertThat(closeFuture).isNotDone();

        acknowledgeFuture.complete(Acknowledge.get());
        testingStatusHandler.handleRequest(statusOperationRequest(triggerId), DUMMY_GATEWAY).get();

        assertThat(closeFuture).isDone();
    }

    private static HandlerRequest<EmptyRequestBody> triggerOperationRequest() {
        return HandlerRequest.create(
                EmptyRequestBody.getInstance(), EmptyMessageParameters.getInstance());
    }

    private static HandlerRequest<EmptyRequestBody> statusOperationRequest(TriggerId triggerId)
            throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new TriggerMessageParameters(),
                Collections.singletonMap(TriggerIdPathParameter.KEY, triggerId.toString()),
                Collections.emptyMap(),
                Collections.emptyList());
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
        @Nullable private final Throwable throwable;

        @Nullable private final Acknowledge value;

        OperationResult(@Nullable Acknowledge value, @Nullable Throwable throwable) {
            this.value = value;
            this.throwable = throwable;
        }
    }

    private static final class TestingTriggerMessageHeaders
            extends AsynchronousOperationTriggerMessageHeaders<
                    EmptyRequestBody, EmptyMessageParameters> {

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

    private static final class TestingStatusMessageHeaders
            extends AsynchronousOperationStatusMessageHeaders<
                    OperationResult, TriggerMessageParameters> {

        private static final TestingStatusMessageHeaders INSTANCE =
                new TestingStatusMessageHeaders();

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

    private static final class TestingAsynchronousOperationHandlers
            extends AbstractAsynchronousOperationHandlers<TestOperationKey, Acknowledge> {

        protected TestingAsynchronousOperationHandlers() {
            super(RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue());
        }

        class TestingTriggerHandler
                extends TriggerHandler<RestfulGateway, EmptyRequestBody, EmptyMessageParameters> {

            protected TestingTriggerHandler(
                    GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                    Time timeout,
                    Map<String, String> responseHeaders,
                    MessageHeaders<EmptyRequestBody, TriggerResponse, EmptyMessageParameters>
                            messageHeaders) {
                super(leaderRetriever, timeout, responseHeaders, messageHeaders);
            }

            private BiFunction<
                            HandlerRequest<EmptyRequestBody>,
                            RestfulGateway,
                            CompletableFuture<Acknowledge>>
                    gatewayCallback =
                            (handlerRequest, restfulGateway) -> {
                                throw new UnsupportedOperationException();
                            };

            public void setGatewayCallback(
                    BiFunction<
                                    HandlerRequest<EmptyRequestBody>,
                                    RestfulGateway,
                                    CompletableFuture<Acknowledge>>
                            callback) {
                this.gatewayCallback = callback;
            }

            @Override
            protected CompletableFuture<Acknowledge> triggerOperation(
                    HandlerRequest<EmptyRequestBody> request, RestfulGateway gateway)
                    throws RestHandlerException {
                return gatewayCallback.apply(request, gateway);
            }

            @Override
            protected TestOperationKey createOperationKey(
                    HandlerRequest<EmptyRequestBody> request) {
                return new TestOperationKey(new TriggerId());
            }
        }

        class TestingStatusHandler
                extends StatusHandler<RestfulGateway, OperationResult, TriggerMessageParameters> {

            protected TestingStatusHandler(
                    GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                    Time timeout,
                    Map<String, String> responseHeaders,
                    MessageHeaders<
                                    EmptyRequestBody,
                                    AsynchronousOperationResult<OperationResult>,
                                    TriggerMessageParameters>
                            messageHeaders) {
                super(leaderRetriever, timeout, responseHeaders, messageHeaders);
            }

            @Override
            protected TestOperationKey getOperationKey(HandlerRequest<EmptyRequestBody> request) {
                final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);

                return new TestOperationKey(triggerId);
            }

            @Override
            protected OperationResult exceptionalOperationResultResponse(Throwable throwable) {
                return new OperationResult(null, throwable);
            }

            @Override
            protected OperationResult operationResultResponse(Acknowledge operationResult) {
                return new OperationResult(operationResult, null);
            }
        }
    }
}
