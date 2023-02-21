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
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * HTTP handlers for asynchronous operations.
 *
 * <p>Some operations are long-running. To avoid blocking HTTP connections, these operations are
 * executed in two steps. First, an HTTP request is issued to trigger the operation asynchronously.
 * The request will be assigned a trigger id, which is returned in the response body. Next, the
 * returned id should be used to poll the status of the operation until it is finished.
 *
 * <p>An operation is triggered by sending an HTTP {@code POST} request to a registered {@code url}.
 * The HTTP request may contain a JSON body to specify additional parameters, e.g.,
 *
 * <pre>
 * { "target-directory": "/tmp" }
 * </pre>
 *
 * <p>As written above, the response will contain a request id, e.g.,
 *
 * <pre>
 * { "request-id": "7d273f5a62eb4730b9dea8e833733c1e" }
 * </pre>
 *
 * <p>To poll for the status of an ongoing operation, an HTTP {@code GET} request is issued to
 * {@code url/:triggerid}. If the specified savepoint is still ongoing, the response will be
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "IN_PROGRESS"
 *     }
 * }
 * </pre>
 *
 * <p>If the specified operation has completed, the status id will transition to {@code COMPLETED},
 * and the response will additionally contain information about the operation result:
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "COMPLETED"
 *     },
 *     "operation": {
 *         "result": "/tmp/savepoint-d9813b-8a68e674325b"
 *     }
 * }
 * </pre>
 *
 * @param <K> type of the operation key under which the result future is stored
 * @param <R> type of the operation result
 */
public abstract class AbstractAsynchronousOperationHandlers<
        K extends OperationKey, R extends Serializable> {

    private final CompletedOperationCache<K, R> completedOperationCache;

    protected AbstractAsynchronousOperationHandlers(Duration cacheDuration) {
        completedOperationCache = new CompletedOperationCache<>(cacheDuration);
    }

    /**
     * Handler which is responsible for triggering an asynchronous operation. After the operation
     * has been triggered, it stores the result future in the {@link #completedOperationCache}.
     *
     * @param <T> type of the gateway
     * @param <B> type of the request
     * @param <M> type of the message parameters
     */
    protected abstract class TriggerHandler<
                    T extends RestfulGateway, B extends RequestBody, M extends MessageParameters>
            extends AbstractRestHandler<T, B, TriggerResponse, M> {

        protected TriggerHandler(
                GatewayRetriever<? extends T> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<B, TriggerResponse, M> messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        public CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<B> request, @Nonnull T gateway)
                throws RestHandlerException {
            final CompletableFuture<R> resultFuture = triggerOperation(request, gateway);

            final K operationKey = createOperationKey(request);

            completedOperationCache.registerOngoingOperation(operationKey, resultFuture);

            return CompletableFuture.completedFuture(
                    new TriggerResponse(operationKey.getTriggerId()));
        }

        /**
         * Trigger the asynchronous operation and return its future result.
         *
         * @param request with which the trigger handler has been called
         * @param gateway to the leader
         * @return Future result of the asynchronous operation
         * @throws RestHandlerException if something went wrong
         */
        protected abstract CompletableFuture<R> triggerOperation(
                HandlerRequest<B> request, T gateway) throws RestHandlerException;

        /**
         * Create the operation key under which the result future of the asynchronous operation will
         * be stored.
         *
         * @param request with which the trigger handler has been called.
         * @return Operation key under which the result future will be stored
         */
        protected abstract K createOperationKey(HandlerRequest<B> request);
    }

    /**
     * Handler which will be polled to retrieve the asynchronous operation's result. The handler
     * returns a {@link AsynchronousOperationResult} which indicates whether the operation is still
     * in progress or has completed. In case that the operation has been completed, the {@link
     * AsynchronousOperationResult} contains the operation result.
     *
     * @param <T> type of the leader gateway
     * @param <V> type of the operation result
     * @param <M> type of the message headers
     */
    protected abstract class StatusHandler<T extends RestfulGateway, V, M extends MessageParameters>
            extends AbstractRestHandler<T, EmptyRequestBody, AsynchronousOperationResult<V>, M> {

        protected StatusHandler(
                GatewayRetriever<? extends T> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders,
                MessageHeaders<EmptyRequestBody, AsynchronousOperationResult<V>, M>
                        messageHeaders) {
            super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        }

        @Override
        public CompletableFuture<AsynchronousOperationResult<V>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull T gateway)
                throws RestHandlerException {

            final K key = getOperationKey(request);

            final Optional<OperationResult<R>> operationResultOptional =
                    completedOperationCache.get(key);
            if (!operationResultOptional.isPresent()) {
                return FutureUtils.completedExceptionally(
                        new NotFoundException("Operation not found under key: " + key));
            }

            final OperationResult<R> operationResult = operationResultOptional.get();
            switch (operationResult.getStatus()) {
                case SUCCESS:
                    return CompletableFuture.completedFuture(
                            AsynchronousOperationResult.completed(
                                    operationResultResponse(operationResult.getResult())));
                case FAILURE:
                    return CompletableFuture.completedFuture(
                            AsynchronousOperationResult.completed(
                                    exceptionalOperationResultResponse(
                                            operationResult.getThrowable())));
                case IN_PROGRESS:
                    return CompletableFuture.completedFuture(
                            AsynchronousOperationResult.inProgress());
                default:
                    throw new IllegalStateException(
                            "No handler for operation status "
                                    + operationResult.getStatus()
                                    + ", encountered for key "
                                    + key);
            }
        }

        @Override
        public CompletableFuture<Void> closeHandlerAsync() {
            return completedOperationCache.closeAsync();
        }

        /**
         * Extract the operation key under which the operation result future is stored.
         *
         * @param request with which the status handler has been called
         * @return Operation key under which the operation result future is stored
         */
        protected abstract K getOperationKey(HandlerRequest<EmptyRequestBody> request);

        /**
         * Create an exceptional operation result from the given {@link Throwable}. This method is
         * called if the asynchronous operation failed.
         *
         * @param throwable failure of the asynchronous operation
         * @return Exceptional operation result
         */
        protected abstract V exceptionalOperationResultResponse(Throwable throwable);

        /**
         * Create the operation result from the given value.
         *
         * @param operationResult of the asynchronous operation
         * @return Operation result
         */
        protected abstract V operationResultResponse(R operationResult);
    }
}
