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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.runtime.dispatcher.UnknownOperationKeyException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointInfo;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointStatusMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerMessageParameters;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointTriggerRequestBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedThrowable;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * HTTP handlers for asynchronous triggering of checkpoints.
 *
 * <p>Drawing checkpoints is a potentially long-running operation. To avoid blocking HTTP
 * connections, checkpoints must be drawn in two steps. First, an HTTP request is issued to trigger
 * the checkpoint asynchronously. The request will be assigned a {@link TriggerId}, which is
 * returned in the response body. Next, the returned {@link TriggerId} should be used to poll the
 * status of the checkpoint until it is finished.
 *
 * <p>A checkpoint is triggered by sending an HTTP {@code POST} request to {@code
 * /jobs/:jobid/checkpoints}. The HTTP request may contain a JSON body to specify a customized
 * {@link TriggerId} and a {@link CheckpointType}, e.g.,
 *
 * <pre>
 * { "triggerId": "7d273f5a62eb4730b9dea8e833733c1e", "checkpointType": "FULL" }
 * </pre>
 *
 * <p>If the body is omitted, or the field {@code checkpointType} is {@code null}, the default
 * checkpointType of {@link CheckpointType#CONFIGURED} will be used. As written above, the response
 * will contain a request id, e.g.,
 *
 * <pre>
 * { "request-id": "7d273f5a62eb4730b9dea8e833733c1e" }
 * </pre>
 *
 * <p>To poll for the status of an ongoing checkpoint, an HTTP {@code GET} request is issued to
 * {@code /jobs/:jobid/checkpoints/:checkpointtriggerid}. If the specified checkpoint is still
 * ongoing, the response will be
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "IN_PROGRESS"
 *     }
 * }
 * </pre>
 *
 * <p>If the specified checkpoints has completed, the status id will transition to {@code
 * COMPLETED}, and the response will additionally contain information about the savepoint, such as
 * the location:
 *
 * <pre>
 * {
 *     "status": {
 *         "id": "COMPLETED"
 *     },
 *     "operation": {
 *         "checkpointId": "123"
 *     }
 * }
 * </pre>
 */
public class CheckpointHandlers {

    /** Handler for the checkpoint trigger operation. */
    public static class CheckpointTriggerHandler
            extends AbstractRestHandler<
                    RestfulGateway,
                    CheckpointTriggerRequestBody,
                    TriggerResponse,
                    CheckpointTriggerMessageParameters> {

        public CheckpointTriggerHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Time timeout,
                final Map<String, String> responseHeaders) {
            super(
                    leaderRetriever,
                    timeout,
                    responseHeaders,
                    CheckpointTriggerHeaders.getInstance());
        }

        private static AsynchronousJobOperationKey createOperationKey(
                final HandlerRequest<CheckpointTriggerRequestBody> request) {
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(
                    request.getRequestBody().getTriggerId().orElseGet(TriggerId::new), jobId);
        }

        @Override
        protected CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull HandlerRequest<CheckpointTriggerRequestBody> request,
                @Nonnull RestfulGateway gateway)
                throws RestHandlerException {
            final AsynchronousJobOperationKey operationKey = createOperationKey(request);

            return gateway.triggerCheckpoint(
                            operationKey,
                            request.getRequestBody().getCheckpointType(),
                            RpcUtils.INF_TIMEOUT)
                    .handle(
                            (acknowledge, throwable) -> {
                                if (throwable == null) {
                                    return new TriggerResponse(operationKey.getTriggerId());
                                } else {
                                    throw new CompletionException(
                                            createInternalServerError(
                                                    throwable, operationKey, "triggering"));
                                }
                            });
        }
    }

    /** HTTP handler to query for the status of the checkpoint. */
    public static class CheckpointStatusHandler
            extends AbstractRestHandler<
                    RestfulGateway,
                    EmptyRequestBody,
                    AsynchronousOperationResult<CheckpointInfo>,
                    CheckpointStatusMessageParameters> {

        public CheckpointStatusHandler(
                final GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                final Time timeout,
                final Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, CheckpointStatusHeaders.getInstance());
        }

        @Override
        public CompletableFuture<AsynchronousOperationResult<CheckpointInfo>> handleRequest(
                @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
                throws RestHandlerException {

            final AsynchronousJobOperationKey key = getOperationKey(request);

            return gateway.getTriggeredCheckpointStatus(key)
                    .handle(
                            (operationResult, throwable) -> {
                                if (throwable == null) {
                                    switch (operationResult.getStatus()) {
                                        case SUCCESS:
                                            return AsynchronousOperationResult.completed(
                                                    operationResultResponse(
                                                            operationResult.getResult()));
                                        case FAILURE:
                                            return AsynchronousOperationResult.completed(
                                                    exceptionalOperationResultResponse(
                                                            operationResult.getThrowable()));
                                        case IN_PROGRESS:
                                            return AsynchronousOperationResult.inProgress();
                                        default:
                                            throw new IllegalStateException(
                                                    "No handler for operation status "
                                                            + operationResult.getStatus()
                                                            + ", encountered for key "
                                                            + key);
                                    }
                                } else {
                                    throw new CompletionException(
                                            maybeCreateNotFoundError(throwable, key)
                                                    .orElseGet(
                                                            () ->
                                                                    createInternalServerError(
                                                                            throwable,
                                                                            key,
                                                                            "retrieving status of")));
                                }
                            });
        }

        private static Optional<RestHandlerException> maybeCreateNotFoundError(
                Throwable throwable, AsynchronousJobOperationKey key) {
            if (ExceptionUtils.findThrowable(throwable, UnknownOperationKeyException.class)
                    .isPresent()) {
                return Optional.of(
                        new RestHandlerException(
                                String.format(
                                        "There is no checkpoint operation with triggerId=%s for job %s.",
                                        key.getTriggerId(), key.getJobId()),
                                HttpResponseStatus.NOT_FOUND));
            }
            return Optional.empty();
        }

        private static AsynchronousJobOperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody> request) {
            final TriggerId triggerId = request.getPathParameter(TriggerIdPathParameter.class);
            final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
            return AsynchronousJobOperationKey.of(triggerId, jobId);
        }

        private static CheckpointInfo exceptionalOperationResultResponse(
                final Throwable throwable) {
            return new CheckpointInfo(null, new SerializedThrowable(throwable));
        }

        private static CheckpointInfo operationResultResponse(final Long checkpointId) {
            return new CheckpointInfo(checkpointId, null);
        }
    }

    private static RestHandlerException createInternalServerError(
            Throwable throwable, AsynchronousJobOperationKey key, String errorMessageInfix) {
        return new RestHandlerException(
                String.format(
                        "Internal server error while %s checkpoint operation with triggerId=%s for job %s.",
                        errorMessageInfix, key.getTriggerId(), key.getJobId()),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                throwable);
    }
}
