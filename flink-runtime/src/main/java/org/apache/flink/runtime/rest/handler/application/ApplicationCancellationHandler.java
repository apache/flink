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

package org.apache.flink.runtime.rest.handler.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkApplicationNotFoundException;
import org.apache.flink.runtime.messages.FlinkApplicationTerminatedWithoutCancellationException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.ApplicationCancellationMessageParameters;
import org.apache.flink.runtime.rest.messages.ApplicationIDPathParameter;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

/** Request handler for the application cancellation request. */
public class ApplicationCancellationHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                EmptyResponseBody,
                ApplicationCancellationMessageParameters> {

    public ApplicationCancellationHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> headers,
            MessageHeaders<
                            EmptyRequestBody,
                            EmptyResponseBody,
                            ApplicationCancellationMessageParameters>
                    messageHeaders) {
        super(leaderRetriever, timeout, headers, messageHeaders);
    }

    @Override
    public CompletableFuture<EmptyResponseBody> handleRequest(
            HandlerRequest<EmptyRequestBody> request, RestfulGateway gateway)
            throws RestHandlerException {
        final ApplicationID applicationId =
                request.getPathParameter(ApplicationIDPathParameter.class);

        final CompletableFuture<Acknowledge> terminationFuture =
                gateway.cancelApplication(applicationId, timeout);

        return terminationFuture.handle(
                (Acknowledge ack, Throwable throwable) -> {
                    if (throwable != null) {
                        Throwable error = ExceptionUtils.stripCompletionException(throwable);

                        if (error
                                instanceof FlinkApplicationTerminatedWithoutCancellationException) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            String.format(
                                                    "Application cancellation failed because the application has already reached another terminal state (%s).",
                                                    ((FlinkApplicationTerminatedWithoutCancellationException)
                                                                    error)
                                                            .getApplicationStatus()),
                                            HttpResponseStatus.CONFLICT));
                        } else if (error instanceof TimeoutException) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            "Application cancellation timed out.",
                                            HttpResponseStatus.REQUEST_TIMEOUT,
                                            error));
                        } else if (error instanceof FlinkApplicationNotFoundException) {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            "Application could not be found.",
                                            HttpResponseStatus.NOT_FOUND,
                                            error));
                        } else {
                            throw new CompletionException(
                                    new RestHandlerException(
                                            "Application cancellation failed: "
                                                    + error.getMessage(),
                                            HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                            error));
                        }
                    } else {
                        return EmptyResponseBody.getInstance();
                    }
                });
    }
}
