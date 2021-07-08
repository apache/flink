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

package org.apache.flink.runtime.rest.handler.job.rescaling;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.async.AbstractAsynchronousOperationHandlers;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationResult;
import org.apache.flink.runtime.rest.handler.async.TriggerResponse;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Rest handler to trigger and poll the rescaling of a running job. */
public class RescalingHandlers
        extends AbstractAsynchronousOperationHandlers<AsynchronousJobOperationKey, Acknowledge> {

    private static RestHandlerException featureDisabledException() {
        return new RestHandlerException(
                "Rescaling is temporarily disabled. See FLINK-12312.",
                HttpResponseStatus.SERVICE_UNAVAILABLE);
    }

    /** Handler which triggers the rescaling of the specified job. */
    public class RescalingTriggerHandler
            extends TriggerHandler<
                    RestfulGateway, EmptyRequestBody, RescalingTriggerMessageParameters> {

        public RescalingTriggerHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, RescalingTriggerHeaders.getInstance());
        }

        @Override
        public CompletableFuture<TriggerResponse> handleRequest(
                @Nonnull
                        final HandlerRequest<EmptyRequestBody, RescalingTriggerMessageParameters>
                                request,
                @Nonnull final RestfulGateway gateway)
                throws RestHandlerException {
            throw featureDisabledException();
        }

        @Override
        protected CompletableFuture<Acknowledge> triggerOperation(
                HandlerRequest<EmptyRequestBody, RescalingTriggerMessageParameters> request,
                RestfulGateway gateway) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AsynchronousJobOperationKey createOperationKey(
                HandlerRequest<EmptyRequestBody, RescalingTriggerMessageParameters> request) {
            throw new UnsupportedOperationException();
        }
    }

    /** Handler which reports the status of the rescaling operation. */
    public class RescalingStatusHandler
            extends StatusHandler<
                    RestfulGateway, AsynchronousOperationInfo, RescalingStatusMessageParameters> {

        public RescalingStatusHandler(
                GatewayRetriever<? extends RestfulGateway> leaderRetriever,
                Time timeout,
                Map<String, String> responseHeaders) {
            super(leaderRetriever, timeout, responseHeaders, RescalingStatusHeaders.getInstance());
        }

        @Override
        public CompletableFuture<AsynchronousOperationResult<AsynchronousOperationInfo>>
                handleRequest(
                        @Nonnull
                                final HandlerRequest<
                                                EmptyRequestBody, RescalingStatusMessageParameters>
                                        request,
                        @Nonnull final RestfulGateway gateway)
                        throws RestHandlerException {
            throw featureDisabledException();
        }

        @Override
        protected AsynchronousJobOperationKey getOperationKey(
                HandlerRequest<EmptyRequestBody, RescalingStatusMessageParameters> request) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AsynchronousOperationInfo exceptionalOperationResultResponse(
                Throwable throwable) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AsynchronousOperationInfo operationResultResponse(Acknowledge operationResult) {
            throw new UnsupportedOperationException();
        }
    }
}
