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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyResponseBody;
import org.apache.flink.runtime.rest.messages.JobClientHeartbeatParameters;
import org.apache.flink.runtime.rest.messages.JobClientHeartbeatRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Receive the heartbeat from the client. */
public class JobClientHeartbeatHandler
        extends AbstractRestHandler<
                RestfulGateway,
                JobClientHeartbeatRequestBody,
                EmptyResponseBody,
                JobClientHeartbeatParameters> {
    private static final Logger LOG = LoggerFactory.getLogger(JobClientHeartbeatHandler.class);

    public JobClientHeartbeatHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            MessageHeaders<
                            JobClientHeartbeatRequestBody,
                            EmptyResponseBody,
                            JobClientHeartbeatParameters>
                    messageHeaders) {
        super(leaderRetriever, timeout, headers, messageHeaders);
    }

    @Override
    public CompletableFuture<EmptyResponseBody> handleRequest(
            HandlerRequest<JobClientHeartbeatRequestBody> request, RestfulGateway gateway)
            throws RestHandlerException {
        return gateway.reportJobClientHeartbeat(
                        request.getPathParameter(JobIDPathParameter.class),
                        request.getRequestBody().getExpiredTimestamp(),
                        timeout)
                .handle(
                        (Void ack, Throwable error) -> {
                            if (error != null) {
                                String errorMessage =
                                        "Fail to report jobClient's heartbeat: "
                                                + error.getMessage();
                                LOG.error(errorMessage, error);
                                throw new CompletionException(
                                        new RestHandlerException(
                                                errorMessage,
                                                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                error));
                            } else {
                                return EmptyResponseBody.getInstance();
                            }
                        });
    }
}
