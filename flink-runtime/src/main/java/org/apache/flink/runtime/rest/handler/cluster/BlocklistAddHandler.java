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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ManagementOptions;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/** Handler for adding nodes to the management blocklist. */
public class BlocklistAddHandler
        extends AbstractRestHandler<
                RestfulGateway,
                BlocklistAddRequestBody,
                BlocklistAddResponseBody,
                EmptyMessageParameters> {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;
    private final Configuration configuration;

    public BlocklistAddHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            MessageHeaders<
                            BlocklistAddRequestBody,
                            BlocklistAddResponseBody,
                            EmptyMessageParameters>
                    messageHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever,
            Configuration configuration) {
        super(leaderRetriever, timeout, messageHeaders);
        this.resourceManagerRetriever = resourceManagerRetriever;
        this.configuration = configuration;
    }

    @Override
    protected CompletableFuture<BlocklistAddResponseBody> handleRequest(
            @Nonnull HandlerRequest<BlocklistAddRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {

        final BlocklistAddRequestBody requestBody = request.getRequestBody();

        // Use provided duration or default duration from configuration
        final Duration duration =
                requestBody.getDuration() != null
                        ? requestBody.getDuration()
                        : configuration.get(ManagementOptions.BLOCKLIST_DEFAULT_DURATION);

        // Validate duration against maximum allowed
        final Duration maxDuration = configuration.get(ManagementOptions.BLOCKLIST_MAX_DURATION);
        if (duration.compareTo(maxDuration) > 0) {
            throw new RestHandlerException(
                    "Requested duration "
                            + duration
                            + " exceeds maximum allowed duration "
                            + maxDuration,
                    org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus
                            .BAD_REQUEST);
        }

        return resourceManagerRetriever
                .getFuture()
                .thenCompose(
                        resourceManagerGateway ->
                                resourceManagerGateway.addManagementBlockedNode(
                                        requestBody.getNodeId(),
                                        requestBody.getReason(),
                                        duration,
                                        timeout))
                .thenApply(
                        ignored ->
                                new BlocklistAddResponseBody(
                                        "Node "
                                                + requestBody.getNodeId()
                                                + " successfully added to management blocklist for "
                                                + duration))
                .exceptionally(
                        throwable -> {
                            throw new RuntimeException(
                                    "Failed to add node to management blocklist: "
                                            + throwable.getMessage(),
                                    throwable);
                        });
    }
}
