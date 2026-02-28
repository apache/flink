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
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistRemoveMessageParameters;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistRemoveResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/** Handler for removing nodes from the management blocklist. */
public class BlocklistRemoveHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                BlocklistRemoveResponseBody,
                BlocklistRemoveMessageParameters> {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;

    public BlocklistRemoveHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            MessageHeaders<
                            EmptyRequestBody,
                            BlocklistRemoveResponseBody,
                            BlocklistRemoveMessageParameters>
                    messageHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever) {
        super(leaderRetriever, timeout, messageHeaders);
        this.resourceManagerRetriever = resourceManagerRetriever;
    }

    @Override
    protected CompletableFuture<BlocklistRemoveResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {

        final String nodeId =
                request.getPathParameter(
                        BlocklistRemoveMessageParameters.NodeIdPathParameter.class);

        return resourceManagerRetriever
                .getFuture()
                .thenCompose(
                        resourceManagerGateway ->
                                resourceManagerGateway.removeManagementBlockedNode(nodeId, timeout))
                .thenApply(
                        ignored ->
                                new BlocklistRemoveResponseBody(
                                        "Node "
                                                + nodeId
                                                + " successfully removed from management blocklist"))
                .exceptionally(
                        throwable -> {
                            throw new RuntimeException(
                                    "Failed to remove node from management blocklist: "
                                            + throwable.getMessage(),
                                    throwable);
                        });
    }
}
