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
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

/** Handler for adding nodes to the blocklist. */
public class BlocklistAddHandler
        extends AbstractRestHandler<
                RestfulGateway,
                BlocklistAddRequestBody,
                BlocklistAddResponseBody,
                EmptyMessageParameters> {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;

    public BlocklistAddHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            MessageHeaders<
                            BlocklistAddRequestBody,
                            BlocklistAddResponseBody,
                            EmptyMessageParameters>
                    messageHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever) {
        super(leaderRetriever, timeout, messageHeaders);
        this.resourceManagerRetriever = resourceManagerRetriever;
    }

    @Override
    protected CompletableFuture<BlocklistAddResponseBody> handleRequest(
            @Nonnull HandlerRequest<BlocklistAddRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {

        final BlocklistAddRequestBody requestBody = request.getRequestBody();

        return resourceManagerRetriever
                .getFuture()
                .thenCompose(
                        resourceManagerGateway ->
                                resourceManagerGateway.addBlockedNode(
                                        requestBody.getNodeId(),
                                        requestBody.getCause(),
                                        requestBody.getEndTimestamp(),
                                        timeout))
                .thenApply(
                        ignored ->
                                new BlocklistAddResponseBody(
                                        "Node "
                                                + requestBody.getNodeId()
                                                + " successfully added to blocklist"))
                .exceptionally(
                        throwable -> {
                            throw new RuntimeException(
                                    "Failed to add node to blocklist: " + throwable.getMessage(),
                                    throwable);
                        });
    }
}
