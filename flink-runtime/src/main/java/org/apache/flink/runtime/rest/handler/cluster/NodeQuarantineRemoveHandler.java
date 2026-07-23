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

import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineRemoveMessageParameters;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineRemoveResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Handler for removing nodes from the management node quarantine. */
public class NodeQuarantineRemoveHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                NodeQuarantineRemoveResponseBody,
                NodeQuarantineRemoveMessageParameters> {

    private final GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever;

    public NodeQuarantineRemoveHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Duration timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<
                            EmptyRequestBody,
                            NodeQuarantineRemoveResponseBody,
                            NodeQuarantineRemoveMessageParameters>
                    messageHeaders,
            GatewayRetriever<ResourceManagerGateway> resourceManagerRetriever) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.resourceManagerRetriever = resourceManagerRetriever;
    }

    @Override
    protected CompletableFuture<NodeQuarantineRemoveResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {

        final String nodeId =
                request.getPathParameter(
                        NodeQuarantineRemoveMessageParameters.NodeIdPathParameter.class);

        return resourceManagerRetriever
                .getFuture()
                .thenCompose(
                        resourceManagerGateway ->
                                resourceManagerGateway.removeManagementQuarantinedNode(
                                        nodeId, timeout))
                .thenApply(
                        ignored ->
                                new NodeQuarantineRemoveResponseBody(
                                        "Node "
                                                + nodeId
                                                + " successfully removed from management node quarantine"))
                .exceptionally(
                        throwable -> {
                            throw new RuntimeException(
                                    "Failed to remove node from management node quarantine: "
                                            + throwable.getMessage(),
                                    throwable);
                        });
    }
}
