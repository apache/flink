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
import org.apache.flink.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineHeaders;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for quarantining nodes via REST API.
 */
public class NodeQuarantineHandler extends AbstractRestHandler<RestfulGateway, NodeQuarantineRequestBody, NodeQuarantineResponseBody, NodeQuarantineHeaders.NodeIdMessageParameters> {

    private final GatewayRetriever<? extends ResourceManagerGateway> resourceManagerGatewayRetriever;

    public NodeQuarantineHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            GatewayRetriever<? extends ResourceManagerGateway> resourceManagerGatewayRetriever) {
        super(leaderRetriever, timeout, responseHeaders, NodeQuarantineHeaders.QuarantineNodeHeaders.INSTANCE);
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
    }

    @Override
    protected CompletableFuture<NodeQuarantineResponseBody> handleRequest(
            @Nonnull HandlerRequest<NodeQuarantineRequestBody> request,
            @Nonnull RestfulGateway gateway) throws RestHandlerException {

        final String nodeIdString = request.getPathParameter(NodeQuarantineHeaders.NodeIdPathParameter.class);
        final ResourceID resourceID = new ResourceID(nodeIdString);
        final NodeQuarantineRequestBody requestBody = request.getRequestBody();

        final String reason = requestBody.getReason();
        final Duration duration = requestBody.getDuration() != null ? 
            requestBody.getDuration() : Duration.ofMinutes(30); // Default 30 minutes

        final ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway();

        return resourceManagerGateway
                .quarantineNode(resourceID, reason, duration, timeout)
                .handle((Void result, Throwable throwable) -> {
                    if (throwable != null) {
                        return new NodeQuarantineResponseBody(
                                false, 
                                "Failed to quarantine node: " + throwable.getMessage());
                    } else {
                        return new NodeQuarantineResponseBody(
                                true, 
                                "Node " + nodeIdString + " has been quarantined successfully");
                    }
                });
    }

    private ResourceManagerGateway getResourceManagerGateway() throws RestHandlerException {
        return resourceManagerGatewayRetriever.getNow()
                .orElseThrow(() -> new RestHandlerException(
                        "ResourceManager not available", 
                        HttpResponseStatus.SERVICE_UNAVAILABLE));
    }
}