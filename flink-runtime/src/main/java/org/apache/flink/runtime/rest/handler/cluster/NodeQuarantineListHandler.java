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
import org.apache.flink.runtime.resourcemanager.health.NodeHealthStatus;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineHeaders;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineListResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Handler for listing quarantined nodes via REST API. */
public class NodeQuarantineListHandler
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                NodeQuarantineListResponseBody,
                MessageParameters> {

    private final GatewayRetriever<? extends ResourceManagerGateway>
            resourceManagerGatewayRetriever;

    public NodeQuarantineListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            GatewayRetriever<? extends ResourceManagerGateway> resourceManagerGatewayRetriever) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                NodeQuarantineHeaders.ListQuarantinedNodesHeaders.INSTANCE);
        this.resourceManagerGatewayRetriever = resourceManagerGatewayRetriever;
    }

    @Override
    protected CompletableFuture<NodeQuarantineListResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {

        final ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway();

        return resourceManagerGateway
                .listQuarantinedNodes(timeout)
                .thenApply(this::convertToResponseBody);
    }

    private NodeQuarantineListResponseBody convertToResponseBody(
            Collection<NodeHealthStatus> nodeHealthStatuses) {
        final Collection<NodeQuarantineListResponseBody.NodeQuarantineInfo> quarantineInfos =
                nodeHealthStatuses.stream()
                        .map(
                                status ->
                                        new NodeQuarantineListResponseBody.NodeQuarantineInfo(
                                                status.getResourceID().toString(),
                                                status.getHostname(),
                                                status.getReason(),
                                                status.getQuarantineTimestamp(),
                                                status.getExpirationTimestamp()))
                        .collect(Collectors.toList());

        return new NodeQuarantineListResponseBody(quarantineInfos);
    }

    private ResourceManagerGateway getResourceManagerGateway() throws RestHandlerException {
        return resourceManagerGatewayRetriever
                .getNow()
                .orElseThrow(
                        () ->
                                new RestHandlerException(
                                        "ResourceManager not available",
                                        HttpResponseStatus.SERVICE_UNAVAILABLE));
    }
}
