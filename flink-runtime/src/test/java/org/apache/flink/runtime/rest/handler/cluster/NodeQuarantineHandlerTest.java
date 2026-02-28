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
import org.apache.flink.runtime.resourcemanager.health.NodeHealthStatus;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineHeaders;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineListResponseBody;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.NodeQuarantineResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NodeQuarantineHandler}, {@link NodeRemoveQuarantineHandler}, and {@link NodeQuarantineListHandler}.
 */
class NodeQuarantineHandlerTest extends TestLogger {

    private static final Time TIMEOUT = Time.seconds(10);
    private static final Map<String, String> RESPONSE_HEADERS = Collections.emptyMap();

    private ResourceManagerGateway resourceManagerGateway;
    private GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
    private GatewayRetriever<RestfulGateway> leaderRetriever;

    @BeforeEach
    void setUp() {
        resourceManagerGateway = mock(ResourceManagerGateway.class);
        resourceManagerGatewayRetriever = mock(GatewayRetriever.class);
        leaderRetriever = mock(GatewayRetriever.class);

        when(resourceManagerGatewayRetriever.getNow()).thenReturn(Optional.of(resourceManagerGateway));
        when(leaderRetriever.getNow()).thenReturn(Optional.of(mock(RestfulGateway.class)));
    }

    @Test
    void testQuarantineNode() throws Exception {
        // Setup
        final ResourceID resourceID = ResourceID.generate();
        final String reason = "Node failure detected";
        final Duration duration = Duration.ofMinutes(30);

        when(resourceManagerGateway.quarantineNode(eq(resourceID), eq(reason), eq(duration), any(Duration.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        final NodeQuarantineHandler handler = new NodeQuarantineHandler(
                leaderRetriever, TIMEOUT, RESPONSE_HEADERS, resourceManagerGatewayRetriever);

        final NodeQuarantineRequestBody requestBody = new NodeQuarantineRequestBody(reason, duration);
        final HandlerRequest<NodeQuarantineRequestBody> request = createQuarantineRequest(resourceID.toString(), requestBody);

        // Execute
        final CompletableFuture<NodeQuarantineResponseBody> responseFuture = 
                handler.handleRequest(request, mock(RestfulGateway.class));
        final NodeQuarantineResponseBody response = responseFuture.get();

        // Verify
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getMessage()).contains("quarantined successfully");
        verify(resourceManagerGateway).quarantineNode(eq(resourceID), eq(reason), eq(duration), any(Duration.class));
    }

    @Test
    void testRemoveQuarantine() throws Exception {
        // Setup
        final ResourceID resourceID = ResourceID.generate();

        when(resourceManagerGateway.removeNodeQuarantine(eq(resourceID), any(Duration.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        final NodeRemoveQuarantineHandler handler = new NodeRemoveQuarantineHandler(
                leaderRetriever, TIMEOUT, RESPONSE_HEADERS, resourceManagerGatewayRetriever);

        final HandlerRequest<EmptyRequestBody> request = createRemoveQuarantineRequest(resourceID.toString());

        // Execute
        final CompletableFuture<NodeQuarantineResponseBody> responseFuture = 
                handler.handleRequest(request, mock(RestfulGateway.class));
        final NodeQuarantineResponseBody response = responseFuture.get();

        // Verify
        assertThat(response.isSuccess()).isTrue();
        assertThat(response.getMessage()).contains("removed");
        verify(resourceManagerGateway).removeNodeQuarantine(eq(resourceID), any(Duration.class));
    }

    @Test
    void testListQuarantinedNodes() throws Exception {
        // Setup
        final ResourceID resourceID1 = ResourceID.generate();
        final ResourceID resourceID2 = ResourceID.generate();
        final Collection<NodeHealthStatus> nodeHealthStatuses = Arrays.asList(
                new NodeHealthStatus(resourceID1, "host1", "reason1", 1000L, 2000L),
                new NodeHealthStatus(resourceID2, "host2", "reason2", 1500L, 2500L)
        );

        when(resourceManagerGateway.listQuarantinedNodes(any(Duration.class)))
                .thenReturn(CompletableFuture.completedFuture(nodeHealthStatuses));

        final NodeQuarantineListHandler handler = new NodeQuarantineListHandler(
                leaderRetriever, TIMEOUT, RESPONSE_HEADERS, resourceManagerGatewayRetriever);

        final HandlerRequest<EmptyRequestBody> request = createListRequest();

        // Execute
        final CompletableFuture<NodeQuarantineListResponseBody> responseFuture = 
                handler.handleRequest(request, mock(RestfulGateway.class));
        final NodeQuarantineListResponseBody response = responseFuture.get();

        // Verify
        assertThat(response.getQuarantinedNodes()).hasSize(2);
        verify(resourceManagerGateway).listQuarantinedNodes(any(Duration.class));
    }

    private HandlerRequest<NodeQuarantineRequestBody> createQuarantineRequest(
            String nodeId, NodeQuarantineRequestBody requestBody) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(NodeQuarantineHeaders.NodeIdPathParameter.KEY, nodeId);

        return HandlerRequest.resolveParametersAndCreate(
                requestBody,
                new NodeQuarantineHeaders.NodeIdPathParameter(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private HandlerRequest<EmptyRequestBody> createRemoveQuarantineRequest(String nodeId) throws HandlerRequestException {
        final Map<String, String> pathParameters = new HashMap<>();
        pathParameters.put(NodeQuarantineHeaders.NodeIdPathParameter.KEY, nodeId);

        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new NodeQuarantineHeaders.NodeIdPathParameter(),
                pathParameters,
                Collections.emptyMap(),
                Collections.emptyList());
    }

    private HandlerRequest<EmptyRequestBody> createListRequest() throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                NodeQuarantineHeaders.EmptyMessageParameters.getInstance(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }
}