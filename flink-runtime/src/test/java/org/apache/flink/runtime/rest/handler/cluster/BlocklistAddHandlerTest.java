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

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddRequestBody;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlocklistAddHandler}. */
class BlocklistAddHandlerTest {

    private TestingResourceManagerGateway resourceManagerGateway;
    private BlocklistAddHandler handler;

    @BeforeEach
    void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
        handler =
                new BlocklistAddHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        BlocklistAddHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway));
    }

    @Test
    void testAddBlockedNode() throws Exception {
        final String nodeId = "test-node";
        final String reason = "Test reason";
        final Duration timeout = Duration.ofMinutes(30);

        // Set up the resource manager gateway to capture the call
        final CompletableFuture<String> capturedNodeId = new CompletableFuture<>();
        final CompletableFuture<String> capturedReason = new CompletableFuture<>();
        final CompletableFuture<Duration> capturedTimeout = new CompletableFuture<>();

        resourceManagerGateway.setAddManagementBlockedNodeFunction(
                (tuple) -> {
                    capturedNodeId.complete(tuple.f0);
                    capturedReason.complete(tuple.f1);
                    capturedTimeout.complete(tuple.f2);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        // Create request
        BlocklistAddRequestBody requestBody = new BlocklistAddRequestBody(nodeId, reason, timeout);
        HandlerRequest<BlocklistAddRequestBody> request = createRequest(requestBody);

        // Handle request
        Acknowledge response = handler.handleRequest(request, resourceManagerGateway).get();

        // Verify response
        assertThat(response).isEqualTo(Acknowledge.get());

        // Verify the resource manager was called with correct parameters
        assertThat(capturedNodeId.get()).isEqualTo(nodeId);
        assertThat(capturedReason.get()).isEqualTo(reason);
        assertThat(capturedTimeout.get()).isEqualTo(timeout);
    }

    @Test
    void testAddBlockedNodeWithDefaultTimeout() throws Exception {
        final String nodeId = "test-node";
        final String reason = "Test reason";

        // Set up the resource manager gateway to capture the call
        final CompletableFuture<String> capturedNodeId = new CompletableFuture<>();
        final CompletableFuture<String> capturedReason = new CompletableFuture<>();
        final CompletableFuture<Duration> capturedTimeout = new CompletableFuture<>();

        resourceManagerGateway.setAddManagementBlockedNodeFunction(
                (tuple) -> {
                    capturedNodeId.complete(tuple.f0);
                    capturedReason.complete(tuple.f1);
                    capturedTimeout.complete(tuple.f2);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        // Create request without timeout (should use default)
        BlocklistAddRequestBody requestBody = new BlocklistAddRequestBody(nodeId, reason, null);
        HandlerRequest<BlocklistAddRequestBody> request = createRequest(requestBody);

        // Handle request
        Acknowledge response = handler.handleRequest(request, resourceManagerGateway).get();

        // Verify response
        assertThat(response).isEqualTo(Acknowledge.get());

        // Verify the resource manager was called with correct parameters
        assertThat(capturedNodeId.get()).isEqualTo(nodeId);
        assertThat(capturedReason.get()).isEqualTo(reason);
        // Timeout should be null (will use default in ResourceManager)
        assertThat(capturedTimeout.get()).isNull();
    }

    private HandlerRequest<BlocklistAddRequestBody> createRequest(
            BlocklistAddRequestBody requestBody) throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                requestBody,
                new EmptyMessageParameters(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
