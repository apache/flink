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
import org.apache.flink.runtime.rest.messages.cluster.BlocklistRemoveRequestBody;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlocklistRemoveHandler}. */
class BlocklistRemoveHandlerTest {

    private TestingResourceManagerGateway resourceManagerGateway;
    private BlocklistRemoveHandler handler;

    @BeforeEach
    void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
        handler =
                new BlocklistRemoveHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        BlocklistRemoveHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway));
    }

    @Test
    void testRemoveBlockedNode() throws Exception {
        final String nodeId = "test-node";

        // Set up the resource manager gateway to capture the call
        final CompletableFuture<String> capturedNodeId = new CompletableFuture<>();

        resourceManagerGateway.setRemoveManagementBlockedNodeFunction(
                (node) -> {
                    capturedNodeId.complete(node);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        // Create request
        BlocklistRemoveRequestBody requestBody = new BlocklistRemoveRequestBody(nodeId);
        HandlerRequest<BlocklistRemoveRequestBody> request = createRequest(requestBody);

        // Handle request
        Acknowledge response = handler.handleRequest(request, resourceManagerGateway).get();

        // Verify response
        assertThat(response).isEqualTo(Acknowledge.get());

        // Verify the resource manager was called with correct parameters
        assertThat(capturedNodeId.get()).isEqualTo(nodeId);
    }

    private HandlerRequest<BlocklistRemoveRequestBody> createRequest(
            BlocklistRemoveRequestBody requestBody) throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                requestBody,
                new EmptyMessageParameters(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
