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

import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.HandlerRequestException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistResponseBody;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BlocklistGetHandler}. */
class BlocklistGetHandlerTest {

    private TestingResourceManagerGateway resourceManagerGateway;
    private BlocklistGetHandler handler;

    @BeforeEach
    void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
        handler =
                new BlocklistGetHandler(
                        () -> CompletableFuture.completedFuture(null),
                        TestingUtils.TIMEOUT,
                        Collections.emptyMap(),
                        BlocklistGetHeaders.getInstance(),
                        () -> CompletableFuture.completedFuture(resourceManagerGateway));
    }

    @Test
    void testGetBlockedNodes() throws Exception {
        final List<String> blockedNodes = Arrays.asList("node1", "node2", "node3");

        // Set up the resource manager gateway to return blocked nodes
        resourceManagerGateway.setGetAllManagementBlockedNodesSupplier(
                () -> CompletableFuture.completedFuture(blockedNodes));

        // Create request
        HandlerRequest<EmptyRequestBody> request = createRequest();

        // Handle request
        BlocklistResponseBody response =
                handler.handleRequest(request, resourceManagerGateway).get();

        // Verify response
        assertThat(response.getBlockedNodes()).containsExactlyInAnyOrderElementsOf(blockedNodes);
    }

    @Test
    void testGetEmptyBlockedNodes() throws Exception {
        // Set up the resource manager gateway to return empty list
        resourceManagerGateway.setGetAllManagementBlockedNodesSupplier(
                () -> CompletableFuture.completedFuture(Collections.emptyList()));

        // Create request
        HandlerRequest<EmptyRequestBody> request = createRequest();

        // Handle request
        BlocklistResponseBody response =
                handler.handleRequest(request, resourceManagerGateway).get();

        // Verify response
        assertThat(response.getBlockedNodes()).isEmpty();
    }

    private HandlerRequest<EmptyRequestBody> createRequest() throws HandlerRequestException {
        return HandlerRequest.resolveParametersAndCreate(
                EmptyRequestBody.getInstance(),
                new EmptyMessageParameters(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyList());
    }
}
