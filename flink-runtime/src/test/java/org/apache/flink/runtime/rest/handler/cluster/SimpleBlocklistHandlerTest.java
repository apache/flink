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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.messages.cluster.BlocklistAddRequestBody;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Simple tests for blocklist REST message classes and gateway methods. */
public class SimpleBlocklistHandlerTest {

    private TestingResourceManagerGateway resourceManagerGateway;

    @BeforeEach
    public void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
    }

    @Test
    public void testBlocklistAddRequestBody() throws Exception {
        // Test data
        final String nodeId = "test-node-1";
        final String reason = "test reason";
        final Duration duration = Duration.ofMinutes(30);

        // Create request body with duration
        BlocklistAddRequestBody requestBodyWithDuration =
                new BlocklistAddRequestBody(nodeId, reason, duration);

        // Verify request body
        assertNotNull(requestBodyWithDuration);
        assertEquals(nodeId, requestBodyWithDuration.getNodeId());
        assertEquals(reason, requestBodyWithDuration.getReason());
        assertEquals(duration, requestBodyWithDuration.getDuration());

        // Create request body without duration (null)
        BlocklistAddRequestBody requestBodyWithoutDuration =
                new BlocklistAddRequestBody(nodeId, reason, null);

        // Verify request body
        assertNotNull(requestBodyWithoutDuration);
        assertEquals(nodeId, requestBodyWithoutDuration.getNodeId());
        assertEquals(reason, requestBodyWithoutDuration.getReason());
        assertNull(requestBodyWithoutDuration.getDuration());
    }

    @Test
    public void testResourceManagerGatewayAddMethod() throws Exception {
        // Test data
        final String nodeId = "test-node";
        final String reason = "test reason";
        final Duration duration = Duration.ofMinutes(15);
        final Duration timeout = Duration.ofSeconds(10);

        // Set up the resource manager gateway to capture the call
        final CompletableFuture<Tuple3<String, String, Duration>> capturedCall =
                new CompletableFuture<>();

        resourceManagerGateway.setAddManagementBlockedNodeFunction(
                (tuple) -> {
                    capturedCall.complete(tuple);
                    return null; // Return null for Void
                });

        // Test addManagementBlockedNode
        CompletableFuture<Void> addFuture =
                resourceManagerGateway.addManagementBlockedNode(nodeId, reason, duration, timeout);
        assertNotNull(addFuture);
        assertNull(addFuture.get());

        // Verify the captured call
        Tuple3<String, String, Duration> captured = capturedCall.get();
        assertEquals(nodeId, captured.f0);
        assertEquals(reason, captured.f1);
        assertEquals(duration, captured.f2);
    }

    @Test
    public void testResourceManagerGatewayRemoveMethod() throws Exception {
        final String nodeId = "test-node";
        final Duration timeout = Duration.ofSeconds(10);

        // Set up the resource manager gateway to capture the call
        final CompletableFuture<String> capturedNodeId = new CompletableFuture<>();

        resourceManagerGateway.setRemoveManagementBlockedNodeFunction(
                (removedNodeId) -> {
                    capturedNodeId.complete(removedNodeId);
                    return null; // Return null for Void
                });

        // Test removeManagementBlockedNode
        CompletableFuture<Void> removeFuture =
                resourceManagerGateway.removeManagementBlockedNode(nodeId, timeout);
        assertNotNull(removeFuture);
        assertNull(removeFuture.get());

        // Verify the captured call
        assertEquals(nodeId, capturedNodeId.get());
    }

    @Test
    public void testResourceManagerGatewayGetAllMethod() throws Exception {
        // Test data - create some BlockedNode objects
        final BlockedNode node1 =
                new BlockedNode("test-node-1", "reason1", System.currentTimeMillis() + 60000);
        final BlockedNode node2 =
                new BlockedNode("test-node-2", "reason2", System.currentTimeMillis() + 120000);
        final Duration timeout = Duration.ofSeconds(10);

        // Set up the resource manager gateway to return test data
        resourceManagerGateway.setGetAllManagementBlockedNodesSupplier(
                () -> CompletableFuture.completedFuture(Arrays.asList(node1, node2)));

        // Test getAllManagementBlockedNodes
        CompletableFuture<Collection<BlockedNode>> getAllFuture =
                resourceManagerGateway.getAllManagementBlockedNodes(timeout);
        assertNotNull(getAllFuture);

        Collection<BlockedNode> result = getAllFuture.get();
        assertEquals(2, result.size());
        assertTrue(result.contains(node1));
        assertTrue(result.contains(node2));
    }

    @Test
    public void testResourceManagerGatewayDefaultBehavior() throws Exception {
        // Test the default behavior without setting custom functions
        final Duration timeout = Duration.ofSeconds(10);

        // Test getAllManagementBlockedNodes default
        CompletableFuture<Collection<BlockedNode>> getAllFuture =
                resourceManagerGateway.getAllManagementBlockedNodes(timeout);
        assertNotNull(getAllFuture);
        assertTrue(getAllFuture.get().isEmpty());

        // Test addManagementBlockedNode default
        CompletableFuture<Void> addFuture =
                resourceManagerGateway.addManagementBlockedNode(
                        "node", "reason", Duration.ofMinutes(5), timeout);
        assertNotNull(addFuture);
        assertNull(addFuture.get());

        // Test removeManagementBlockedNode default
        CompletableFuture<Void> removeFuture =
                resourceManagerGateway.removeManagementBlockedNode("node", timeout);
        assertNotNull(removeFuture);
        assertNull(removeFuture.get());
    }
}
