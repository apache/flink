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

package org.apache.flink.runtime.management.nodequarantine;

import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for edge cases and boundary conditions in management node quarantine functionality. */
public class ManagementNodeQuarantineEdgeCasesTest {

    private DefaultManagementNodeQuarantineHandler handler;
    private ScheduledExecutorService executorService;

    @BeforeEach
    public void setUp() {
        executorService = Executors.newSingleThreadScheduledExecutor();
        handler =
                new DefaultManagementNodeQuarantineHandler(
                        ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                                executorService),
                        Duration.ofSeconds(1)); // 1 second cleanup interval
    }

    @Test
    public void testAddNodeWithNullNodeId() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addQuarantinedNode(null, "reason", Duration.ofMinutes(5));
                });
    }

    @Test
    public void testAddNodeWithNullReason() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addQuarantinedNode("node1", null, Duration.ofMinutes(5));
                });
    }

    @Test
    public void testAddNodeWithNullDuration() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addQuarantinedNode("node1", "reason", null);
                });
    }

    @Test
    public void testRemoveNodeWithNullNodeId() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.removeQuarantinedNode(null);
                });
    }

    @Test
    public void testRemoveNonExistentNode() {
        boolean result = handler.removeQuarantinedNode("non-existent-node");
        assertFalse(result);
    }

    @Test
    public void testIsQuarantinedWithNullNodeId() {
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.isNodeQuarantined(null);
                });
    }

    @Test
    public void testAddSameNodeMultipleTimes() {
        String nodeId = "test-node";
        String reason1 = "first reason";
        String reason2 = "second reason";
        Duration duration1 = Duration.ofMinutes(10);
        Duration duration2 = Duration.ofMinutes(20);

        // Add node first time
        handler.addQuarantinedNode(nodeId, reason1, duration1);
        assertTrue(handler.isNodeQuarantined(nodeId));

        // Add same node second time (should update)
        handler.addQuarantinedNode(nodeId, reason2, duration2);
        assertTrue(handler.isNodeQuarantined(nodeId));

        // Verify the node exists and has updated information
        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(1, allNodes.size());
        BlockedNode node = allNodes.iterator().next();
        assertEquals(nodeId, node.getNodeId());
        assertEquals(reason2, node.getCause()); // Should have the updated reason
    }

    @Test
    public void testVeryShortDuration() {
        String nodeId = "short-duration-node";
        Duration shortDuration = Duration.ofMillis(10);

        handler.addQuarantinedNode(nodeId, "short test", shortDuration);
        assertTrue(handler.isNodeQuarantined(nodeId));

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertNotNull(allNodes);
    }

    @Test
    public void testVeryLongDuration() {
        String nodeId = "long-duration-node";
        Duration longDuration = Duration.ofDays(365); // 1 year

        handler.addQuarantinedNode(nodeId, "long test", longDuration);
        assertTrue(handler.isNodeQuarantined(nodeId));

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(1, allNodes.size());
        BlockedNode node = allNodes.iterator().next();
        assertEquals(nodeId, node.getNodeId());
        assertTrue(
                node.getEndTimestamp()
                        > System.currentTimeMillis() + Duration.ofDays(300).toMillis());
    }

    @Test
    public void testLargeNumberOfNodes() {
        int nodeCount = 100;
        Duration duration = Duration.ofMinutes(30);

        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node-" + i;
            handler.addQuarantinedNode(nodeId, "reason-" + i, duration);
        }

        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node-" + i;
            assertTrue(
                    handler.isNodeQuarantined(nodeId), "Node " + nodeId + " should be quarantined");
        }

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(nodeCount, allNodes.size());
    }

    @Test
    public void testSpecialCharactersInNodeId() {
        String[] specialNodeIds = {
            "node-with-dashes", "node_with_underscores", "node.with.dots", "node:with:colons"
        };

        Duration duration = Duration.ofMinutes(10);

        for (String nodeId : specialNodeIds) {
            handler.addQuarantinedNode(nodeId, "special char test", duration);
            assertTrue(
                    handler.isNodeQuarantined(nodeId),
                    "Node with special chars should be quarantined: " + nodeId);
        }

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(specialNodeIds.length, allNodes.size());
    }

    @Test
    public void testSpecialCharactersInReason() {
        String nodeId = "test-node";
        String[] specialReasons = {
            "Reason with spaces and punctuation!",
            "Reason with unicode: 测试原因",
            "Reason with newlines:\nSecond line",
            "Reason with tabs:\tTabbed content",
            "Reason with quotes: \"quoted\" and 'single quoted'"
        };

        Duration duration = Duration.ofMinutes(10);

        for (int i = 0; i < specialReasons.length; i++) {
            String currentNodeId = nodeId + "-" + i;
            handler.addQuarantinedNode(currentNodeId, specialReasons[i], duration);
            assertTrue(handler.isNodeQuarantined(currentNodeId));
        }

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(specialReasons.length, allNodes.size());
    }

    @Test
    public void testBasicAddRemoveOperations() {
        String nodeId = "test-node";
        Duration duration = Duration.ofMinutes(30);

        // Initially not quarantined
        assertFalse(handler.isNodeQuarantined(nodeId));

        // Add node
        handler.addQuarantinedNode(nodeId, "test reason", duration);
        assertTrue(handler.isNodeQuarantined(nodeId));

        // Remove node
        boolean removed = handler.removeQuarantinedNode(nodeId);
        assertTrue(removed);
        assertFalse(handler.isNodeQuarantined(nodeId));

        // Try to remove again
        boolean removedAgain = handler.removeQuarantinedNode(nodeId);
        assertFalse(removedAgain);
    }

    @Test
    public void testGetAllQuarantinedNodes() {
        assertTrue(handler.getAllQuarantinedNodes().isEmpty());

        handler.addQuarantinedNode("node1", "reason1", Duration.ofMinutes(10));
        handler.addQuarantinedNode("node2", "reason2", Duration.ofMinutes(20));
        handler.addQuarantinedNode("node3", "reason3", Duration.ofMinutes(30));

        Set<BlockedNode> allNodes = handler.getAllQuarantinedNodes();
        assertEquals(3, allNodes.size());

        handler.removeQuarantinedNode("node2");
        allNodes = handler.getAllQuarantinedNodes();
        assertEquals(2, allNodes.size());
    }

    @Test
    public void testExpiredNodeRemoval() throws InterruptedException {
        String nodeId = "expiring-node";
        Duration shortDuration = Duration.ofMillis(100);

        handler.addQuarantinedNode(nodeId, "expiring test", shortDuration);
        assertTrue(handler.isNodeQuarantined(nodeId));

        // Wait for expiration
        Thread.sleep(200);

        // Manually trigger cleanup
        handler.removeExpiredNodes();

        // Node should no longer be quarantined
        assertFalse(handler.isNodeQuarantined(nodeId));
        assertTrue(handler.getAllQuarantinedNodes().isEmpty());
    }
}
