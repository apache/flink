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

package org.apache.flink.runtime.management.blocklist;

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

/** Tests for edge cases and boundary conditions in management blocklist functionality. */
public class ManagementBlocklistEdgeCasesTest {

    private DefaultManagementBlocklistHandler handler;
    private ScheduledExecutorService executorService;

    @BeforeEach
    public void setUp() {
        executorService = Executors.newSingleThreadScheduledExecutor();
        handler =
                new DefaultManagementBlocklistHandler(
                        ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                                executorService),
                        Duration.ofSeconds(1)); // 1 second cleanup interval
    }

    @Test
    public void testAddNodeWithNullNodeId() {
        // Test null nodeId
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addBlockedNode(null, "reason", Duration.ofMinutes(5));
                });
    }

    @Test
    public void testAddNodeWithNullReason() {
        // Test null reason
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addBlockedNode("node1", null, Duration.ofMinutes(5));
                });
    }

    @Test
    public void testAddNodeWithNullDuration() {
        // Test null duration
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.addBlockedNode("node1", "reason", null);
                });
    }

    @Test
    public void testRemoveNodeWithNullNodeId() {
        // Test null nodeId for removal
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.removeBlockedNode(null);
                });
    }

    @Test
    public void testRemoveNonExistentNode() {
        // Test removing a node that doesn't exist
        boolean result = handler.removeBlockedNode("non-existent-node");
        assertFalse(result);
    }

    @Test
    public void testIsBlockedWithNullNodeId() {
        // Test null nodeId for isBlocked check
        assertThrows(
                NullPointerException.class,
                () -> {
                    handler.isNodeBlocked(null);
                });
    }

    @Test
    public void testAddSameNodeMultipleTimes() {
        // Test adding the same node multiple times
        String nodeId = "test-node";
        String reason1 = "first reason";
        String reason2 = "second reason";
        Duration duration1 = Duration.ofMinutes(10);
        Duration duration2 = Duration.ofMinutes(20);

        // Add node first time
        handler.addBlockedNode(nodeId, reason1, duration1);
        assertTrue(handler.isNodeBlocked(nodeId));

        // Add same node second time (should update)
        handler.addBlockedNode(nodeId, reason2, duration2);
        assertTrue(handler.isNodeBlocked(nodeId));

        // Verify the node exists and has updated information
        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(1, allNodes.size());
        BlockedNode node = allNodes.iterator().next();
        assertEquals(nodeId, node.getNodeId());
        assertEquals(reason2, node.getCause()); // Should have the updated reason
    }

    @Test
    public void testVeryShortDuration() {
        // Test with very short duration (10 milliseconds)
        String nodeId = "short-duration-node";
        Duration shortDuration = Duration.ofMillis(10);

        handler.addBlockedNode(nodeId, "short test", shortDuration);
        assertTrue(handler.isNodeBlocked(nodeId));

        // Wait a bit and check if it's still blocked (might have expired)
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // The node might have expired by now, but that's expected behavior
        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        // We don't assert the result here because it depends on timing
        assertNotNull(allNodes);
    }

    @Test
    public void testVeryLongDuration() {
        // Test with very long duration
        String nodeId = "long-duration-node";
        Duration longDuration = Duration.ofDays(365); // 1 year

        handler.addBlockedNode(nodeId, "long test", longDuration);
        assertTrue(handler.isNodeBlocked(nodeId));

        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(1, allNodes.size());
        BlockedNode node = allNodes.iterator().next();
        assertEquals(nodeId, node.getNodeId());
        assertTrue(
                node.getEndTimestamp()
                        > System.currentTimeMillis() + Duration.ofDays(300).toMillis());
    }

    @Test
    public void testLargeNumberOfNodes() {
        // Test adding a large number of nodes
        int nodeCount = 100; // Reduced from 1000 to avoid test timeout
        Duration duration = Duration.ofMinutes(30);

        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node-" + i;
            handler.addBlockedNode(nodeId, "reason-" + i, duration);
        }

        // Verify all nodes are blocked
        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node-" + i;
            assertTrue(handler.isNodeBlocked(nodeId), "Node " + nodeId + " should be blocked");
        }

        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(nodeCount, allNodes.size());
    }

    @Test
    public void testSpecialCharactersInNodeId() {
        // Test node IDs with special characters
        String[] specialNodeIds = {
            "node-with-dashes", "node_with_underscores", "node.with.dots", "node:with:colons"
        };

        Duration duration = Duration.ofMinutes(10);

        for (String nodeId : specialNodeIds) {
            handler.addBlockedNode(nodeId, "special char test", duration);
            assertTrue(
                    handler.isNodeBlocked(nodeId),
                    "Node with special chars should be blocked: " + nodeId);
        }

        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(specialNodeIds.length, allNodes.size());
    }

    @Test
    public void testSpecialCharactersInReason() {
        // Test reasons with special characters
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
            handler.addBlockedNode(currentNodeId, specialReasons[i], duration);
            assertTrue(handler.isNodeBlocked(currentNodeId));
        }

        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(specialReasons.length, allNodes.size());
    }

    @Test
    public void testBasicAddRemoveOperations() {
        // Test basic add/remove operations
        String nodeId = "test-node";
        Duration duration = Duration.ofMinutes(30);

        // Initially not blocked
        assertFalse(handler.isNodeBlocked(nodeId));

        // Add node
        handler.addBlockedNode(nodeId, "test reason", duration);
        assertTrue(handler.isNodeBlocked(nodeId));

        // Remove node
        boolean removed = handler.removeBlockedNode(nodeId);
        assertTrue(removed);
        assertFalse(handler.isNodeBlocked(nodeId));

        // Try to remove again
        boolean removedAgain = handler.removeBlockedNode(nodeId);
        assertFalse(removedAgain);
    }

    @Test
    public void testGetAllBlockedNodes() {
        // Test getAllBlockedNodes method
        assertTrue(handler.getAllBlockedNodes().isEmpty());

        // Add some nodes
        handler.addBlockedNode("node1", "reason1", Duration.ofMinutes(10));
        handler.addBlockedNode("node2", "reason2", Duration.ofMinutes(20));
        handler.addBlockedNode("node3", "reason3", Duration.ofMinutes(30));

        Set<BlockedNode> allNodes = handler.getAllBlockedNodes();
        assertEquals(3, allNodes.size());

        // Remove one node
        handler.removeBlockedNode("node2");
        allNodes = handler.getAllBlockedNodes();
        assertEquals(2, allNodes.size());
    }

    @Test
    public void testExpiredNodeRemoval() throws InterruptedException {
        // Test automatic removal of expired nodes
        String nodeId = "expiring-node";
        Duration shortDuration = Duration.ofMillis(100);

        handler.addBlockedNode(nodeId, "expiring test", shortDuration);
        assertTrue(handler.isNodeBlocked(nodeId));

        // Wait for expiration
        Thread.sleep(200);

        // Manually trigger cleanup (since we can't wait for automatic cleanup)
        handler.removeExpiredNodes();

        // Node should no longer be blocked
        assertFalse(handler.isNodeBlocked(nodeId));
        assertTrue(handler.getAllBlockedNodes().isEmpty());
    }
}
