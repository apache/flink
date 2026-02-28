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
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple test for {@link DefaultManagementBlocklistHandler}. */
class SimpleManagementBlocklistTest {

    @Test
    void testBasicFunctionality() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutor executor = new ScheduledExecutorServiceAdapter(executorService);

        try (DefaultManagementBlocklistHandler handler =
                new DefaultManagementBlocklistHandler(executor, Duration.ofSeconds(1))) {

            assertThat(handler.getAllBlockedNodes()).isEmpty();

            // Add a blocked node
            handler.addBlockedNode("node1", "Test reason", Duration.ofMinutes(10));

            Set<BlockedNode> blockedNodes = handler.getAllBlockedNodes();
            assertThat(blockedNodes).hasSize(1);

            BlockedNode blockedNode = blockedNodes.iterator().next();
            assertThat(blockedNode.getNodeId()).isEqualTo("node1");
            assertThat(blockedNode.getCause()).isEqualTo("Test reason");

            // Check if node is blocked
            assertThat(handler.isNodeBlocked("node1")).isTrue();
            assertThat(handler.isNodeBlocked("node2")).isFalse();

            // Remove the blocked node
            boolean removed = handler.removeBlockedNode("node1");
            assertThat(removed).isTrue();
            assertThat(handler.isNodeBlocked("node1")).isFalse();
            assertThat(handler.getAllBlockedNodes()).isEmpty();

            // Try to remove non-existent node
            boolean removedAgain = handler.removeBlockedNode("node1");
            assertThat(removedAgain).isFalse();

        } finally {
            executorService.shutdown();
        }
    }
}
