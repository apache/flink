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
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Simple test for {@link DefaultManagementNodeQuarantineHandler}. */
class SimpleManagementNodeQuarantineTest {

    @Test
    void testBasicFunctionality() {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        ScheduledExecutor executor = new ScheduledExecutorServiceAdapter(executorService);

        try (DefaultManagementNodeQuarantineHandler handler =
                new DefaultManagementNodeQuarantineHandler(executor, Duration.ofSeconds(1))) {

            assertThat(handler.getAllQuarantinedNodes()).isEmpty();

            // Add a quarantined node
            handler.addQuarantinedNode("node1", "Test reason", Duration.ofMinutes(10));

            Set<BlockedNode> quarantinedNodes = handler.getAllQuarantinedNodes();
            assertThat(quarantinedNodes).hasSize(1);

            BlockedNode quarantinedNode = quarantinedNodes.iterator().next();
            assertThat(quarantinedNode.getNodeId()).isEqualTo("node1");
            assertThat(quarantinedNode.getCause()).isEqualTo("Test reason");

            // Check if node is quarantined
            assertThat(handler.isNodeQuarantined("node1")).isTrue();
            assertThat(handler.isNodeQuarantined("node2")).isFalse();

            // Remove the quarantined node
            boolean removed = handler.removeQuarantinedNode("node1");
            assertThat(removed).isTrue();
            assertThat(handler.isNodeQuarantined("node1")).isFalse();
            assertThat(handler.getAllQuarantinedNodes()).isEmpty();

            // Try to remove non-existent node
            boolean removedAgain = handler.removeQuarantinedNode("node1");
            assertThat(removedAgain).isFalse();

        } finally {
            executorService.shutdown();
        }
    }
}
