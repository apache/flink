/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.blocklist;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultBlocklistHandler}. */
class DefaultBlocklistHandlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBlocklistHandlerTest.class);

    @Test
    void testAddNewBlockedNodes() throws Exception {
        BlockedNode node1 = new BlockedNode("node1", "cause", Long.MAX_VALUE);
        BlockedNode node2 = new BlockedNode("node2", "cause", Long.MAX_VALUE);

        TestBlocklistContext context = new TestBlocklistContext();
        TestBlocklistListener listener = new TestBlocklistListener();

        try (DefaultBlocklistHandler handler = createDefaultBlocklistHandler(context)) {
            handler.registerBlocklistListener(listener);
            assertThat(listener.notifiedTimes).isEqualTo(0);
            assertThat(listener.notifiedNodes).isEmpty();
            assertThat(context.allBlockedNodes).isEmpty();

            // add node1, node2
            handler.addNewBlockedNodes(Arrays.asList(node1, node2));
            // check listener and context
            assertThat(listener.notifiedTimes).isEqualTo(1);
            assertThat(listener.notifiedNodes).containsExactlyInAnyOrder(node1, node2);
            assertThat(context.allBlockedNodes).containsExactlyInAnyOrder(node1, node2);

            // add node1, node2 again, should not notify listener
            handler.addNewBlockedNodes(Arrays.asList(node1, node2));
            assertThat(listener.notifiedTimes).isEqualTo(1);

            // register a new listener, will notify all items
            TestBlocklistListener listener2 = new TestBlocklistListener();
            handler.registerBlocklistListener(listener2);
            assertThat(listener2.notifiedTimes).isEqualTo(1);
            assertThat(listener2.notifiedNodes).containsExactlyInAnyOrder(node1, node2);
        }
    }

    @Test
    void testRemoveTimeoutNodes() throws Exception {
        long currentTimestamp = System.currentTimeMillis();
        BlockedNode node1 = new BlockedNode("node1", "cause", currentTimestamp + 1000L);
        BlockedNode node2 = new BlockedNode("node2", "cause", currentTimestamp + 3000L);

        TestBlocklistContext context = new TestBlocklistContext();
        try (DefaultBlocklistHandler handler = createDefaultBlocklistHandler(context)) {

            handler.addNewBlockedNodes(Arrays.asList(node1, node2));
            assertThat(handler.getAllBlockedNodeIds()).hasSize(2);
            assertThat(context.allUnBlockedNodes).hasSize(0);

            // wait node1 timeout
            CommonTestUtils.waitUntilCondition(() -> handler.getAllBlockedNodeIds().size() == 1);
            assertThat(context.allUnBlockedNodes).containsExactly(node1);

            // wait node2 timeout
            CommonTestUtils.waitUntilCondition(() -> handler.getAllBlockedNodeIds().size() == 0);
            assertThat(context.allUnBlockedNodes).containsExactly(node1, node2);
        }
    }

    @Test
    void testIsBlockedTaskManager() throws Exception {
        ResourceID resourceID1 = ResourceID.generate();
        ResourceID resourceID2 = ResourceID.generate();
        ResourceID resourceID3 = ResourceID.generate();

        Map<ResourceID, String> taskManagerToNode = new HashMap<>();
        taskManagerToNode.put(resourceID1, "node1");
        taskManagerToNode.put(resourceID2, "node1");
        taskManagerToNode.put(resourceID3, "node2");

        try (DefaultBlocklistHandler handler = createDefaultBlocklistHandler(taskManagerToNode)) {

            handler.addNewBlockedNodes(
                    Collections.singleton(new BlockedNode("node1", "cause", Long.MAX_VALUE)));

            assertThat(handler.isBlockedTaskManager(resourceID1)).isTrue();
            assertThat(handler.isBlockedTaskManager(resourceID2)).isTrue();
            assertThat(handler.isBlockedTaskManager(resourceID3)).isFalse();
        }
    }

    private DefaultBlocklistHandler createDefaultBlocklistHandler(
            BlocklistContext blocklistContext) {
        return new DefaultBlocklistHandler(
                new DefaultBlocklistTracker(),
                blocklistContext,
                resourceID -> "node",
                Time.milliseconds(100L),
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                LOG);
    }

    private DefaultBlocklistHandler createDefaultBlocklistHandler(
            Map<ResourceID, String> taskManagerToNode) {
        return new DefaultBlocklistHandler(
                new DefaultBlocklistTracker(),
                new TestBlocklistContext(),
                taskManagerToNode::get,
                Time.milliseconds(100L),
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                LOG);
    }

    private static class TestBlocklistListener implements BlocklistListener {

        private int notifiedTimes = 0;

        private final List<BlockedNode> notifiedNodes = new ArrayList<>();

        @Override
        public void notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
            notifiedTimes++;
            notifiedNodes.addAll(newNodes);
        }
    }

    private static class TestBlocklistContext implements BlocklistContext {
        private final List<BlockedNode> allBlockedNodes = new ArrayList<>();

        private final List<BlockedNode> allUnBlockedNodes = new ArrayList<>();

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            allBlockedNodes.addAll(blockedNodes);
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unBlockedNodes) {
            allUnBlockedNodes.addAll(unBlockedNodes);
        }
    }
}
