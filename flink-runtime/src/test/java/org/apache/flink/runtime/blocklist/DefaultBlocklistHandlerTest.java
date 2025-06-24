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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultBlocklistHandler}. */
class DefaultBlocklistHandlerTest {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultBlocklistHandlerTest.class);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testAddNewBlockedNodes() throws Exception {
        BlockedNode node1 = new BlockedNode("node1", "cause", 1L);
        BlockedNode node2 = new BlockedNode("node2", "cause", 1L);
        BlockedNode node2Update = new BlockedNode("node2", "cause", 2L);

        List<List<BlockedNode>> contextReceivedNodes = new ArrayList<>();

        TestBlocklistContext context =
                TestBlocklistContext.newBuilder()
                        .setBlockResourcesConsumer(
                                blockedNodes ->
                                        contextReceivedNodes.add(new ArrayList<>(blockedNodes)))
                        .build();
        TestBlocklistListener listener = new TestBlocklistListener();

        try (DefaultBlocklistHandler handler = createDefaultBlocklistHandler(context)) {
            handler.registerBlocklistListener(listener);
            assertThat(listener.listenerReceivedNodes).isEmpty();
            assertThat(contextReceivedNodes).isEmpty();

            // add node1, node2
            handler.addNewBlockedNodes(Arrays.asList(node1, node2));
            // check listener and context
            assertThat(listener.listenerReceivedNodes).hasSize(1);
            assertThat(listener.listenerReceivedNodes.get(0))
                    .containsExactlyInAnyOrder(node1, node2);
            assertThat(contextReceivedNodes).hasSize(1);
            assertThat(contextReceivedNodes.get(0)).containsExactlyInAnyOrder(node1, node2);

            // add node1, node2 again, should not notify context and listener
            assertThat(contextReceivedNodes).hasSize(1);
            assertThat(listener.listenerReceivedNodes).hasSize(1);

            // update node2, should notify listener, not notify context
            handler.addNewBlockedNodes(Collections.singleton(node2Update));
            assertThat(listener.listenerReceivedNodes).hasSize(2);
            assertThat(listener.listenerReceivedNodes.get(1)).containsExactly(node2Update);
            assertThat(contextReceivedNodes).hasSize(1);

            // register a new listener, will notify all items
            TestBlocklistListener listener2 = new TestBlocklistListener();
            handler.registerBlocklistListener(listener2);
            assertThat(listener2.listenerReceivedNodes).hasSize(1);
            assertThat(listener2.listenerReceivedNodes.get(0))
                    .containsExactlyInAnyOrder(node1, node2Update);
        }
    }

    @Test
    void testRemoveTimeoutNodes() throws Exception {
        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        EXECUTOR_EXTENSION.getExecutor());

        final CompletableFuture<Collection<BlockedNode>> unblockResourcesFuture =
                new CompletableFuture<>();

        final TestBlocklistContext context =
                TestBlocklistContext.newBuilder()
                        .setUnblockResourcesConsumer(unblockResourcesFuture::complete)
                        .build();

        try (DefaultBlocklistHandler handler =
                createDefaultBlocklistHandler(context, mainThreadExecutor)) {
            CompletableFuture.supplyAsync(
                            () -> {
                                BlockedNode blockedNode =
                                        new BlockedNode(
                                                "node",
                                                "cause",
                                                System.currentTimeMillis() + 1000L);
                                handler.addNewBlockedNodes(Collections.singleton(blockedNode));
                                assertThat(handler.getAllBlockedNodeIds()).hasSize(1);
                                return blockedNode;
                            },
                            mainThreadExecutor)
                    // wait for the timeout to occur
                    .thenAcceptBoth(
                            unblockResourcesFuture,
                            (blockedNode, unblockResources) -> {
                                assertThat(handler.getAllBlockedNodeIds()).isEmpty();
                                assertThat(unblockResources).containsExactly(blockedNode);
                            })
                    .get();
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
                Duration.ofMillis(100L),
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                LOG);
    }

    private DefaultBlocklistHandler createDefaultBlocklistHandler(
            Map<ResourceID, String> taskManagerToNode) {
        return new DefaultBlocklistHandler(
                new DefaultBlocklistTracker(),
                TestBlocklistContext.newBuilder().build(),
                taskManagerToNode::get,
                Duration.ofMillis(100L),
                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                LOG);
    }

    private DefaultBlocklistHandler createDefaultBlocklistHandler(
            BlocklistContext blocklistContext, ComponentMainThreadExecutor mainThreadExecutor) {
        return new DefaultBlocklistHandler(
                new DefaultBlocklistTracker(),
                blocklistContext,
                resourceID -> "node",
                Duration.ofMillis(100L),
                mainThreadExecutor,
                LOG);
    }

    private static class TestBlocklistListener implements BlocklistListener {

        private final List<List<BlockedNode>> listenerReceivedNodes = new ArrayList<>();

        @Override
        public CompletableFuture<Acknowledge> notifyNewBlockedNodes(
                Collection<BlockedNode> newNodes) {
            listenerReceivedNodes.add(new ArrayList<>(newNodes));
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    private static class TestBlocklistContext implements BlocklistContext {
        private final Consumer<Collection<BlockedNode>> blockResourcesConsumer;

        private final Consumer<Collection<BlockedNode>> unblockResourcesConsumer;

        private TestBlocklistContext(
                Consumer<Collection<BlockedNode>> blockResourcesConsumer,
                Consumer<Collection<BlockedNode>> unblockResourcesConsumer) {
            this.blockResourcesConsumer = checkNotNull(blockResourcesConsumer);
            this.unblockResourcesConsumer = checkNotNull(unblockResourcesConsumer);
        }

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            blockResourcesConsumer.accept(blockedNodes);
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {
            unblockResourcesConsumer.accept(unblockedNodes);
        }

        private static class Builder {
            private Consumer<Collection<BlockedNode>> blockResourcesConsumer = ignored -> {};

            private Consumer<Collection<BlockedNode>> unblockResourcesConsumer = ignored -> {};

            public Builder setBlockResourcesConsumer(
                    Consumer<Collection<BlockedNode>> blockResourcesConsumer) {
                this.blockResourcesConsumer = blockResourcesConsumer;
                return this;
            }

            public Builder setUnblockResourcesConsumer(
                    Consumer<Collection<BlockedNode>> unblockResourcesConsumer) {
                this.unblockResourcesConsumer = unblockResourcesConsumer;
                return this;
            }

            public TestBlocklistContext build() {
                return new TestBlocklistContext(blockResourcesConsumer, unblockResourcesConsumer);
            }
        }

        static Builder newBuilder() {
            return new Builder();
        }
    }
}
