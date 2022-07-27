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

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link BlocklistHandler}. */
public class DefaultBlocklistHandler implements BlocklistHandler, AutoCloseable {

    private final Logger log;

    private final Function<ResourceID, String> taskManagerNodeIdRetriever;

    private final BlocklistTracker blocklistTracker;

    private final BlocklistContext blocklistContext;

    private final Set<BlocklistListener> blocklistListeners = new HashSet<>();

    private final Duration timeoutCheckInterval;

    private volatile ScheduledFuture<?> timeoutCheckFuture;

    private final ComponentMainThreadExecutor mainThreadExecutor;

    DefaultBlocklistHandler(
            BlocklistTracker blocklistTracker,
            BlocklistContext blocklistContext,
            Function<ResourceID, String> taskManagerNodeIdRetriever,
            Duration timeoutCheckInterval,
            ComponentMainThreadExecutor mainThreadExecutor,
            Logger log) {
        this.blocklistTracker = checkNotNull(blocklistTracker);
        this.blocklistContext = checkNotNull(blocklistContext);
        this.taskManagerNodeIdRetriever = checkNotNull(taskManagerNodeIdRetriever);
        this.timeoutCheckInterval = checkNotNull(timeoutCheckInterval);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
        this.log = checkNotNull(log);

        scheduleTimeoutCheck();
    }

    private void scheduleTimeoutCheck() {
        this.timeoutCheckFuture =
                mainThreadExecutor.schedule(
                        () -> {
                            removeTimeoutNodes();
                            scheduleTimeoutCheck();
                        },
                        timeoutCheckInterval.toMillis(),
                        TimeUnit.MILLISECONDS);
    }

    private void removeTimeoutNodes() {
        assertRunningInMainThread();
        Collection<BlockedNode> removedNodes =
                blocklistTracker.removeTimeoutNodes(System.currentTimeMillis());
        if (!removedNodes.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Remove {} timeout blocked nodes, details {}. "
                                + "Total {} blocked nodes currently, details: {}.",
                        removedNodes.size(),
                        removedNodes,
                        blocklistTracker.getAllBlockedNodes().size(),
                        blocklistTracker.getAllBlockedNodes());
            } else {
                log.info(
                        "Remove {} timeout blocked nodes. Total {} blocked nodes currently.",
                        removedNodes.size(),
                        blocklistTracker.getAllBlockedNodes().size());
            }
            blocklistContext.unblockResources(removedNodes);
        }
    }

    private void assertRunningInMainThread() {
        mainThreadExecutor.assertRunningInMainThread();
    }

    @Override
    public void addNewBlockedNodes(Collection<BlockedNode> newNodes) {
        assertRunningInMainThread();

        if (newNodes.isEmpty()) {
            return;
        }

        BlockedNodeAdditionResult result = blocklistTracker.addNewBlockedNodes(newNodes);
        Collection<BlockedNode> newlyAddedNodes = result.getNewlyAddedNodes();
        Collection<BlockedNode> allNodes =
                Stream.concat(newlyAddedNodes.stream(), result.getMergedNodes().stream())
                        .collect(Collectors.toList());

        if (!newlyAddedNodes.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Newly added {} blocked nodes, details: {}."
                                + " Total {} blocked nodes currently, details: {}.",
                        newlyAddedNodes.size(),
                        newlyAddedNodes,
                        blocklistTracker.getAllBlockedNodes().size(),
                        blocklistTracker.getAllBlockedNodes());
            } else {
                log.info(
                        "Newly added {} blocked nodes. Total {} blocked nodes currently.",
                        newlyAddedNodes.size(),
                        blocklistTracker.getAllBlockedNodes().size());
            }

            blocklistListeners.forEach(listener -> listener.notifyNewBlockedNodes(allNodes));
            blocklistContext.blockResources(newlyAddedNodes);
        } else if (!allNodes.isEmpty()) {
            blocklistListeners.forEach(listener -> listener.notifyNewBlockedNodes(allNodes));
        }
    }

    @Override
    public boolean isBlockedTaskManager(ResourceID taskManagerId) {
        assertRunningInMainThread();
        String nodeId = checkNotNull(taskManagerNodeIdRetriever.apply(taskManagerId));
        return blocklistTracker.isBlockedNode(nodeId);
    }

    @Override
    public Set<String> getAllBlockedNodeIds() {
        assertRunningInMainThread();

        return blocklistTracker.getAllBlockedNodeIds();
    }

    @Override
    public void registerBlocklistListener(BlocklistListener blocklistListener) {
        assertRunningInMainThread();

        checkNotNull(blocklistListener);
        if (!blocklistListeners.contains(blocklistListener)) {
            blocklistListeners.add(blocklistListener);
            Collection<BlockedNode> allBlockedNodes = blocklistTracker.getAllBlockedNodes();
            if (!allBlockedNodes.isEmpty()) {
                blocklistListener.notifyNewBlockedNodes(allBlockedNodes);
            }
        }
    }

    @Override
    public void deregisterBlocklistListener(BlocklistListener blocklistListener) {
        assertRunningInMainThread();

        checkNotNull(blocklistListener);
        blocklistListeners.remove(blocklistListener);
    }

    @Override
    public void close() throws Exception {
        if (timeoutCheckFuture != null) {
            timeoutCheckFuture.cancel(false);
        }
    }

    /** The factory to instantiate {@link DefaultBlocklistHandler}. */
    public static class Factory implements BlocklistHandler.Factory {

        private final Duration timeoutCheckInterval;

        public Factory(Duration timeoutCheckInterval) {
            this.timeoutCheckInterval = checkNotNull(timeoutCheckInterval);
        }

        @Override
        public BlocklistHandler create(
                BlocklistContext blocklistContext,
                Function<ResourceID, String> taskManagerNodeIdRetriever,
                ComponentMainThreadExecutor mainThreadExecutor,
                Logger log) {
            return new DefaultBlocklistHandler(
                    new DefaultBlocklistTracker(),
                    blocklistContext,
                    taskManagerNodeIdRetriever,
                    timeoutCheckInterval,
                    mainThreadExecutor,
                    log);
        }
    }
}
