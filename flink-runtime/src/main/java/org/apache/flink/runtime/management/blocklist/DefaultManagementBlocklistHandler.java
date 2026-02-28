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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link ManagementBlocklistHandler}. */
public class DefaultManagementBlocklistHandler
        implements ManagementBlocklistHandler, AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultManagementBlocklistHandler.class);

    private final Map<String, BlockedNode> blockedNodes = new HashMap<>();
    private final ScheduledExecutor scheduledExecutor;
    private final Duration cleanupInterval;
    private final ScheduledFuture<?> cleanupTask;

    public DefaultManagementBlocklistHandler(
            ScheduledExecutor scheduledExecutor, Duration cleanupInterval) {
        this.scheduledExecutor = checkNotNull(scheduledExecutor);
        this.cleanupInterval = checkNotNull(cleanupInterval);

        // Schedule periodic cleanup of expired nodes
        this.cleanupTask =
                scheduledExecutor.scheduleWithFixedDelay(
                        this::removeExpiredNodes,
                        cleanupInterval.toMillis(),
                        cleanupInterval.toMillis(),
                        TimeUnit.MILLISECONDS);

        LOG.info(
                "Management blocklist handler initialized with cleanup interval: {}",
                cleanupInterval);
    }

    @Override
    public synchronized void addBlockedNode(String nodeId, String reason, Duration duration) {
        checkNotNull(nodeId, "nodeId");
        checkNotNull(reason, "reason");
        checkNotNull(duration, "duration");

        Instant endTime = Instant.now().plus(duration);
        BlockedNode blockedNode = new BlockedNode(nodeId, reason, endTime);

        BlockedNode existing = blockedNodes.put(nodeId, blockedNode);
        if (existing != null) {
            LOG.info("Updated blocked node: {} (was: {}, now: {})", nodeId, existing, blockedNode);
        } else {
            LOG.info("Added blocked node: {}", blockedNode);
        }
    }

    @Override
    public synchronized boolean removeBlockedNode(String nodeId) {
        checkNotNull(nodeId, "nodeId");

        BlockedNode removed = blockedNodes.remove(nodeId);
        if (removed != null) {
            LOG.info("Removed blocked node: {}", removed);
            return true;
        } else {
            LOG.debug("Attempted to remove non-existent blocked node: {}", nodeId);
            return false;
        }
    }

    @Override
    public synchronized Set<BlockedNode> getAllBlockedNodes() {
        return new HashSet<>(blockedNodes.values());
    }

    @Override
    public synchronized boolean isNodeBlocked(String nodeId) {
        checkNotNull(nodeId, "nodeId");

        BlockedNode blockedNode = blockedNodes.get(nodeId);
        if (blockedNode == null) {
            return false;
        }

        // Check if the node has expired
        if (blockedNode.getEndTimestamp().isBefore(Instant.now())) {
            blockedNodes.remove(nodeId);
            LOG.debug("Removed expired blocked node: {}", blockedNode);
            return false;
        }

        return true;
    }

    @Override
    public synchronized Collection<String> removeExpiredNodes() {
        if (blockedNodes.isEmpty()) {
            return Collections.emptyList();
        }

        Instant now = Instant.now();
        Set<String> expiredNodes = new HashSet<>();

        blockedNodes
                .entrySet()
                .removeIf(
                        entry -> {
                            BlockedNode node = entry.getValue();
                            if (node.getEndTimestamp().isBefore(now)) {
                                expiredNodes.add(entry.getKey());
                                LOG.debug("Removed expired blocked node: {}", node);
                                return true;
                            }
                            return false;
                        });

        if (!expiredNodes.isEmpty()) {
            LOG.info("Removed {} expired blocked nodes: {}", expiredNodes.size(), expiredNodes);
        }

        return expiredNodes;
    }

    @Override
    public void close() {
        if (cleanupTask != null) {
            cleanupTask.cancel(false);
        }
        LOG.info("Management blocklist handler closed");
    }

    /** Factory for creating {@link DefaultManagementBlocklistHandler} instances. */
    public static class Factory implements ManagementBlocklistHandler.Factory {

        private final ScheduledExecutor scheduledExecutor;
        private final Duration cleanupInterval;

        public Factory(ScheduledExecutor scheduledExecutor, Duration cleanupInterval) {
            this.scheduledExecutor = checkNotNull(scheduledExecutor);
            this.cleanupInterval = checkNotNull(cleanupInterval);
        }

        @Override
        public ManagementBlocklistHandler create() {
            return new DefaultManagementBlocklistHandler(scheduledExecutor, cleanupInterval);
        }
    }
}
