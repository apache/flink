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

/** Default implementation of {@link ManagementNodeQuarantineHandler}. */
public class DefaultManagementNodeQuarantineHandler
        implements ManagementNodeQuarantineHandler, AutoCloseable {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultManagementNodeQuarantineHandler.class);

    private final Map<String, BlockedNode> quarantinedNodes = new HashMap<>();
    private final ScheduledExecutor scheduledExecutor;
    private final Duration cleanupInterval;
    private final ScheduledFuture<?> cleanupTask;

    public DefaultManagementNodeQuarantineHandler(
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
                "Management node quarantine handler initialized with cleanup interval: {}",
                cleanupInterval);
    }

    @Override
    public synchronized void addQuarantinedNode(String nodeId, String reason, Duration duration) {
        checkNotNull(nodeId, "nodeId");
        checkNotNull(reason, "reason");
        checkNotNull(duration, "duration");

        Instant endTime = Instant.now().plus(duration);
        BlockedNode quarantinedNode = new BlockedNode(nodeId, reason, endTime.toEpochMilli());

        BlockedNode existing = quarantinedNodes.put(nodeId, quarantinedNode);
        if (existing != null) {
            LOG.info(
                    "Updated quarantined node: {} (was: {}, now: {})",
                    nodeId,
                    existing,
                    quarantinedNode);
        } else {
            LOG.info("Added quarantined node: {}", quarantinedNode);
        }
    }

    @Override
    public synchronized boolean removeQuarantinedNode(String nodeId) {
        checkNotNull(nodeId, "nodeId");

        BlockedNode removed = quarantinedNodes.remove(nodeId);
        if (removed != null) {
            LOG.info("Removed quarantined node: {}", removed);
            return true;
        } else {
            LOG.debug("Attempted to remove non-existent quarantined node: {}", nodeId);
            return false;
        }
    }

    @Override
    public synchronized Set<BlockedNode> getAllQuarantinedNodes() {
        return new HashSet<>(quarantinedNodes.values());
    }

    @Override
    public synchronized boolean isNodeQuarantined(String nodeId) {
        checkNotNull(nodeId, "nodeId");

        BlockedNode quarantinedNode = quarantinedNodes.get(nodeId);
        if (quarantinedNode == null) {
            return false;
        }

        // Check if the node has expired
        if (quarantinedNode.getEndTimestamp() < System.currentTimeMillis()) {
            quarantinedNodes.remove(nodeId);
            LOG.debug("Removed expired quarantined node: {}", quarantinedNode);
            return false;
        }

        return true;
    }

    @Override
    public synchronized Collection<String> removeExpiredNodes() {
        if (quarantinedNodes.isEmpty()) {
            return Collections.emptyList();
        }

        long now = System.currentTimeMillis();
        Set<String> expiredNodes = new HashSet<>();

        quarantinedNodes
                .entrySet()
                .removeIf(
                        entry -> {
                            BlockedNode node = entry.getValue();
                            if (node.getEndTimestamp() < now) {
                                expiredNodes.add(entry.getKey());
                                LOG.debug("Removed expired quarantined node: {}", node);
                                return true;
                            }
                            return false;
                        });

        if (!expiredNodes.isEmpty()) {
            LOG.info("Removed {} expired quarantined nodes: {}", expiredNodes.size(), expiredNodes);
        }

        return expiredNodes;
    }

    @Override
    public void close() {
        if (cleanupTask != null) {
            cleanupTask.cancel(false);
        }
        LOG.info("Management node quarantine handler closed");
    }

    /** Factory for creating {@link DefaultManagementNodeQuarantineHandler} instances. */
    public static class Factory implements ManagementNodeQuarantineHandler.Factory {

        private final ScheduledExecutor scheduledExecutor;
        private final Duration cleanupInterval;

        public Factory(ScheduledExecutor scheduledExecutor, Duration cleanupInterval) {
            this.scheduledExecutor = checkNotNull(scheduledExecutor);
            this.cleanupInterval = checkNotNull(cleanupInterval);
        }

        @Override
        public ManagementNodeQuarantineHandler create() {
            return new DefaultManagementNodeQuarantineHandler(scheduledExecutor, cleanupInterval);
        }
    }
}
