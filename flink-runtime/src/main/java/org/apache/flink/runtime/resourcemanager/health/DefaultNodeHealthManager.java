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

package org.apache.flink.runtime.resourcemanager.health;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Default implementation of {@link NodeHealthManager}.
 *
 * <p>This implementation maintains a thread-safe map of quarantined nodes and provides
 * functionality to check node health, mark nodes as quarantined, remove quarantine, list all
 * statuses, and clean up expired entries.
 */
public class DefaultNodeHealthManager implements NodeHealthManager {

    /** Map storing node health statuses keyed by resource ID. */
    private final ConcurrentMap<ResourceID, NodeHealthStatus> quarantinedNodes;

    /** Creates a new DefaultNodeHealthManager. */
    public DefaultNodeHealthManager() {
        this.quarantinedNodes = new ConcurrentHashMap<>();
    }

    @Override
    public boolean isHealthy(ResourceID resourceID) {
        Preconditions.checkNotNull(resourceID, "resourceID must not be null");

        NodeHealthStatus status = quarantinedNodes.get(resourceID);
        if (status == null) {
            // Node not in quarantine - healthy
            return true;
        }

        // Check if quarantine has expired
        long now = System.currentTimeMillis();
        if (status.isExpired(now)) {
            // Clean up expired entry and consider healthy
            quarantinedNodes.remove(resourceID, status);
            return true;
        }

        // Node is quarantined and not expired - unhealthy
        return false;
    }

    @Override
    public void markQuarantined(
            ResourceID resourceID, String hostname, String reason, Duration duration) {
        Preconditions.checkNotNull(resourceID, "resourceID must not be null");
        Preconditions.checkNotNull(hostname, "hostname must not be null");
        Preconditions.checkNotNull(reason, "reason must not be null");
        Preconditions.checkNotNull(duration, "duration must not be null");
        Preconditions.checkArgument(!duration.isNegative(), "duration must be non-negative");

        long now = System.currentTimeMillis();
        long expirationTimestamp = now + duration.toMillis();

        NodeHealthStatus newStatus =
                new NodeHealthStatus(resourceID, hostname, reason, now, expirationTimestamp);

        quarantinedNodes.put(resourceID, newStatus);
    }

    @Override
    public void removeQuarantine(ResourceID resourceID) {
        Preconditions.checkNotNull(resourceID, "resourceID must not be null");

        quarantinedNodes.remove(resourceID);
    }

    @Override
    public Collection<NodeHealthStatus> listAll() {
        // Return a snapshot of the current status
        return new ArrayList<>(quarantinedNodes.values());
    }

    @Override
    public void cleanupExpired() {
        long now = System.currentTimeMillis();

        // Remove all expired entries
        quarantinedNodes.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
    }
}
