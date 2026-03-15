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

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

/**
 * Interface for managing cluster-level node quarantine (also referred to as the management
 * blocklist). This provides a mechanism for cluster administrators to manually quarantine
 * TaskManagers that are experiencing issues (e.g., hardware degradation, network instability, or
 * high error rates) so that they stop receiving new slot allocations.
 *
 * <p>Quarantined TaskManagers will not be assigned new slots, but tasks already running on them
 * will continue to execute normally. This allows operators to gracefully drain problematic
 * TaskManagers without causing immediate job failures.
 *
 * <p>This is independent of Flink's internal batch execution blocklist and speculative execution
 * mechanisms, which are driven automatically by the framework. The management node quarantine is
 * intended for manual operational control.
 *
 * <p>Each quarantined node has an associated expiration timestamp ({@code endTimestamp}). A node
 * becomes available for new allocations only <b>after</b> it has been removed from the quarantine
 * list — either explicitly via {@link #removeQuarantinedNode(String)} or by the periodic cleanup
 * invoked through {@link #removeExpiredNodes()}.
 */
public interface ManagementNodeQuarantineHandler {

    /**
     * Add a TaskManager to the management node quarantine.
     *
     * @param nodeId the ID of the TaskManager to quarantine
     * @param reason the reason for quarantining the TaskManager
     * @param duration the duration for which the TaskManager should be quarantined
     */
    void addQuarantinedNode(String nodeId, String reason, Duration duration);

    /**
     * Remove a TaskManager from the management node quarantine, making it immediately available for
     * new slot allocations.
     *
     * @param nodeId the ID of the TaskManager to remove from quarantine
     * @return true if the TaskManager was removed, false if it wasn't in the quarantine list
     */
    boolean removeQuarantinedNode(String nodeId);

    /**
     * Get all currently quarantined TaskManagers in the management node quarantine.
     *
     * @return a set of all quarantined nodes
     */
    Set<BlockedNode> getAllQuarantinedNodes();

    /**
     * Check if a specific TaskManager is quarantined in the management node quarantine.
     *
     * @param nodeId the ID of the TaskManager to check
     * @return true if the TaskManager is quarantined, false otherwise
     */
    boolean isNodeQuarantined(String nodeId);

    /**
     * Remove all expired nodes from the management node quarantine and make them available for new
     * slot allocations again. A node is considered expired when the current time has passed its
     * {@code endTimestamp}. This method is called periodically by the cleanup scheduler.
     *
     * <p>Note: a node does <b>not</b> become available simply by reaching its expiration time; it
     * must be explicitly removed from the quarantine list by this method (or by {@link
     * #removeQuarantinedNode(String)}).
     *
     * @return the collection of node IDs that were removed due to expiration
     */
    Collection<String> removeExpiredNodes();

    /** Factory interface for creating ManagementNodeQuarantineHandler instances. */
    interface Factory {
        ManagementNodeQuarantineHandler create();
    }
}
