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

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

/**
 * Interface for managing cluster-level node blocklist. This is independent of batch execution
 * blocklist and speculative execution.
 */
public interface ManagementBlocklistHandler {

    /**
     * Add a node to the management blocklist.
     *
     * @param nodeId the ID of the node to block
     * @param reason the reason for blocking the node
     * @param duration the duration for which the node should be blocked
     */
    void addBlockedNode(String nodeId, String reason, Duration duration);

    /**
     * Remove a node from the management blocklist.
     *
     * @param nodeId the ID of the node to unblock
     * @return true if the node was removed, false if it wasn't in the blocklist
     */
    boolean removeBlockedNode(String nodeId);

    /**
     * Get all currently blocked nodes in the management blocklist.
     *
     * @return a set of all blocked nodes
     */
    Set<BlockedNode> getAllBlockedNodes();

    /**
     * Check if a specific node is blocked in the management blocklist.
     *
     * @param nodeId the ID of the node to check
     * @return true if the node is blocked, false otherwise
     */
    boolean isNodeBlocked(String nodeId);

    /**
     * Remove all expired nodes from the management blocklist. This method is called periodically to
     * clean up expired entries.
     *
     * @return the collection of node IDs that were removed due to expiration
     */
    Collection<String> removeExpiredNodes();

    /** Factory interface for creating ManagementBlocklistHandler instances. */
    interface Factory {
        ManagementBlocklistHandler create();
    }
}
