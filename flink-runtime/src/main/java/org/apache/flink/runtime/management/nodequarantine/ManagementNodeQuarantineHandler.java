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
 * Interface for managing cluster-level node quarantine. This is independent of batch execution
 * blocklist and speculative execution.
 */
public interface ManagementNodeQuarantineHandler {

    /**
     * Add a node to the management node quarantine.
     *
     * @param nodeId the ID of the node to quarantine
     * @param reason the reason for quarantining the node
     * @param duration the duration for which the node should be quarantined
     */
    void addQuarantinedNode(String nodeId, String reason, Duration duration);

    /**
     * Remove a node from the management node quarantine.
     *
     * @param nodeId the ID of the node to remove from quarantine
     * @return true if the node was removed, false if it wasn't in the quarantine list
     */
    boolean removeQuarantinedNode(String nodeId);

    /**
     * Get all currently quarantined nodes in the management node quarantine.
     *
     * @return a set of all quarantined nodes
     */
    Set<BlockedNode> getAllQuarantinedNodes();

    /**
     * Check if a specific node is quarantined in the management node quarantine.
     *
     * @param nodeId the ID of the node to check
     * @return true if the node is quarantined, false otherwise
     */
    boolean isNodeQuarantined(String nodeId);

    /**
     * Remove all expired nodes from the management node quarantine. This method is called
     * periodically to clean up expired entries.
     *
     * @return the collection of node IDs that were removed due to expiration
     */
    Collection<String> removeExpiredNodes();

    /** Factory interface for creating ManagementNodeQuarantineHandler instances. */
    interface Factory {
        ManagementNodeQuarantineHandler create();
    }
}
