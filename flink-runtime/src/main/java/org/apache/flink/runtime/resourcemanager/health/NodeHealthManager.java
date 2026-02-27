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

import java.time.Duration;
import java.util.Collection;

/**
 * Interface for managing node health status and quarantine.
 *
 * <p>This interface provides mechanisms to check node health, mark nodes as quarantined, remove
 * quarantine, and list all node health statuses.
 */
public interface NodeHealthManager {

    /**
     * Checks if a node with the given resource ID is healthy.
     *
     * @param resourceID the resource ID of the node
     * @return true if the node is healthy, false otherwise
     */
    boolean isHealthy(ResourceID resourceID);

    /**
     * Marks a node as quarantined for a specified duration.
     *
     * @param resourceID the resource ID of the node
     * @param hostname the hostname of the node
     * @param reason the reason for quarantining
     * @param duration the duration of the quarantine
     */
    void markQuarantined(ResourceID resourceID, String hostname, String reason, Duration duration);

    /**
     * Removes the quarantine from a node.
     *
     * @param resourceID the resource ID of the node
     */
    void removeQuarantine(ResourceID resourceID);

    /**
     * Lists all node health statuses.
     *
     * @return a collection of all node health statuses
     */
    Collection<NodeHealthStatus> listAll();

    /** Cleans up expired quarantine entries. */
    void cleanupExpired();
}
