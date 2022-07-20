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

import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

/**
 * This class is responsible for managing all {@link BlockedNode}s and performing them on resources.
 */
public interface BlocklistHandler {

    /**
     * Add new blocked node records. If a node (identified by node id) already exists, the newly
     * added one will be merged with the existing one.
     *
     * @param newNodes the new blocked node records
     */
    void addNewBlockedNodes(Collection<BlockedNode> newNodes);

    /**
     * Returns whether the given task manager is blocked (located on blocked nodes).
     *
     * @param taskManagerId ID of the task manager to query
     * @return true if the given task manager is blocked, otherwise false
     */
    boolean isBlockedTaskManager(ResourceID taskManagerId);

    /**
     * Get all blocked node ids.
     *
     * @return a set containing all blocked node ids
     */
    Set<String> getAllBlockedNodeIds();

    /**
     * Register a new blocklist listener.
     *
     * @param blocklistListener the newly registered listener
     */
    void registerBlocklistListener(BlocklistListener blocklistListener);

    /**
     * Deregister a blocklist listener.
     *
     * @param blocklistListener the listener to deregister
     */
    void deregisterBlocklistListener(BlocklistListener blocklistListener);

    /** Factory to instantiate {@link BlocklistHandler}. */
    interface Factory {

        /**
         * Instantiates a {@link BlocklistHandler}.
         *
         * @param blocklistContext the blocklist context
         * @param taskManagerNodeIdRetriever to map a task manager to the node it's located on
         * @param mainThreadExecutor to schedule the timeout check
         * @param log the logger
         * @return an instantiated blocklist handler.
         */
        BlocklistHandler create(
                BlocklistContext blocklistContext,
                Function<ResourceID, String> taskManagerNodeIdRetriever,
                ComponentMainThreadExecutor mainThreadExecutor,
                Logger log);
    }
}
