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
import java.util.Collections;
import java.util.Set;

/** No-op implementation of {@link ManagementBlocklistHandler} that does nothing. */
public class NoOpManagementBlocklistHandler implements ManagementBlocklistHandler {

    @Override
    public void addBlockedNode(String nodeId, String reason, Duration duration) {
        // No-op
    }

    @Override
    public boolean removeBlockedNode(String nodeId) {
        return false;
    }

    @Override
    public Set<BlockedNode> getAllBlockedNodes() {
        return Collections.emptySet();
    }

    @Override
    public boolean isNodeBlocked(String nodeId) {
        return false;
    }

    @Override
    public Collection<String> removeExpiredNodes() {
        return Collections.emptyList();
    }

    /** Factory for creating {@link NoOpManagementBlocklistHandler} instances. */
    public static class Factory implements ManagementBlocklistHandler.Factory {

        @Override
        public ManagementBlocklistHandler create() {
            return new NoOpManagementBlocklistHandler();
        }
    }
}
