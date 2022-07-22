/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executor;

@Internal
@ThreadSafe
class TaskChangelogRegistryImpl implements TaskChangelogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskChangelogRegistryImpl.class);

    private final Map<PhysicalStateHandleID, Set<UUID>> entries = new ConcurrentHashMap<>();
    private final Executor executor;

    public TaskChangelogRegistryImpl(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void startTracking(StreamStateHandle handle, Set<UUID> backendIDs) {
        LOG.debug(
                "start tracking state, key: {}, state: {}",
                handle.getStreamStateHandleID(),
                handle);
        entries.put(handle.getStreamStateHandleID(), new CopyOnWriteArraySet<>(backendIDs));
    }

    @Override
    public void stopTracking(StreamStateHandle handle) {
        LOG.debug(
                "stop tracking state, key: {}, state: {}", handle.getStreamStateHandleID(), handle);
        entries.remove(handle.getStreamStateHandleID());
    }

    @Override
    public void notUsed(StreamStateHandle handle, UUID backendId) {
        PhysicalStateHandleID key = handle.getStreamStateHandleID();
        LOG.debug("backend {} not using state, key: {}, state: {}", backendId, key, handle);
        Set<UUID> backends = entries.get(key);
        if (backends == null) {
            LOG.warn("backend {} was not using state, key: {}, state: {}", backendId, key, handle);
            return;
        }
        backends.remove(backendId);
        if (backends.isEmpty() && entries.remove(key) != null) {
            LOG.debug("state is not used by any backend, schedule discard: {}/{}", key, handle);
            scheduleDiscard(handle);
        }
    }

    private void scheduleDiscard(StreamStateHandle handle) {
        executor.execute(
                () -> {
                    try {
                        LOG.trace("discard uploaded but unused state changes: {}", handle);
                        handle.discardState();
                    } catch (Exception e) {
                        LOG.warn("unable to discard uploaded but unused state changes", e);
                    }
                });
    }
}
