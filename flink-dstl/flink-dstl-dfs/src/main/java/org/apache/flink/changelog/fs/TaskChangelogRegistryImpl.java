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
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

@Internal
@ThreadSafe
class TaskChangelogRegistryImpl implements TaskChangelogRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TaskChangelogRegistryImpl.class);

    private final Map<PhysicalStateHandleID, Long> entries = new ConcurrentHashMap<>();
    private final Executor executor;

    public TaskChangelogRegistryImpl(Executor executor) {
        this.executor = executor;
    }

    @Override
    public void startTracking(StreamStateHandle handle, long refCount) {
        Preconditions.checkState(refCount > 0, "Initial refCount of state must larger than zero");
        LOG.debug(
                "start tracking state, key: {}, state: {}",
                handle.getStreamStateHandleID(),
                handle);
        entries.put(handle.getStreamStateHandleID(), refCount);
    }

    @Override
    public void stopTracking(StreamStateHandle handle) {
        LOG.debug(
                "stop tracking state, key: {}, state: {}", handle.getStreamStateHandleID(), handle);
        entries.remove(handle.getStreamStateHandleID());
    }

    @Override
    public void release(StreamStateHandle handle) {
        PhysicalStateHandleID key = handle.getStreamStateHandleID();
        LOG.debug("state reference count decreased by one, key: {}, state: {}", key, handle);

        entries.compute(
                key,
                (handleID, refCount) -> {
                    if (refCount == null) {
                        LOG.warn("state is not in tracking, key: {}, state: {}", key, handle);
                        return null;
                    }

                    long newRefCount = refCount - 1;
                    if (newRefCount == 0) {
                        LOG.debug(
                                "state is not used by any backend, schedule discard: {}/{}",
                                key,
                                handle);
                        scheduleDiscard(handle);
                        return null;
                    } else {
                        return newRefCount;
                    }
                });
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
