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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * TM-side registry of {@link org.apache.flink.runtime.state.StateObject StateObjects}, each
 * representing one or more changelog segments. Changelog segments are uploaded by {@link
 * org.apache.flink.runtime.state.changelog.StateChangelogWriter StateChangelogWriters} of a {@link
 * org.apache.flink.runtime.state.changelog.StateChangelogStorage StateChangelogStorage}.
 *
 * <p>Initially, when {@link #startTracking(StreamStateHandle, Set) starting the tracking}, the
 * ownership of a changelog segments is not clear, and it is assumed that JM <strong>might</strong>
 * be the owner. Once the backends are not using the segments, JM can not become an owner anymore.
 * the state is discarded.
 *
 * <p>However, if at any point it becomes known that JM is the owner, tracking is {@link
 * #stopTracking(StreamStateHandle) stopped} and the state will not be discarded.
 *
 * <p>It is the client responsibility to make sure that JM can not become an owner when calling
 * {@link #notUsed(StreamStateHandle, UUID)}.
 */
@Internal
public interface TaskChangelogRegistry {

    /** Start tracking the state uploaded for the given backends. */
    void startTracking(StreamStateHandle handle, Set<UUID> backendIDs);

    /** Stop tracking the state, so that it's not tracked (some other component is doing that). */
    void stopTracking(StreamStateHandle handle);

    /**
     * Mark the state as unused by the given backend, e.g. if it was pre-emptively uploaded and
     * materialized. Once no backend is using the state, it is discarded (unless it was {@link
     * #stopTracking(StreamStateHandle) unregistered} earlier).
     */
    void notUsed(StreamStateHandle handle, UUID backendId);

    TaskChangelogRegistry NO_OP =
            new TaskChangelogRegistry() {
                @Override
                public void startTracking(StreamStateHandle handle, Set<UUID> backendIDs) {}

                @Override
                public void stopTracking(StreamStateHandle handle) {}

                @Override
                public void notUsed(StreamStateHandle handle, UUID backendId) {}
            };

    static TaskChangelogRegistry defaultChangelogRegistry(int numAsyncDiscardThreads) {
        return defaultChangelogRegistry(Executors.newFixedThreadPool(numAsyncDiscardThreads));
    }

    @VisibleForTesting
    static TaskChangelogRegistry defaultChangelogRegistry(Executor executor) {
        return new TaskChangelogRegistryImpl(executor);
    }
}
