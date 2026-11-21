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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

/**
 * Tracks, on a best-effort basis, how long recent checkpoints took to complete from the perspective
 * of a single {@link SourceCoordinator}, keeping a bounded rolling window of the most recent
 * durations. This state is not checkpointed: it is rebuilt from scratch on coordinator
 * restart/failover, which is acceptable since it only feeds a soft, self-correcting rate-limit
 * signal rather than exactly-once state.
 */
@Internal
class CheckpointDurationTracker {

    private final Map<Long, Long> checkpointStartTimestamps = new HashMap<>();
    private final Deque<Long> recentDurationsMillis = new ArrayDeque<>();
    private final int windowSize;

    CheckpointDurationTracker(int windowSize) {
        this.windowSize = windowSize;
    }

    void onCheckpointStart(long checkpointId, long nowMillis) {
        checkpointStartTimestamps.put(checkpointId, nowMillis);
    }

    /**
     * Records the completion of a checkpoint and returns its duration, if a matching start
     * timestamp was recorded. Also purges any older, still-pending start timestamps, since a
     * checkpoint completion implies all lower checkpoint IDs are no longer pending.
     */
    OptionalLong onCheckpointComplete(long checkpointId, long nowMillis) {
        Long start = checkpointStartTimestamps.remove(checkpointId);
        checkpointStartTimestamps.keySet().removeIf(id -> id <= checkpointId);
        if (start == null) {
            return OptionalLong.empty();
        }
        long duration = Math.max(0, nowMillis - start);
        recentDurationsMillis.addLast(duration);
        while (recentDurationsMillis.size() > windowSize) {
            recentDurationsMillis.removeFirst();
        }
        return OptionalLong.of(duration);
    }

    List<Long> getRecentDurations() {
        return new ArrayList<>(recentDurationsMillis);
    }
}
