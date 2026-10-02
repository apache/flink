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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Provides context about a completed checkpoint, allowing {@link CheckpointListener}
 * implementations to distinguish between a global checkpoint and a regional checkpoint.
 *
 * <p>A <b>global checkpoint</b> is one where all tasks acknowledged successfully. A <b>regional
 * checkpoint</b> is one where some tasks failed to acknowledge and their state was replaced by
 * state from a previous successful checkpoint.
 *
 * <p>This class provides:
 *
 * <ul>
 *   <li>{@link #isGlobalCheckpoint()} — whether all tasks contributed current state
 *   <li>{@link #getFallbackCheckpointSubtasks()} — which subtasks (by operator name and subtask
 *       index) used historical state, grouped by the fallback checkpoint ID they reference
 * </ul>
 *
 * <p>For a global checkpoint, {@link #isGlobalCheckpoint()} returns {@code true} and the fallback
 * map is empty.
 */
@PublicEvolving
public class RegionalCheckpointInfo {

    /** Singleton instance representing a global checkpoint (no fallback subtasks). */
    private static final RegionalCheckpointInfo GLOBAL =
            new RegionalCheckpointInfo(Collections.emptyMap());

    /**
     * Mapping from fallback checkpointId to the set of operator-subtask identifiers whose state
     * originates from that historical checkpoint rather than the current one.
     *
     * <p>Each entry in the set is formatted as "operatorName#subtaskIndex" (e.g., "Source:
     * my_source -> Sink: my_sink#0"). In practice, implementations typically only need to check
     * {@link #isGlobalCheckpoint()} or use {@link #getFallbackCheckpointIds()} to determine which
     * historical checkpoints are referenced.
     */
    private final Map<Long, Set<String>> fallbackCheckpointSubtasks;

    public RegionalCheckpointInfo(Map<Long, Set<String>> fallbackCheckpointSubtasks) {
        this.fallbackCheckpointSubtasks = Collections.unmodifiableMap(fallbackCheckpointSubtasks);
    }

    /** Returns a {@link RegionalCheckpointInfo} representing a global checkpoint. */
    public static RegionalCheckpointInfo globalCheckpoint() {
        return GLOBAL;
    }

    /**
     * Returns {@code true} if this is a global checkpoint where all tasks acknowledged
     * successfully.
     */
    public boolean isGlobalCheckpoint() {
        return fallbackCheckpointSubtasks.isEmpty();
    }

    /**
     * Returns the set of fallback checkpoint IDs referenced by this regional checkpoint.
     *
     * <p>For a global checkpoint, this returns an empty set. For a regional checkpoint, each ID in
     * the returned set represents a historical checkpoint whose state is used by some subtasks in
     * this completed checkpoint.
     */
    public Set<Long> getFallbackCheckpointIds() {
        return fallbackCheckpointSubtasks.keySet();
    }

    /**
     * Returns the full mapping from fallback checkpoint IDs to the set of subtask identifiers whose
     * state originates from that historical checkpoint.
     *
     * <p>Each subtask identifier is a string in the format "operatorName#subtaskIndex".
     *
     * <p>For a global checkpoint, this returns an empty map.
     */
    public Map<Long, Set<String>> getFallbackCheckpointSubtasks() {
        return fallbackCheckpointSubtasks;
    }
}
