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

package org.apache.flink.state.hashmap;

import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

final class IncrementalSnapshot {

    static class Versions {
        private final Map<String, Map<Integer, Integer>> kvVersions;

        Versions(Map<String, Map<Integer, Integer>> kvVersions) {
            this.kvVersions = kvVersions;
        }
    }

    private final SnapshotResult<KeyedStateHandle> stateSnapshot;
    private final Map<String, Map<Integer, Integer>> kvVersions;
    private final long checkpointID;

    public IncrementalSnapshot(
            Map<String, Map<Integer, Integer>> kvVersions,
            SnapshotResult<KeyedStateHandle> stateSnapshot,
            long checkpointID) {
        this.kvVersions = checkNotNull(kvVersions);
        this.stateSnapshot = checkNotNull(stateSnapshot);
        this.checkpointID = checkpointID;
    }

    public IncrementalSnapshot(
            Versions kvVersions,
            SnapshotResult<KeyedStateHandle> stateSnapshot,
            long checkpointID) {
        this.kvVersions = checkNotNull(kvVersions.kvVersions);
        this.stateSnapshot = checkNotNull(stateSnapshot);
        this.checkpointID = checkpointID;
    }

    public int getVersions(String kvStateName, int keyGroup) {
        return kvVersions.getOrDefault(kvStateName, emptyMap()).getOrDefault(keyGroup, -1);
    }

    public SnapshotResult<KeyedStateHandle> getStateSnapshot() {
        return stateSnapshot;
    }

    public Map<String, Map<Integer, Integer>> getAllVersions() {
        return kvVersions;
    }

    public long getCheckpointID() {
        return checkpointID;
    }
}
