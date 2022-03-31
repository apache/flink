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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.List;
import java.util.Map;

/**
 * Base class for heap {@link SnapshotResources}.
 *
 * @param <K> type of key
 */
@Internal
public class HeapSnapshotResourcesBase<K> implements SnapshotResources {
    protected final List<StateMetaInfoSnapshot> metaInfoSnapshots;
    protected final Map<StateUID, StateSnapshot> cowStateStableSnapshots;
    protected final Map<StateUID, Integer> stateNamesToId;
    protected final TypeSerializer<K> keySerializer;

    protected HeapSnapshotResourcesBase(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> cowStateStableSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            TypeSerializer<K> keySerializer) {
        this.metaInfoSnapshots = metaInfoSnapshots;
        this.cowStateStableSnapshots = cowStateStableSnapshots;
        this.stateNamesToId = stateNamesToId;
        this.keySerializer = keySerializer;
    }

    @Override
    public void release() {
        for (StateSnapshot stateSnapshot : cowStateStableSnapshots.values()) {
            stateSnapshot.release();
        }
    }

    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return metaInfoSnapshots;
    }

    public Map<StateUID, StateSnapshot> getCowStateStableSnapshots() {
        return cowStateStableSnapshots;
    }

    public Map<StateUID, Integer> getStateNamesToId() {
        return stateNamesToId;
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }
}
