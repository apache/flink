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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMapSnapshot;
import org.apache.flink.state.hashmap.RemovalLog.StateEntryRemoval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.NavigableMap;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

class IncrementalCopyOnWriteStateMapSnapshot<K, N, S> extends CopyOnWriteStateMapSnapshot<K, N, S> {

    private final NavigableMap<Integer, Set<StateEntryRemoval<K, N>>> removedByVersion;
    private final Runnable confirmCallback;

    IncrementalCopyOnWriteStateMapSnapshot(IncrementalCopyOnWriteStateMap<K, N, S> owningStateMap) {
        super(owningStateMap);
        RemovalLog<K, N> removalLog = owningStateMap.getRemovalLog();
        int version = snapshotVersion;
        this.confirmCallback = () -> removalLog.confirmed(version);
        this.removedByVersion = removalLog.snapshot();
    }

    public void writeState(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            @Nonnull DataOutputView dov,
            @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer,
            int checkpointedVersion)
            throws IOException {

        Set<StateEntryRemoval<K, N>> removals =
                removedByVersion.tailMap(checkpointedVersion, false).values().stream()
                        .flatMap(Collection::stream)
                        .collect(toSet());

        IncrementalCopyOnWriteStateMapEntryIterator<K, N, S> iterator =
                new IncrementalCopyOnWriteStateMapEntryIterator<>(
                        snapshotData, checkpointedVersion, stateSnapshotTransformer);

        IncrementalKeyGroupWriter<K, N, S> writer =
                new IncrementalKeyGroupWriter<>(
                        keySerializer, namespaceSerializer, stateSerializer);

        writer.write(dov, iterator, removals);
    }

    public Runnable getConfirmCallback() {
        return confirmCallback;
    }
}
