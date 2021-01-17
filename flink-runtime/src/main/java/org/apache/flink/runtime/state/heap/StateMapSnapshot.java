/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Base class for snapshots of a {@link StateMap}.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public abstract class StateMapSnapshot<K, N, S, T extends StateMap<K, N, S>> {

    /** The {@link StateMap} from which this snapshot was created. */
    protected final T owningStateMap;

    public StateMapSnapshot(T stateMap) {
        this.owningStateMap = Preconditions.checkNotNull(stateMap);
    }

    /** Returns true iff the given state map is the owner of this snapshot object. */
    public boolean isOwner(T stateMap) {
        return owningStateMap == stateMap;
    }

    /** Release the snapshot. */
    public void release() {}

    /**
     * Writes the state in this snapshot to output. The state need to be transformed with the given
     * transformer if the transformer is non-null.
     *
     * @param keySerializer the key serializer.
     * @param namespaceSerializer the namespace serializer.
     * @param stateSerializer the state serializer.
     * @param dov the output.
     * @param stateSnapshotTransformer state transformer, and can be null.
     * @throws IOException on write-related problems.
     */
    public abstract void writeState(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer,
            @Nonnull DataOutputView dov,
            @Nullable StateSnapshotTransformer<S> stateSnapshotTransformer)
            throws IOException;
}
