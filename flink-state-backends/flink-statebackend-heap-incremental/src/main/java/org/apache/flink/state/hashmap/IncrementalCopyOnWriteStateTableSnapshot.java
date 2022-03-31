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
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

final class IncrementalCopyOnWriteStateTableSnapshot<K, N, S>
        extends CopyOnWriteStateTableSnapshot<K, N, S> {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalCopyOnWriteStateTableSnapshot.class);

    public IncrementalCopyOnWriteStateTableSnapshot(
            CopyOnWriteStateTable<K, N, S> owningStateTable,
            TypeSerializer<K> localKeySerializer,
            TypeSerializer<N> localNamespaceSerializer,
            TypeSerializer<S> localStateSerializer,
            StateSnapshotTransformer<S> stateSnapshotTransformer) {
        super(
                owningStateTable,
                localKeySerializer,
                localNamespaceSerializer,
                localStateSerializer,
                stateSnapshotTransformer);
    }

    public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId, int version)
            throws IOException {
        LOG.trace("write state in key group: {}, checkpointed version: {}", keyGroupId, version);
        @SuppressWarnings("unchecked")
        IncrementalCopyOnWriteStateMapSnapshot<K, N, S> stateMapSnapshot =
                (IncrementalCopyOnWriteStateMapSnapshot<K, N, S>)
                        getStateMapSnapshotForKeyGroup(keyGroupId);
        stateMapSnapshot.writeState(
                localKeySerializer,
                localNamespaceSerializer,
                localStateSerializer,
                dov,
                stateSnapshotTransformer,
                version);
        stateMapSnapshot.release();
    }

    public List<Runnable> getConfirmCallbacks() {
        List<Runnable> result = new ArrayList<>(stateMapSnapshots.size());
        for (CopyOnWriteStateMapSnapshot<K, N, S> snapshot : stateMapSnapshots) {
            result.add(
                    ((IncrementalCopyOnWriteStateMapSnapshot<K, N, S>) snapshot)
                            .getConfirmCallback());
        }
        return result;
    }

    public String getStateName() {
        return owningStateTable.getMetaInfo().getName();
    }
}
