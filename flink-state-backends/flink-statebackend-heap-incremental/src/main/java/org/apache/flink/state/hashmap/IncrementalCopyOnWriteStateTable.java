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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateTable;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

import java.util.HashMap;
import java.util.Map;

class IncrementalCopyOnWriteStateTable<K, N, S> extends CopyOnWriteStateTable<K, N, S> {

    public IncrementalCopyOnWriteStateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
            TypeSerializer<K> keySerializer) {
        super(keyContext, metaInfo, keySerializer);
    }

    @Override
    protected CopyOnWriteStateMap<K, N, S> createStateMap() {
        return new IncrementalCopyOnWriteStateMap<>(
                getStateSerializer(), getMetaInfo().getName(), keyGroupRange);
    }

    public Map<Integer, Integer> getMapVersions() {
        Map<Integer, Integer> versions = new HashMap<>();
        for (int i = 0; i < keyGroupedStateMaps.length; i++) {
            versions.put(
                    getKeyGroupOffset() + i,
                    ((IncrementalCopyOnWriteStateMap<K, N, S>) keyGroupedStateMaps[i])
                            .getStateMapVersion());
        }
        return versions;
    }

    @Override
    public IncrementalCopyOnWriteStateTableSnapshot<K, N, S> stateSnapshot() {
        return new IncrementalCopyOnWriteStateTableSnapshot<>(
                this,
                getKeySerializer().duplicate(),
                getNamespaceSerializer().duplicate(),
                getStateSerializer().duplicate(),
                getMetaInfo()
                        .getStateSnapshotTransformFactory()
                        .createForDeserializedState()
                        .orElse(null));
    }
}
