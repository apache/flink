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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.heap.CopyOnWriteStateMap;

class IncrementalCopyOnWriteStateMap<K, N, S> extends CopyOnWriteStateMap<K, N, S> {
    private final RemovalLog<K, N> removalLog;

    IncrementalCopyOnWriteStateMap(
            TypeSerializer<S> stateSerializer, String name, KeyGroupRange keyGroupRange) {
        super(stateSerializer);
        this.removalLog = new RemovalLogImpl<>(name, keyGroupRange);
    }

    @Override
    public IncrementalCopyOnWriteStateMapSnapshot<K, N, S> stateSnapshot() {
        return new IncrementalCopyOnWriteStateMapSnapshot<>(this);
    }

    @Override
    protected void onVersionUpdate(int stateMapVersion) {
        removalLog.startNewVersion(stateMapVersion);
    }

    @Override
    protected StateMapEntry<K, N, S> putEntry(K key, N namespace) {
        removalLog.added(key, namespace);
        return super.putEntry(key, namespace);
    }

    @Override
    protected StateMapEntry<K, N, S> removeEntry(K key, N namespace) {
        removalLog.removed(key, namespace);
        return super.removeEntry(key, namespace);
    }

    public void removeOnRecovery(K key, N namespace) {
        super.removeEntry(key, namespace);
    }

    public RemovalLog<K, N> getRemovalLog() {
        return removalLog;
    }
}
