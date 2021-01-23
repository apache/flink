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
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;

/**
 * Synchronous behavior for heap snapshot strategy.
 *
 * @param <K> The data type that the serializer serializes.
 */
class SyncSnapshotStrategySynchronicityBehavior<K>
        implements SnapshotStrategySynchronicityBehavior<K> {

    @Override
    public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
        // this triggers a synchronous execution from the main checkpointing thread.
        runnable.run();
    }

    @Override
    public boolean isAsynchronous() {
        return false;
    }

    @Override
    public <N, V> StateTable<K, N, V> newStateTable(
            InternalKeyContext<K> keyContext,
            RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo,
            TypeSerializer<K> keySerializer) {
        return new NestedMapsStateTable<>(keyContext, newMetaInfo, keySerializer);
    }
}
