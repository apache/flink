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

package org.apache.flink.state.changelog.restore;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

class ChangelogApplierFactoryImpl implements ChangelogApplierFactory {
    public static final ChangelogApplierFactoryImpl INSTANCE = new ChangelogApplierFactoryImpl();

    private ChangelogApplierFactoryImpl() {}

    @Override
    public <K, N, UK, UV> KvStateChangeApplier<K, N> forMap(
            InternalMapState<K, N, UK, UV> map, InternalKeyContext<K> keyContext) {
        return new MapStateChangeApplier<>(map, keyContext);
    }

    @Override
    public <K, N, T> KvStateChangeApplier<K, N> forList(
            InternalListState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ListStateChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, T> KvStateChangeApplier<K, N> forReducing(
            InternalReducingState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ReducingStateChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, IN, SV, OUT> KvStateChangeApplier<K, N> forAggregating(
            InternalAggregatingState<K, N, IN, SV, OUT> state, InternalKeyContext<K> keyContext) {
        return new AggregatingStateChangeApplier<>(keyContext, state);
    }

    @Override
    public <K, N, T> KvStateChangeApplier<K, N> forValue(
            InternalValueState<K, N, T> state, InternalKeyContext<K> keyContext) {
        return new ValueStateChangeApplier<>(keyContext, state);
    }

    @Override
    public <T> StateChangeApplier forPriorityQueue(
            KeyGroupedInternalPriorityQueue<T> queue, TypeSerializer<T> serializer) {
        return new PriorityQueueStateChangeApplier<>(queue, serializer);
    }
}
