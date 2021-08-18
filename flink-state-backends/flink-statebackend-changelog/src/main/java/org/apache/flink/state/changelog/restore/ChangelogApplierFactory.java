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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.internal.InternalValueState;

/**
 * {@link StateChangeApplier} factory. It's purpose is to decouple restore/apply logic from state
 * logic.
 */
@Internal
public interface ChangelogApplierFactory {

    <K, N, UK, UV> KvStateChangeApplier<K, N> forMap(
            InternalMapState<K, N, UK, UV> map, InternalKeyContext<K> keyContext);

    <K, N, T> KvStateChangeApplier<K, N> forValue(
            InternalValueState<K, N, T> value, InternalKeyContext<K> keyContext);

    <K, N, T> KvStateChangeApplier<K, N> forList(
            InternalListState<K, N, T> list, InternalKeyContext<K> keyContext);

    <K, N, T> KvStateChangeApplier<K, N> forReducing(
            InternalReducingState<K, N, T> reducing, InternalKeyContext<K> keyContext);

    <K, N, IN, SV, OUT> KvStateChangeApplier<K, N> forAggregating(
            InternalAggregatingState<K, N, IN, SV, OUT> aggregating,
            InternalKeyContext<K> keyContext);

    <T> StateChangeApplier forPriorityQueue(
            KeyGroupedInternalPriorityQueue<T> priorityQueue, TypeSerializer<T> serializer);
}
