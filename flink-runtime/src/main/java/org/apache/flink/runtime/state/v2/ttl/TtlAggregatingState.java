/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.v2.ttl;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.state.ttl.TtlStateContext;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.runtime.state.v2.internal.InternalAggregatingState;

import java.util.Collection;

/**
 * This class wraps aggregating state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <ACC> The type of the accumulator (intermediate aggregate state).
 * @param <OUT> Type of the value extracted from the state
 */
class TtlAggregatingState<K, N, IN, ACC, OUT>
        extends AbstractTtlState<
                K, N, ACC, TtlValue<ACC>, InternalAggregatingState<K, N, IN, TtlValue<ACC>, OUT>>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    TtlAggregatingState(
            TtlStateContext<InternalAggregatingState<K, N, IN, TtlValue<ACC>, OUT>, ACC>
                    ttlStateContext,
            TtlAggregateFunction<IN, ACC, OUT> aggregateFunction) {
        super(ttlStateContext);
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources) {
        return original.asyncMergeNamespaces(target, sources);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        original.mergeNamespaces(target, sources);
    }

    @Override
    public StateFuture<OUT> asyncGet() {
        return original.asyncGet();
    }

    @Override
    public StateFuture<Void> asyncAdd(IN value) {
        return original.asyncAdd(value);
    }

    @Override
    public OUT get() {
        return original.get();
    }

    @Override
    public void add(IN value) {
        original.add(value);
    }

    @Override
    public void clear() {
        original.clear();
    }

    @Override
    public StateFuture<ACC> asyncGetInternal() {
        return original.asyncGetInternal().thenApply(ttlValue -> getElementWithTtlCheck(ttlValue));
    }

    @Override
    public StateFuture<Void> asyncUpdateInternal(ACC valueToStore) {
        return original.asyncUpdateInternal(wrapWithTs(valueToStore));
    }

    @Override
    public ACC getInternal() {
        TtlValue<ACC> ttlValue = original.getInternal();
        return getElementWithTtlCheck(ttlValue);
    }

    @Override
    public void updateInternal(ACC valueToStore) {
        original.updateInternal(wrapWithTs(valueToStore));
    }
}
