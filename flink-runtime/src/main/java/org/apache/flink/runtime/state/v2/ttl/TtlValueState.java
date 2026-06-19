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
import org.apache.flink.runtime.state.v2.internal.InternalValueState;

/**
 * This class wraps value state with TTL logic.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the user value of state with TTL
 */
class TtlValueState<K, N, T>
        extends AbstractTtlState<K, N, T, TtlValue<T>, InternalValueState<K, N, TtlValue<T>>>
        implements InternalValueState<K, N, T> {

    protected TtlValueState(
            TtlStateContext<InternalValueState<K, N, TtlValue<T>>, T> ttlStateContext) {
        super(ttlStateContext);
    }

    @Override
    public StateFuture<T> asyncValue() {
        return original.asyncValue().thenApply((ttlValue) -> getElementWithTtlCheck(ttlValue));
    }

    @Override
    public StateFuture<Void> asyncUpdate(T value) {
        return original.asyncUpdate(value == null ? null : wrapWithTs(value));
    }

    @Override
    public T value() {
        TtlValue<T> ttlValue = original.value();
        return getElementWithTtlCheck(ttlValue);
    }

    @Override
    public void update(T value) {
        original.update(value == null ? null : wrapWithTs(value));
    }
}
