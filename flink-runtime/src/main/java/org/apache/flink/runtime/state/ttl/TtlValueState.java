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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.runtime.state.internal.InternalValueState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

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
    TtlValueState(TtlStateContext<InternalValueState<K, N, TtlValue<T>>, T> tTtlStateContext) {
        super(tTtlStateContext);
    }

    @Override
    public T value() throws IOException {
        accessCallback.run();
        return getWithTtlCheckAndUpdate(original::value, original::update);
    }

    @Override
    public void update(T value) throws IOException {
        accessCallback.run();
        original.update(value == null ? null : wrapWithTs(value));
    }

    @Nullable
    @Override
    public TtlValue<T> getUnexpiredOrNull(@Nonnull TtlValue<T> ttlValue) {
        return expired(ttlValue) ? null : ttlValue;
    }
}
