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

package org.apache.flink.runtime.asyncprocessing.declare.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;

/** Value state wrapped with declared namespace. */
@Internal
class ValueStateWithDeclaredNamespace<K, N, V> extends StateWithDeclaredNamespace<K, N, V>
        implements InternalValueState<K, N, V> {
    private final InternalValueState<K, N, V> state;

    public ValueStateWithDeclaredNamespace(
            InternalValueState<K, N, V> state, DeclaredVariable<N> declaredNamespace) {
        super(state, declaredNamespace);
        this.state = state;
    }

    @Override
    public StateFuture<Void> asyncClear() {
        resetNamespace();
        return state.asyncClear();
    }

    @Override
    public StateFuture<V> asyncValue() {
        resetNamespace();
        return state.asyncValue();
    }

    @Override
    public StateFuture<Void> asyncUpdate(V value) {
        resetNamespace();
        return state.asyncUpdate(value);
    }

    @Override
    public void clear() {
        state.clear();
    }

    @Override
    public V value() {
        return state.value();
    }

    @Override
    public void update(V value) {
        state.update(value);
    }
}
