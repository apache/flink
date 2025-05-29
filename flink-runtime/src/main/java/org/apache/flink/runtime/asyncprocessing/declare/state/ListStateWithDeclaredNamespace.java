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
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.asyncprocessing.declare.DeclaredVariable;
import org.apache.flink.runtime.state.v2.internal.InternalListState;

import java.util.Collection;
import java.util.List;

/** ListState wrapped with declared namespace. */
@Internal
class ListStateWithDeclaredNamespace<K, N, V> extends StateWithDeclaredNamespace<K, N, V>
        implements InternalListState<K, N, V> {
    private final InternalListState<K, N, V> state;

    public ListStateWithDeclaredNamespace(
            InternalListState<K, N, V> state, DeclaredVariable<N> declaredNamespace) {
        super(state, declaredNamespace);
        this.state = state;
    }

    @Override
    public StateFuture<StateIterator<V>> asyncGet() {
        resetNamespace();
        return state.asyncGet();
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        resetNamespace();
        return state.asyncAdd(value);
    }

    @Override
    public StateFuture<Void> asyncUpdate(List<V> values) {
        resetNamespace();
        return state.asyncUpdate(values);
    }

    @Override
    public StateFuture<Void> asyncAddAll(List<V> values) {
        resetNamespace();
        return state.asyncAddAll(values);
    }

    @Override
    public StateFuture<Void> asyncClear() {
        resetNamespace();
        return state.asyncClear();
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources) {
        resetNamespace();
        return state.asyncMergeNamespaces(target, sources);
    }

    @Override
    public Iterable<V> get() {
        return state.get();
    }

    @Override
    public void add(V value) {
        state.add(value);
    }

    @Override
    public void update(List<V> values) {
        state.update(values);
    }

    @Override
    public void addAll(List<V> values) {
        state.addAll(values);
    }

    @Override
    public void clear() {
        state.clear();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        state.mergeNamespaces(target, sources);
    }
}
