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
import org.apache.flink.runtime.state.v2.internal.InternalReducingState;

import java.util.Collection;

/** Reducing state wrapped with declared namespace. */
@Internal
class ReducingStateWithDeclaredNamespace<K, N, T> extends StateWithDeclaredNamespace<K, N, T>
        implements InternalReducingState<K, N, T> {
    private final InternalReducingState<K, N, T> state;

    public ReducingStateWithDeclaredNamespace(
            InternalReducingState<K, N, T> state, DeclaredVariable<N> declaredNamespace) {
        super(state, declaredNamespace);
        this.state = state;
    }

    @Override
    public StateFuture<T> asyncGet() {
        resetNamespace();
        return state.asyncGet();
    }

    @Override
    public StateFuture<Void> asyncAdd(T value) {
        resetNamespace();
        return state.asyncAdd(value);
    }

    @Override
    public StateFuture<Void> asyncClear() {
        resetNamespace();
        return state.asyncClear();
    }

    @Override
    public StateFuture<T> asyncGetInternal() {
        resetNamespace();
        return state.asyncGetInternal();
    }

    @Override
    public StateFuture<Void> asyncUpdateInternal(T valueToStore) {
        resetNamespace();
        return state.asyncUpdateInternal(valueToStore);
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources) {
        resetNamespace();
        return state.asyncMergeNamespaces(target, sources);
    }

    @Override
    public T get() {
        return state.get();
    }

    @Override
    public void add(T value) {
        state.add(value);
    }

    @Override
    public void clear() {
        state.clear();
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        state.mergeNamespaces(target, sources);
    }

    @Override
    public T getInternal() {
        return state.getInternal();
    }

    @Override
    public void updateInternal(T valueToStore) {
        state.updateInternal(valueToStore);
    }
}
