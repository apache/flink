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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalListState;

import java.util.Collection;
import java.util.List;

/**
 * Delegated partitioned {@link ListState} that forwards changes to {@link StateChange} upon {@link
 * ListState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class ChangelogListState<K, N, V>
        extends AbstractChangelogState<K, N, List<V>, InternalListState<K, N, V>>
        implements InternalListState<K, N, V> {

    ChangelogListState(InternalListState<K, N, V> delegatedState) {
        super(delegatedState);
    }

    @Override
    public void update(List<V> values) throws Exception {
        delegatedState.update(values);
    }

    @Override
    public void addAll(List<V> values) throws Exception {
        delegatedState.addAll(values);
    }

    @Override
    public void updateInternal(List<V> valueToStore) throws Exception {
        delegatedState.updateInternal(valueToStore);
    }

    @Override
    public void add(V value) throws Exception {
        delegatedState.add(value);
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        delegatedState.mergeNamespaces(target, sources);
    }

    @Override
    public List<V> getInternal() throws Exception {
        return delegatedState.getInternal();
    }

    @Override
    public Iterable<V> get() throws Exception {
        return delegatedState.get();
    }

    @Override
    public void clear() {
        delegatedState.clear();
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> listState) {
        return (IS) new ChangelogListState<>((InternalListState<K, N, SV>) listState);
    }
}
