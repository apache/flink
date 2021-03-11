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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * Delegated partitioned {@link MapState} that forwards changes to {@link StateChange} upon {@link
 * MapState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <UK> The type of the keys in the state.
 * @param <UV> The type of the values in the state.
 */
class ChangelogMapState<K, N, UK, UV>
        extends AbstractChangelogState<K, N, Map<UK, UV>, InternalMapState<K, N, UK, UV>>
        implements InternalMapState<K, N, UK, UV> {

    ChangelogMapState(InternalMapState<K, N, UK, UV> delegatedState) {
        super(delegatedState);
    }

    @Override
    public UV get(UK key) throws Exception {
        return delegatedState.get(key);
    }

    @Override
    public void put(UK key, UV value) throws Exception {
        delegatedState.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) throws Exception {
        delegatedState.putAll(map);
    }

    @Override
    public void remove(UK key) throws Exception {
        delegatedState.remove(key);
    }

    @Override
    public boolean contains(UK key) throws Exception {
        return delegatedState.contains(key);
    }

    @Override
    public Iterable<Map.Entry<UK, UV>> entries() throws Exception {
        return delegatedState.entries();
    }

    @Override
    public Iterable<UK> keys() throws Exception {
        return delegatedState.keys();
    }

    @Override
    public Iterable<UV> values() throws Exception {
        return delegatedState.values();
    }

    @Override
    public Iterator<Map.Entry<UK, UV>> iterator() throws Exception {
        return delegatedState.iterator();
    }

    @Override
    public boolean isEmpty() throws Exception {
        return delegatedState.isEmpty();
    }

    @Override
    public void clear() {
        delegatedState.clear();
    }

    @SuppressWarnings("unchecked")
    static <UK, UV, K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> mapState) {
        return (IS) new ChangelogMapState<>((InternalMapState<K, N, UK, UV>) mapState);
    }
}
