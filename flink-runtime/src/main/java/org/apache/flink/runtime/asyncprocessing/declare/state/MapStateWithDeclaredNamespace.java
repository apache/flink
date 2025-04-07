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
import org.apache.flink.runtime.state.v2.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/** MapState wrapped with declared namespace. */
@Internal
class MapStateWithDeclaredNamespace<K, N, UK, UV> extends StateWithDeclaredNamespace<K, N, UV>
        implements InternalMapState<K, N, UK, UV> {

    private final InternalMapState<K, N, UK, UV> state;

    public MapStateWithDeclaredNamespace(
            InternalMapState<K, N, UK, UV> state, DeclaredVariable<N> declaredNamespace) {
        super(state, declaredNamespace);
        this.state = state;
    }

    @Override
    public StateFuture<UV> asyncGet(UK key) {
        resetNamespace();
        return state.asyncGet(key);
    }

    @Override
    public StateFuture<Void> asyncPut(UK key, UV value) {
        resetNamespace();
        return state.asyncPut(key, value);
    }

    @Override
    public StateFuture<Void> asyncPutAll(Map<UK, UV> map) {
        resetNamespace();
        return state.asyncPutAll(map);
    }

    @Override
    public StateFuture<Void> asyncRemove(UK key) {
        resetNamespace();
        return state.asyncRemove(key);
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK key) {
        resetNamespace();
        return state.asyncContains(key);
    }

    @Override
    public StateFuture<StateIterator<Entry<UK, UV>>> asyncEntries() {
        resetNamespace();
        return state.asyncEntries();
    }

    @Override
    public StateFuture<StateIterator<UK>> asyncKeys() {
        resetNamespace();
        return state.asyncKeys();
    }

    @Override
    public StateFuture<StateIterator<UV>> asyncValues() {
        resetNamespace();
        return state.asyncValues();
    }

    @Override
    public StateFuture<Boolean> asyncIsEmpty() {
        resetNamespace();
        return state.asyncIsEmpty();
    }

    @Override
    public StateFuture<Void> asyncClear() {
        resetNamespace();
        return state.asyncClear();
    }

    @Override
    public UV get(UK key) {
        return state.get(key);
    }

    @Override
    public void put(UK key, UV value) {
        state.put(key, value);
    }

    @Override
    public void putAll(Map<UK, UV> map) {
        state.putAll(map);
    }

    @Override
    public void remove(UK key) {
        state.remove(key);
    }

    @Override
    public boolean contains(UK key) {
        return state.contains(key);
    }

    @Override
    public Iterable<Entry<UK, UV>> entries() {
        return state.entries();
    }

    @Override
    public Iterable<UK> keys() {
        return state.keys();
    }

    @Override
    public Iterable<UV> values() {
        return state.values();
    }

    @Override
    public Iterator<Entry<UK, UV>> iterator() {
        return state.iterator();
    }

    @Override
    public boolean isEmpty() {
        return state.isEmpty();
    }

    @Override
    public void clear() {
        state.clear();
    }
}
