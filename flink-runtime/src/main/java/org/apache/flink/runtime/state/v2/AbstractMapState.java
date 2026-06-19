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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalMapState;

import java.util.Iterator;
import java.util.Map;

/**
 * A default implementation of {@link MapState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of partitioned key the state is associated to.
 * @param <UK> The type of user key of this state.
 * @param <V> The type of values kept internally in state.
 */
public class AbstractMapState<K, N, UK, V> extends AbstractKeyedState<K, N, V>
        implements InternalMapState<K, N, UK, V> {

    public AbstractMapState(StateRequestHandler stateRequestHandler, TypeSerializer<V> serializer) {
        super(stateRequestHandler, serializer);
    }

    @Override
    public StateFuture<V> asyncGet(UK key) {
        return handleRequest(StateRequestType.MAP_GET, key);
    }

    @Override
    public StateFuture<Void> asyncPut(UK key, V value) {
        return handleRequest(StateRequestType.MAP_PUT, Tuple2.of(key, value));
    }

    @Override
    public StateFuture<Void> asyncPutAll(Map<UK, V> map) {
        return handleRequest(StateRequestType.MAP_PUT_ALL, map);
    }

    @Override
    public StateFuture<Void> asyncRemove(UK key) {
        return handleRequest(StateRequestType.MAP_REMOVE, key);
    }

    @Override
    public StateFuture<Boolean> asyncContains(UK key) {
        return handleRequest(StateRequestType.MAP_CONTAINS, key);
    }

    @Override
    public StateFuture<StateIterator<Map.Entry<UK, V>>> asyncEntries() {
        return handleRequest(StateRequestType.MAP_ITER, null);
    }

    @Override
    public StateFuture<StateIterator<UK>> asyncKeys() {
        return handleRequest(StateRequestType.MAP_ITER_KEY, null);
    }

    @Override
    public StateFuture<StateIterator<V>> asyncValues() {
        return handleRequest(StateRequestType.MAP_ITER_VALUE, null);
    }

    @Override
    public StateFuture<Boolean> asyncIsEmpty() {
        return handleRequest(StateRequestType.MAP_IS_EMPTY, null);
    }

    @Override
    public V get(UK key) {
        return handleRequestSync(StateRequestType.MAP_GET, key);
    }

    @Override
    public void put(UK key, V value) {
        handleRequestSync(StateRequestType.MAP_PUT, Tuple2.of(key, value));
    }

    @Override
    public void putAll(Map<UK, V> map) {
        handleRequestSync(StateRequestType.MAP_PUT_ALL, map);
    }

    @Override
    public void remove(UK key) {
        handleRequestSync(StateRequestType.MAP_REMOVE, key);
    }

    @Override
    public boolean contains(UK key) {
        return handleRequestSync(StateRequestType.MAP_CONTAINS, key);
    }

    // wait
    @Override
    public Iterable<Map.Entry<UK, V>> entries() {
        return this::iterator;
    }

    @Override
    public Iterable<UK> keys() {
        return () -> {
            StateIterator<UK> stateIterator =
                    handleRequestSync(StateRequestType.MAP_ITER_KEY, null);
            return new SyncIteratorWrapper<>(stateIterator);
        };
    }

    @Override
    public Iterable<V> values() {
        return () -> {
            StateIterator<V> stateIterator =
                    handleRequestSync(StateRequestType.MAP_ITER_VALUE, null);
            return new SyncIteratorWrapper<>(stateIterator);
        };
    }

    @Override
    public Iterator<Map.Entry<UK, V>> iterator() {
        StateIterator<Map.Entry<UK, V>> stateIterator =
                handleRequestSync(StateRequestType.MAP_ITER, null);
        return new SyncIteratorWrapper<>(stateIterator);
    }

    @Override
    public boolean isEmpty() {
        return handleRequestSync(StateRequestType.MAP_IS_EMPTY, null);
    }
}
