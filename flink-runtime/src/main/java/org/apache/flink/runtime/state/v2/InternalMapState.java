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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.Map;

/**
 * A default implementation of {@link MapState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of partitioned key the state is associated to.
 * @param <UK> The type of user key of this state.
 * @param <V> The type of values kept internally in state.
 */
public class InternalMapState<K, UK, V> extends InternalKeyedState<K, V>
        implements MapState<UK, V> {

    public InternalMapState(
            StateRequestHandler stateRequestHandler, MapStateDescriptor<UK, V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
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
}
