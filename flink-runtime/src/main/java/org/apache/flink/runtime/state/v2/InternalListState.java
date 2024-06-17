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

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

import java.util.List;

/**
 * A default implementation of {@link ListState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
public class InternalListState<K, V> extends InternalKeyedState<K, V> implements ListState<V> {

    public InternalListState(
            StateRequestHandler stateRequestHandler, ListStateDescriptor<V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
    }

    @Override
    public StateFuture<StateIterator<V>> asyncGet() {
        return handleRequest(StateRequestType.LIST_GET, null);
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        return handleRequest(StateRequestType.LIST_ADD, value);
    }

    @Override
    public StateFuture<Void> asyncUpdate(List<V> values) {
        return handleRequest(StateRequestType.LIST_UPDATE, values);
    }

    @Override
    public StateFuture<Void> asyncAddAll(List<V> values) {
        return handleRequest(StateRequestType.LIST_ADD_ALL, values);
    }
}
