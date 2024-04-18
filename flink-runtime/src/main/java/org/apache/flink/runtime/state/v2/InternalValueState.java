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

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

/**
 * A default implementation of {@link ValueState} which delegates all async requests to {@link
 * AsyncExecutionController}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
public class InternalValueState<K, V> extends InternalKeyedState<K, V> implements ValueState<V> {

    public InternalValueState(
            StateRequestHandler stateRequestHandler, ValueStateDescriptor<V> valueStateDescriptor) {
        super(stateRequestHandler, valueStateDescriptor);
    }

    @Override
    public final StateFuture<V> asyncValue() {
        return handleRequest(StateRequestType.VALUE_GET, null);
    }

    @Override
    public final StateFuture<Void> asyncUpdate(V value) {
        return handleRequest(StateRequestType.VALUE_UPDATE, value);
    }
}
