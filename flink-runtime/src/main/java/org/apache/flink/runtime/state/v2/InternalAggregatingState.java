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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;

/**
 * The default implementation of {@link AggregatingState}, which delegates all async requests to
 * {@link StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <IN> The type of the values that are added into the state.
 * @param <ACC> TThe type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
public class InternalAggregatingState<K, IN, ACC, OUT> extends InternalKeyedState<K, ACC>
        implements AggregatingState<IN, OUT> {

    protected final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    /**
     * Creates a new InternalKeyedState with the given asyncExecutionController and stateDescriptor.
     *
     * @param stateRequestHandler The async request handler for handling all requests.
     * @param stateDescriptor The properties of the state.
     */
    public InternalAggregatingState(
            StateRequestHandler stateRequestHandler,
            AggregatingStateDescriptor<IN, ACC, OUT> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
        this.aggregateFunction = stateDescriptor.getAggregateFunction();
    }

    @Override
    public StateFuture<OUT> asyncGet() {
        return handleRequest(StateRequestType.AGGREGATING_GET, null);
    }

    @Override
    public StateFuture<Void> asyncAdd(IN value) {
        return handleRequest(StateRequestType.AGGREGATING_ADD, value);
    }
}
