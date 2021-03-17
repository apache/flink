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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;

/** In memory mock internal aggregating state. */
class MockInternalAggregatingState<K, N, IN, ACC, OUT>
        extends MockInternalMergingState<K, N, IN, ACC, OUT>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {
    private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    private MockInternalAggregatingState(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
        this.aggregateFunction = aggregateFunction;
    }

    @Override
    public OUT get() {
        return aggregateFunction.getResult(getInternal());
    }

    @Override
    public void add(IN value) {
        updateInternal(aggregateFunction.add(value, getInternal()));
    }

    @Override
    ACC mergeState(ACC acc, ACC nAcc) {
        return aggregateFunction.merge(acc, nAcc);
    }

    @SuppressWarnings({"unchecked", "unused"})
    static <IN, OUT, N, ACC, S extends State, IS extends S> IS createState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, ACC> stateDesc) {
        AggregatingStateDescriptor<IN, ACC, OUT> aggregatingStateDesc =
                (AggregatingStateDescriptor<IN, ACC, OUT>) stateDesc;
        return (IS) new MockInternalAggregatingState<>(aggregatingStateDesc.getAggregateFunction());
    }
}
