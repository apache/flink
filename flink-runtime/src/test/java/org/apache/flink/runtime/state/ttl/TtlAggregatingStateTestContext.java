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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test suite for {@link TtlAggregatingState}. */
class TtlAggregatingStateTestContext
        extends TtlMergingStateTestContext.TtlIntegerMergingStateTestContext<
                TtlAggregatingState<?, String, Integer, Long, String>, Integer, String> {
    private static final long DEFAULT_ACCUMULATOR = 3L;

    @Override
    void initTestValues() {
        updateEmpty = 5;
        updateUnexpired = 7;
        updateExpired = 6;

        getUpdateEmpty = "8";
        getUnexpired = "15";
        getUpdateExpired = "9";
    }

    @SuppressWarnings("unchecked")
    @Override
    public <US extends State, SV> StateDescriptor<US, SV> createStateDescriptor() {
        return (StateDescriptor<US, SV>)
                new AggregatingStateDescriptor<>(getName(), AGGREGATE, LongSerializer.INSTANCE);
    }

    @Override
    public void update(Integer value) throws Exception {
        ttlState.add(value);
    }

    @Override
    public String get() throws Exception {
        return ttlState.get();
    }

    @Override
    public Object getOriginal() throws Exception {
        return ttlState.original.get();
    }

    @Override
    String getMergeResult(
            List<Tuple2<String, Integer>> unexpiredUpdatesToMerge,
            List<Tuple2<String, Integer>> finalUpdatesToMerge) {
        Set<String> namespaces = new HashSet<>();
        unexpiredUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
        finalUpdatesToMerge.forEach(t -> namespaces.add(t.f0));
        return Integer.toString(
                getIntegerMergeResult(unexpiredUpdatesToMerge, finalUpdatesToMerge)
                        + namespaces.size() * (int) DEFAULT_ACCUMULATOR);
    }

    private static final AggregateFunction<Integer, Long, String> AGGREGATE =
            new AggregateFunction<Integer, Long, String>() {
                private static final long serialVersionUID = 815663074737539631L;

                @Override
                public Long createAccumulator() {
                    return DEFAULT_ACCUMULATOR;
                }

                @Override
                public Long add(Integer value, Long accumulator) {
                    return accumulator + value;
                }

                @Override
                public String getResult(Long accumulator) {
                    return accumulator.toString();
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            };
}
