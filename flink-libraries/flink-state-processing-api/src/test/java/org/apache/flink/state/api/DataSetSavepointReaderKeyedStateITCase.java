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

package org.apache.flink.state.api;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.utils.SavepointTestBase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** IT case for reading state. */
public abstract class DataSetSavepointReaderKeyedStateITCase<B extends StateBackend>
        extends SavepointTestBase {
    private static final String uid = "stateful-operator";

    private static ValueStateDescriptor<Integer> valueState =
            new ValueStateDescriptor<>("value", Types.INT);

    private static final List<Pojo> elements =
            Arrays.asList(Pojo.of(1, 1), Pojo.of(2, 2), Pojo.of(3, 3));

    protected abstract B getStateBackend();

    @Test
    public void testUserKeyedStateReader() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(getStateBackend());
        env.setParallelism(4);

        env.addSource(createSource(elements))
                .returns(Pojo.class)
                .rebalance()
                .keyBy(id -> id.key)
                .process(new KeyedStatefulOperator())
                .uid(uid)
                .sinkTo(new DiscardingSink<>());

        String savepointPath = takeSavepoint(env);

        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(batchEnv, savepointPath, getStateBackend());

        List<Pojo> results = savepoint.readKeyedState(uid, new Reader()).collect();

        Set<Pojo> expected = new HashSet<>(elements);

        Assert.assertEquals(
                "Unexpected results from keyed state", expected, new HashSet<>(results));
    }

    private static class KeyedStatefulOperator extends KeyedProcessFunction<Integer, Pojo, Void> {
        private transient ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(valueState);
        }

        @Override
        public void processElement(Pojo value, Context ctx, Collector<Void> out) throws Exception {
            state.update(value.state);

            value.eventTimeTimer.forEach(timer -> ctx.timerService().registerEventTimeTimer(timer));
            value.processingTimeTimer.forEach(
                    timer -> ctx.timerService().registerProcessingTimeTimer(timer));
        }
    }

    private static class Reader extends KeyedStateReaderFunction<Integer, Pojo> {

        private transient ValueState<Integer> state;

        @Override
        public void open(OpenContext openContext) {
            state = getRuntimeContext().getState(valueState);
        }

        @Override
        public void open(Configuration parameters) {
            throw new UnsupportedOperationException(
                    "This method is deprecated and shouldn't be invoked. Please use open(OpenContext) instead.");
        }

        @Override
        public void readKey(Integer key, Context ctx, Collector<Pojo> out) throws Exception {
            Pojo pojo = new Pojo();
            pojo.key = key;
            pojo.state = state.value();
            pojo.eventTimeTimer = ctx.registeredEventTimeTimers();
            pojo.processingTimeTimer = ctx.registeredProcessingTimeTimers();

            out.collect(pojo);
        }
    }

    /** A simple pojo type. */
    public static class Pojo {
        public static Pojo of(Integer key, Integer state) {
            Pojo wrapper = new Pojo();
            wrapper.key = key;
            wrapper.state = state;
            wrapper.eventTimeTimer = Collections.singleton(Long.MAX_VALUE - 1);
            wrapper.processingTimeTimer = Collections.singleton(Long.MAX_VALUE - 2);

            return wrapper;
        }

        Integer key;

        Integer state;

        Set<Long> eventTimeTimer;

        Set<Long> processingTimeTimer;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Pojo pojo = (Pojo) o;
            return Objects.equals(key, pojo.key)
                    && Objects.equals(state, pojo.state)
                    && Objects.equals(eventTimeTimer, pojo.eventTimeTimer)
                    && Objects.equals(processingTimeTimer, pojo.processingTimeTimer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, state, eventTimeTimer, processingTimeTimer);
        }
    }
}
