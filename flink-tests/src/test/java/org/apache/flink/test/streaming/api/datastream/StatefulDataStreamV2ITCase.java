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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDeclaration;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Integration test for state access and usage of DataStream V2. */
class StatefulDataStreamV2ITCase {

    private KeyedPartitionStream<Long, Long> keyedPartitionStream;
    private ExecutionEnvironment env;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        DefaultKeySelector defaultKeySelector = new DefaultKeySelector();
        env = ExecutionEnvironment.getInstance();
        keyedPartitionStream =
                env.fromSource(
                                DataStreamV2SourceUtils.fromData(Arrays.asList(1L, 1L, 1L)),
                                "test-source")
                        .keyBy(defaultKeySelector);
    }

    @Test
    void testValueState() throws Exception {
        MockSumProcessFunction processFunction = new MockSumProcessFunction();
        MockVerifierFunction verifierFunction =
                new MockVerifierFunction(Arrays.asList("1", "2", "3"));
        keyedPartitionStream.process(processFunction).global().process(verifierFunction);
        env.execute("dsV2 job");
    }

    @Test
    void testListState() throws Exception {
        MockListCountProcessFunction processFunction = new MockListCountProcessFunction();
        MockVerifierFunction verifierFunction =
                new MockVerifierFunction(Arrays.asList("1", "1,1", "1,1,1"));
        keyedPartitionStream.process(processFunction).global().process(verifierFunction);
        env.execute("dsV2 job");
    }

    @Test
    void testMapState() throws Exception {
        MockCountMapProcessFunction processFunction = new MockCountMapProcessFunction();
        MockVerifierFunction verifierFunction =
                new MockVerifierFunction(Arrays.asList("1", "2", "3"));
        keyedPartitionStream.process(processFunction).global().process(verifierFunction);
        env.execute("dsV2 job");
    }

    @Test
    void testReducingState() throws Exception {
        MockReducingSumProcessFunction processFunction = new MockReducingSumProcessFunction();
        MockVerifierFunction verifierFunction =
                new MockVerifierFunction(Arrays.asList("1", "2", "3"));
        keyedPartitionStream.process(processFunction).global().process(verifierFunction);
        env.execute("dsV2 job");
    }

    @Test
    void testAggregatingState() throws Exception {
        MockAggregateSumProcessFunction processFunction = new MockAggregateSumProcessFunction();
        MockVerifierFunction verifierFunction =
                new MockVerifierFunction(Arrays.asList("1", "2", "3"));
        keyedPartitionStream.process(processFunction).global().process(verifierFunction);
        env.execute("dsV2 job");
    }

    private static class DefaultKeySelector implements KeySelector<Long, Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long getKey(Long value) throws Exception {
            return value;
        }
    }

    /** {@link AggregateFunction} that sums values. */
    private static class MockAggregateSumFunction implements AggregateFunction<Long, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Long value, Long accumulator) {
            return value + accumulator;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    /** {@link ReduceFunction} that sums values. */
    private static class MockReduceSumFunction implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    /** {@link OneInputStreamProcessFunction} that sums records and outputs the sum. */
    private static class MockAggregateSumProcessFunction
            implements OneInputStreamProcessFunction<Long, String> {

        private final AggregatingStateDeclaration<Long, Long, Long> stateDeclaration;

        public MockAggregateSumProcessFunction() {
            this.stateDeclaration =
                    StateDeclarations.aggregatingState(
                            "reducing-state", TypeDescriptors.LONG, new MockAggregateSumFunction());
        }

        @Override
        public void processRecord(Long record, Collector<String> output, PartitionedContext ctx)
                throws Exception {
            Optional<AggregatingState<Long, Long>> maybeState =
                    ctx.getStateManager().getState(stateDeclaration);
            if (!maybeState.isPresent()) {
                throw new FlinkRuntimeException("State not found: " + stateDeclaration);
            }
            maybeState.get().add(record);
            output.collect(Long.toString(maybeState.get().get()));
        }
    }

    /** {@link OneInputStreamProcessFunction} that sums records and outputs the sum. */
    private static class MockReducingSumProcessFunction
            implements OneInputStreamProcessFunction<Long, String> {

        private final ReducingStateDeclaration<Long> stateDeclaration;

        public MockReducingSumProcessFunction() {
            this.stateDeclaration =
                    StateDeclarations.reducingState(
                            "reducing-state", TypeDescriptors.LONG, new MockReduceSumFunction());
        }

        @Override
        public void processRecord(Long record, Collector<String> output, PartitionedContext ctx)
                throws Exception {
            Optional<ReducingState<Long>> maybeState =
                    ctx.getStateManager().getState(stateDeclaration);
            if (!maybeState.isPresent()) {
                throw new FlinkRuntimeException("State not found: " + stateDeclaration);
            }
            maybeState.get().add(record);
            output.collect(Long.toString(maybeState.get().get()));
        }
    }

    /**
     * {@link OneInputStreamProcessFunction} that counts the occurrence of each record and outputs
     * the recent occurrence.
     */
    private static class MockCountMapProcessFunction
            implements OneInputStreamProcessFunction<Long, String> {

        private final MapStateDeclaration<Long, Long> stateDeclaration;

        public MockCountMapProcessFunction() {
            this.stateDeclaration =
                    StateDeclarations.mapState(
                            "map-state", TypeDescriptors.LONG, TypeDescriptors.LONG);
        }

        @Override
        public void processRecord(Long record, Collector<String> output, PartitionedContext ctx)
                throws Exception {
            Optional<MapState<Long, Long>> maybeState =
                    ctx.getStateManager().getState(stateDeclaration);
            if (!maybeState.isPresent()) {
                throw new FlinkRuntimeException("State not found: " + stateDeclaration);
            }
            Long curOccurence = maybeState.get().get(record);
            curOccurence = curOccurence == null ? 1L : curOccurence + 1L;
            maybeState.get().put(record, curOccurence);
            output.collect(Long.toString(curOccurence));
        }
    }

    /**
     * {@link OneInputStreamProcessFunction} that appends every record to a list and outputs the
     * appended result.
     */
    private static class MockListCountProcessFunction
            implements OneInputStreamProcessFunction<Long, String> {

        private final ListStateDeclaration<Long> stateDeclaration;

        public MockListCountProcessFunction() {
            this.stateDeclaration = StateDeclarations.listState("list-state", TypeDescriptors.LONG);
        }

        @Override
        public void processRecord(Long record, Collector<String> output, PartitionedContext ctx)
                throws Exception {
            Optional<ListState<Long>> maybeState = ctx.getStateManager().getState(stateDeclaration);
            if (!maybeState.isPresent()) {
                throw new FlinkRuntimeException("State not found: " + stateDeclaration);
            }
            ListState<Long> currentValue = maybeState.get();
            currentValue.add(record);
            StringBuilder stringBuilder = new StringBuilder();
            for (Long val : currentValue.get()) {
                stringBuilder.append(val);
                stringBuilder.append(",");
            }
            if (stringBuilder.length() > 0) {
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            }
            output.collect(stringBuilder.toString());
        }
    }

    /** {@link OneInputStreamProcessFunction} that sums records and outputs the sum. */
    private static class MockSumProcessFunction
            implements OneInputStreamProcessFunction<Long, String> {

        private final ValueStateDeclaration<Long> stateDeclaration;

        public MockSumProcessFunction() {
            this.stateDeclaration =
                    StateDeclarations.valueState("value-state", TypeDescriptors.LONG);
        }

        @Override
        public void processRecord(Long record, Collector<String> output, PartitionedContext ctx)
                throws Exception {
            Optional<ValueState<Long>> maybeState =
                    ctx.getStateManager().getState(stateDeclaration);
            if (!maybeState.isPresent()) {
                throw new FlinkRuntimeException("State not found: " + stateDeclaration);
            }
            Long currentValue = maybeState.get().value();
            currentValue = currentValue == null ? 0 : currentValue;
            maybeState.get().update(currentValue + record);
            output.collect(Long.toString(maybeState.get().value()));
        }
    }

    /**
     * {@link OneInputStreamProcessFunction} that verifies the result. If verification fails, it
     * throws an exception.
     */
    private static class MockVerifierFunction
            implements OneInputStreamProcessFunction<String, Object> {

        private final List<Object> allValues;

        public MockVerifierFunction(List<Object> allValues) {
            // copying list values to ensure that allValues is not restricted to the fixed-size list
            this.allValues = new ArrayList<>(allValues);
        }

        @Override
        public void processRecord(String record, Collector<Object> output, PartitionedContext ctx)
                throws Exception {
            if (!allValues.contains(record)) {
                throw new FlinkRuntimeException("Record not found: " + record);
            }
            allValues.remove(record);
        }
    }
}
