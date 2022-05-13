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

package org.apache.flink.state.api.output;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.output.operators.KeyedStateBootstrapOperator;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test writing keyed bootstrap state. */
public class KeyedStateBootstrapOperatorTest {

    private static final ValueStateDescriptor<Long> descriptor =
            new ValueStateDescriptor<>("state", Types.LONG);

    private static final Long EVENT_TIMER = Long.MAX_VALUE - 1;

    private static final Long PROC_TIMER = Long.MAX_VALUE - 2;

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testTimerStateRestorable() throws Exception {
        Path path = new Path(folder.newFolder().toURI());

        OperatorSubtaskState state;
        KeyedStateBootstrapOperator<Long, Long> bootstrapOperator =
                new KeyedStateBootstrapOperator<>(0L, path, new TimerBootstrapFunction());
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, TaggedOperatorSubtaskState>
                harness = getHarness(bootstrapOperator)) {
            processElements(harness, 1L, 2L, 3L);
            state = getState(bootstrapOperator, harness);
        }

        KeyedProcessOperator<Long, Long, Tuple3<Long, Long, TimeDomain>> procOperator =
                new KeyedProcessOperator<>(new SimpleProcessFunction());
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Tuple3<Long, Long, TimeDomain>>
                harness = getHarness(procOperator, state)) {
            harness.processWatermark(EVENT_TIMER);
            harness.setProcessingTime(PROC_TIMER);

            assertHarnessOutput(
                    harness,
                    Tuple3.of(1L, EVENT_TIMER, TimeDomain.EVENT_TIME),
                    Tuple3.of(2L, EVENT_TIMER, TimeDomain.EVENT_TIME),
                    Tuple3.of(3L, EVENT_TIMER, TimeDomain.EVENT_TIME),
                    Tuple3.of(1L, PROC_TIMER, TimeDomain.PROCESSING_TIME),
                    Tuple3.of(2L, PROC_TIMER, TimeDomain.PROCESSING_TIME),
                    Tuple3.of(3L, PROC_TIMER, TimeDomain.PROCESSING_TIME));
        }
    }

    @Test
    public void testNonTimerStatesRestorableByNonProcessesOperator() throws Exception {
        Path path = new Path(folder.newFolder().toURI());

        OperatorSubtaskState state;
        KeyedStateBootstrapOperator<Long, Long> bootstrapOperator =
                new KeyedStateBootstrapOperator<>(0L, path, new SimpleBootstrapFunction());
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, TaggedOperatorSubtaskState>
                harness = getHarness(bootstrapOperator)) {
            processElements(harness, 1L, 2L, 3L);
            state = getState(bootstrapOperator, harness);
        }

        StreamMap<Long, Long> mapOperator = new StreamMap<>(new StreamingFunction());
        try (KeyedOneInputStreamOperatorTestHarness<Long, Long, Long> harness =
                getHarness(mapOperator, state)) {
            processElements(harness, 1L, 2L, 3L);

            assertHarnessOutput(harness, 1L, 2L, 3L);
            harness.snapshot(0L, 0L);
        }
    }

    private <T> KeyedOneInputStreamOperatorTestHarness<Long, Long, T> getHarness(
            OneInputStreamOperator<Long, T> bootstrapOperator) throws Exception {
        return getHarness(bootstrapOperator, null);
    }

    private <T> KeyedOneInputStreamOperatorTestHarness<Long, Long, T> getHarness(
            OneInputStreamOperator<Long, T> bootstrapOperator, OperatorSubtaskState state)
            throws Exception {
        KeyedOneInputStreamOperatorTestHarness<Long, Long, T> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        bootstrapOperator, id -> id, Types.LONG, 128, 1, 0);

        harness.setStateBackend(new RocksDBStateBackend(folder.newFolder().toURI()));
        if (state != null) {
            harness.initializeState(state);
        }
        harness.open();
        return harness;
    }

    private <T> void processElements(
            KeyedOneInputStreamOperatorTestHarness<Long, Long, T> harness, Long... records)
            throws Exception {
        for (Long record : records) {
            harness.processElement(record, 0);
        }
    }

    private OperatorSubtaskState getState(
            KeyedStateBootstrapOperator<Long, Long> bootstrapOperator,
            KeyedOneInputStreamOperatorTestHarness<Long, Long, TaggedOperatorSubtaskState> harness)
            throws Exception {
        OperatorSubtaskState state;
        bootstrapOperator.endInput();
        state = harness.extractOutputValues().get(0).state;
        return state;
    }

    private <T> void assertHarnessOutput(
            KeyedOneInputStreamOperatorTestHarness<Long, Long, T> harness, T... output) {
        Assert.assertThat(
                "The output from the operator does not match the expected values",
                harness.extractOutputValues(),
                Matchers.containsInAnyOrder(output));
    }

    private static class TimerBootstrapFunction extends KeyedStateBootstrapFunction<Long, Long> {

        @Override
        public void processElement(Long value, Context ctx) throws Exception {
            ctx.timerService().registerEventTimeTimer(EVENT_TIMER);
            ctx.timerService().registerProcessingTimeTimer(PROC_TIMER);
        }
    }

    private static class SimpleProcessFunction
            extends KeyedProcessFunction<Long, Long, Tuple3<Long, Long, TimeDomain>> {

        @Override
        public void processElement(
                Long value, Context ctx, Collector<Tuple3<Long, Long, TimeDomain>> out)
                throws Exception {}

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Long, TimeDomain>> out)
                throws Exception {
            out.collect(Tuple3.of(ctx.getCurrentKey(), timestamp, ctx.timeDomain()));
        }
    }

    private static class SimpleBootstrapFunction extends KeyedStateBootstrapFunction<Long, Long> {

        private ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Long value, Context ctx) throws Exception {
            state.update(value);
        }
    }

    private static class StreamingFunction extends RichMapFunction<Long, Long> {

        private ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Long map(Long value) throws Exception {
            return state.value();
        }
    }
}
