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

package org.apache.flink.state.api.input.source.broadcast;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.testing.CollectingSink;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/** Test for operator broadcast state source. */
public class BroadcastStateSourceTest {
    private static final MapStateDescriptor<Integer, Integer> STATE_DESCRIPTOR =
            new MapStateDescriptor<>("state", Types.INT, Types.INT);

    @Test
    public void testReadBroadcastState() throws Exception {
        try (TwoInputStreamOperatorTestHarness<Void, Integer, Void> testHarness =
                getTestHarness()) {
            testHarness.open();

            testHarness.processElement2(new StreamRecord<>(1));
            testHarness.processElement2(new StreamRecord<>(2));
            testHarness.processElement2(new StreamRecord<>(3));

            OperatorSubtaskState subtaskState = testHarness.snapshot(0, 0);
            OperatorState operatorState =
                    new OperatorState(null, null, OperatorIDGenerator.fromUid("uid"), 1, 4);
            operatorState.putState(0, subtaskState);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);

            BroadcastStateSource<Integer, Integer> source =
                    new BroadcastStateSource<>(
                            new HashMapStateBackend(),
                            operatorState,
                            new Configuration(),
                            new ExecutionConfig(),
                            STATE_DESCRIPTOR);

            DataStream<Tuple2<Integer, Integer>> stream =
                    env.fromSource(
                            source,
                            WatermarkStrategy.noWatermarks(),
                            "broadcast-state-source",
                            Types.TUPLE(Types.INT, Types.INT));

            CollectingSink<Tuple2<Integer, Integer>> sink = new CollectingSink<>();
            stream.sinkTo(sink);
            env.execute();

            Map<Integer, Integer> output =
                    sink.getRemainingOutput().stream()
                            .collect(Collectors.toMap(t -> t.f0, t -> t.f1));
            sink.close();

            Map<Integer, Integer> expected = new HashMap<>(3);
            expected.put(1, 1);
            expected.put(2, 2);
            expected.put(3, 3);

            Assert.assertEquals(
                    "Failed to read correct list state from state backend", expected, output);
        }
    }

    private TwoInputStreamOperatorTestHarness<Void, Integer, Void> getTestHarness()
            throws Exception {
        return new TwoInputStreamOperatorTestHarness<>(
                new CoBroadcastWithNonKeyedOperator<>(
                        new StatefulFunction(), Collections.singletonList(STATE_DESCRIPTOR)));
    }

    static class StatefulFunction extends BroadcastProcessFunction<Void, Integer, Void> {

        @Override
        public void processElement(Void value, ReadOnlyContext ctx, Collector<Void> out) {}

        @Override
        public void processBroadcastElement(Integer value, Context ctx, Collector<Void> out)
                throws Exception {
            ctx.getBroadcastState(STATE_DESCRIPTOR).put(value, value);
        }
    }
}
