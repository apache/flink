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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.checkpointing.utils.FailingSource;
import org.apache.flink.test.checkpointing.utils.IntType;
import org.apache.flink.test.checkpointing.utils.ValidatingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This verifies that checkpointing works correctly with event time windows.
 *
 * <p>This is a version of {@link EventTimeWindowCheckpointingITCase} for All-Windows.
 */
@SuppressWarnings("serial")
public class EventTimeAllWindowCheckpointingITCase extends TestLogger {

    private static final int PARALLELISM = 4;

    @ClassRule
    public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(PARALLELISM / 2)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
        config.set(AkkaOptions.LOOKUP_TIMEOUT_DURATION, Duration.ofMinutes(1));
        config.set(AkkaOptions.ASK_TIMEOUT_DURATION, Duration.ofMinutes(1));
        return config;
    }

    // ------------------------------------------------------------------------

    @Test
    public void testTumblingTimeWindow() {
        final int numElementsPerKey = 3000;
        final int windowSize = 100;
        final int numKeys = 1;

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

            env.addSource(
                            new FailingSource(
                                    new EventTimeWindowCheckpointingITCase.KeyedEventTimeGenerator(
                                            numKeys, windowSize),
                                    numElementsPerKey))
                    .rebalance()
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                    .apply(
                            new RichAllWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(Configuration parameters) {
                                    assertEquals(
                                            1, getRuntimeContext().getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> values,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    int sum = 0;
                                    long key = -1;

                                    for (Tuple2<Long, IntType> value : values) {
                                        sum += value.f1.value;
                                        key = value.f0;
                                    }
                                    out.collect(
                                            new Tuple4<>(
                                                    key,
                                                    window.getStart(),
                                                    window.getEnd(),
                                                    new IntType(sum)));
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorUpdateFun(
                                            numElementsPerKey),
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSize)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSlidingTimeWindow() {
        final int numElementsPerKey = 3000;
        final int windowSize = 1000;
        final int windowSlide = 100;
        final int numKeys = 1;

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

            env.addSource(
                            new FailingSource(
                                    new EventTimeWindowCheckpointingITCase.KeyedEventTimeGenerator(
                                            numKeys, windowSlide),
                                    numElementsPerKey))
                    .rebalance()
                    .windowAll(
                            SlidingEventTimeWindows.of(
                                    Time.milliseconds(windowSize), Time.milliseconds(windowSlide)))
                    .apply(
                            new RichAllWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(Configuration parameters) {
                                    assertEquals(
                                            1, getRuntimeContext().getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> values,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    int sum = 0;
                                    long key = -1;

                                    for (Tuple2<Long, IntType> value : values) {
                                        sum += value.f1.value;
                                        key = value.f0;
                                    }
                                    out.collect(
                                            new Tuple4<>(
                                                    key,
                                                    window.getStart(),
                                                    window.getEnd(),
                                                    new IntType(sum)));
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorUpdateFun(
                                            numElementsPerKey),
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSlide)))
                    .setParallelism(1);

            env.execute("Sliding Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPreAggregatedTumblingTimeWindow() {
        final int numElementsPerKey = 3000;
        final int windowSize = 100;
        final int numKeys = 1;

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

            env.addSource(
                            new FailingSource(
                                    new EventTimeWindowCheckpointingITCase.KeyedEventTimeGenerator(
                                            numKeys, windowSize),
                                    numElementsPerKey))
                    .rebalance()
                    .windowAll(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                    .reduce(
                            new ReduceFunction<Tuple2<Long, IntType>>() {

                                @Override
                                public Tuple2<Long, IntType> reduce(
                                        Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {

                                    return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
                                }
                            },
                            new RichAllWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(Configuration parameters) {
                                    assertEquals(
                                            1, getRuntimeContext().getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> input,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    for (Tuple2<Long, IntType> in : input) {
                                        out.collect(
                                                new Tuple4<>(
                                                        in.f0,
                                                        window.getStart(),
                                                        window.getEnd(),
                                                        in.f1));
                                    }
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorUpdateFun(
                                            numElementsPerKey),
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSize)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPreAggregatedSlidingTimeWindow() {
        final int numElementsPerKey = 3000;
        final int windowSize = 1000;
        final int windowSlide = 100;
        final int numKeys = 1;

        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(PARALLELISM);
            env.enableCheckpointing(100);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

            env.addSource(
                            new FailingSource(
                                    new EventTimeWindowCheckpointingITCase.KeyedEventTimeGenerator(
                                            numKeys, windowSlide),
                                    numElementsPerKey))
                    .rebalance()
                    .windowAll(
                            SlidingEventTimeWindows.of(
                                    Time.milliseconds(windowSize), Time.milliseconds(windowSlide)))
                    .reduce(
                            new ReduceFunction<Tuple2<Long, IntType>>() {

                                @Override
                                public Tuple2<Long, IntType> reduce(
                                        Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {

                                    return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
                                }
                            },
                            new RichAllWindowFunction<
                                    Tuple2<Long, IntType>,
                                    Tuple4<Long, Long, Long, IntType>,
                                    TimeWindow>() {

                                private boolean open = false;

                                @Override
                                public void open(Configuration parameters) {
                                    assertEquals(
                                            1, getRuntimeContext().getNumberOfParallelSubtasks());
                                    open = true;
                                }

                                @Override
                                public void apply(
                                        TimeWindow window,
                                        Iterable<Tuple2<Long, IntType>> input,
                                        Collector<Tuple4<Long, Long, Long, IntType>> out) {

                                    // validate that the function has been opened properly
                                    assertTrue(open);

                                    for (Tuple2<Long, IntType> in : input) {
                                        out.collect(
                                                new Tuple4<>(
                                                        in.f0,
                                                        window.getStart(),
                                                        window.getEnd(),
                                                        in.f1));
                                    }
                                }
                            })
                    .addSink(
                            new ValidatingSink<>(
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorUpdateFun(
                                            numElementsPerKey),
                                    new EventTimeWindowCheckpointingITCase.SinkValidatorCheckFun(
                                            numKeys, numElementsPerKey, windowSlide)))
                    .setParallelism(1);

            env.execute("Tumbling Window Test");
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
