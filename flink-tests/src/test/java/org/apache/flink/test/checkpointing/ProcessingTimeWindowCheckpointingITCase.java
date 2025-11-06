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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.checkpointing.utils.FailingSource;
import org.apache.flink.test.checkpointing.utils.IntType;
import org.apache.flink.test.checkpointing.utils.ValidatingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test uses a custom non-serializable data type to ensure that state serializability is
 * handled correctly.
 */
@SuppressWarnings("serial")
public class ProcessingTimeWindowCheckpointingITCase extends TestLogger {

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
        return config;
    }

    // ------------------------------------------------------------------------

    @Test
    public void testTumblingProcessingTimeWindow() throws Exception {
        final int numElements = 3000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().setAutoWatermarkInterval(10);
        env.enableCheckpointing(100);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);

        SinkValidatorUpdaterAndChecker updaterAndChecker =
                new SinkValidatorUpdaterAndChecker(numElements, 1);

        env.addSource(new FailingSource(new Generator(), numElements, true))
                .rebalance()
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(100)))
                .apply(
                        new RichWindowFunction<
                                Tuple2<Long, IntType>, Tuple2<Long, IntType>, Long, TimeWindow>() {

                            private boolean open = false;

                            @Override
                            public void open(OpenContext openContext) {
                                assertEquals(
                                        PARALLELISM,
                                        getRuntimeContext()
                                                .getTaskInfo()
                                                .getNumberOfParallelSubtasks());
                                open = true;
                            }

                            @Override
                            public void apply(
                                    Long l,
                                    TimeWindow window,
                                    Iterable<Tuple2<Long, IntType>> values,
                                    Collector<Tuple2<Long, IntType>> out) {

                                // validate that the function has been opened properly
                                assertTrue(open);

                                for (Tuple2<Long, IntType> value : values) {
                                    assertEquals(value.f0.intValue(), value.f1.value);
                                    out.collect(new Tuple2<>(value.f0, new IntType(1)));
                                }
                            }
                        })
                .addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, true))
                .setParallelism(1);

        tryExecute(env, "Tumbling Window Test");
    }

    @Test
    public void testSlidingProcessingTimeWindow() throws Exception {
        final int numElements = 3000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().setAutoWatermarkInterval(10);
        env.enableCheckpointing(100);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
        SinkValidatorUpdaterAndChecker updaterAndChecker =
                new SinkValidatorUpdaterAndChecker(numElements, 3);
        env.addSource(new FailingSource(new Generator(), numElements, true))
                .rebalance()
                .keyBy(x -> x.f0)
                .window(
                        SlidingProcessingTimeWindows.of(
                                Duration.ofMillis(150), Duration.ofMillis(50)))
                .apply(
                        new RichWindowFunction<
                                Tuple2<Long, IntType>, Tuple2<Long, IntType>, Long, TimeWindow>() {

                            private boolean open = false;

                            @Override
                            public void open(OpenContext openContext) {
                                assertEquals(
                                        PARALLELISM,
                                        getRuntimeContext()
                                                .getTaskInfo()
                                                .getNumberOfParallelSubtasks());
                                open = true;
                            }

                            @Override
                            public void apply(
                                    Long l,
                                    TimeWindow window,
                                    Iterable<Tuple2<Long, IntType>> values,
                                    Collector<Tuple2<Long, IntType>> out) {

                                // validate that the function has been opened properly
                                assertTrue(open);

                                for (Tuple2<Long, IntType> value : values) {
                                    assertEquals(value.f0.intValue(), value.f1.value);
                                    out.collect(new Tuple2<>(value.f0, new IntType(1)));
                                }
                            }
                        })
                .addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, true))
                .setParallelism(1);

        tryExecute(env, "Sliding Window Test");
    }

    @Test
    public void testAggregatingTumblingProcessingTimeWindow() throws Exception {
        final int numElements = 3000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().setAutoWatermarkInterval(10);
        env.enableCheckpointing(100);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
        SinkValidatorUpdaterAndChecker updaterAndChecker =
                new SinkValidatorUpdaterAndChecker(numElements, 1);
        env.addSource(new FailingSource(new Generator(), numElements, true))
                .map(
                        new MapFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>>() {
                            @Override
                            public Tuple2<Long, IntType> map(Tuple2<Long, IntType> value) {
                                value.f1.value = 1;
                                return value;
                            }
                        })
                .rebalance()
                .keyBy(x -> x.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMillis(100)))
                .reduce(
                        new ReduceFunction<Tuple2<Long, IntType>>() {

                            @Override
                            public Tuple2<Long, IntType> reduce(
                                    Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {
                                return new Tuple2<>(a.f0, new IntType(1));
                            }
                        })
                .addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, true))
                .setParallelism(1);

        tryExecute(env, "Aggregating Tumbling Window Test");
    }

    @Test
    public void testAggregatingSlidingProcessingTimeWindow() throws Exception {
        final int numElements = 3000;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.getConfig().setAutoWatermarkInterval(10);
        env.enableCheckpointing(100);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);
        SinkValidatorUpdaterAndChecker updaterAndChecker =
                new SinkValidatorUpdaterAndChecker(numElements, 3);
        env.addSource(new FailingSource(new Generator(), numElements, true))
                .map(
                        new MapFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>>() {
                            @Override
                            public Tuple2<Long, IntType> map(Tuple2<Long, IntType> value) {
                                value.f1.value = 1;
                                return value;
                            }
                        })
                .rebalance()
                .keyBy(x -> x.f0)
                .window(
                        SlidingProcessingTimeWindows.of(
                                Duration.ofMillis(150), Duration.ofMillis(50)))
                .reduce(
                        new ReduceFunction<Tuple2<Long, IntType>>() {
                            @Override
                            public Tuple2<Long, IntType> reduce(
                                    Tuple2<Long, IntType> a, Tuple2<Long, IntType> b) {
                                return new Tuple2<>(a.f0, new IntType(1));
                            }
                        })
                .addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, true))
                .setParallelism(1);

        tryExecute(env, "Aggregating Sliding Window Test");
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    static class Generator implements FailingSource.EventEmittingGenerator {

        @Override
        public void emitEvent(
                SourceFunction.SourceContext<Tuple2<Long, IntType>> ctx, int eventSequenceNo) {
            ctx.collect(new Tuple2<>((long) eventSequenceNo, new IntType(eventSequenceNo)));
        }
    }

    static class SinkValidatorUpdaterAndChecker
            implements ValidatingSink.CountUpdater<Tuple2<Long, IntType>>,
                    ValidatingSink.ResultChecker {

        private final int elementCountExpected;
        private final int countPerElementExpected;

        SinkValidatorUpdaterAndChecker(int elementCountExpected, int countPerElementExpected) {
            this.elementCountExpected = elementCountExpected;
            this.countPerElementExpected = countPerElementExpected;
        }

        @Override
        public void updateCount(Tuple2<Long, IntType> value, Map<Long, Integer> windowCounts) {
            windowCounts.merge(value.f0, value.f1.value, (a, b) -> a + b);
        }

        @Override
        public boolean checkResult(Map<Long, Integer> windowCounts) {
            int aggCount = 0;

            for (Integer i : windowCounts.values()) {
                aggCount += i;
            }

            if (aggCount < elementCountExpected * countPerElementExpected
                    || elementCountExpected != windowCounts.size()) {
                return false;
            }

            for (Map.Entry<Long, Integer> e : windowCounts.entrySet()) {
                if (e.getValue() < countPerElementExpected) {
                    return false;
                } else if (e.getValue() > countPerElementExpected) {
                    fail(
                            String.format(
                                    "counter too big for %d: %d (expected %d)",
                                    e.getKey(), e.getValue(), countPerElementExpected));
                }
            }

            return true;
        }
    }
}
