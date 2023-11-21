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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL_DURING_BACKLOG;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for streaming job with backlog. */
public class StreamingWithBacklogITCase {
    @Test
    public void testKeyedAggregationWithBacklog() throws Exception {
        final Configuration config = new Configuration();
        config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        List<Tuple2<Integer, Long>> expectedRes = new ArrayList<>();
        for (int i = 1; i <= 10000; ++i) {
            expectedRes.add(Tuple2.of(0, (long) i));
            expectedRes.add(Tuple2.of(1, (long) i));
        }

        assertThat(runKeyedAggregation(env)).containsExactlyInAnyOrderElementsOf(expectedRes);
    }

    @Test
    public void testKeyedAggregationWithBacklogParallelismOne() throws Exception {
        final Configuration config = new Configuration();
        config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        List<Tuple2<Integer, Long>> backlogExpectedRes = new ArrayList<>();
        List<Tuple2<Integer, Long>> realTimeExpectedRes = new ArrayList<>();
        for (int i = 1; i <= 5000; ++i) {
            backlogExpectedRes.add(Tuple2.of(0, (long) i));
        }
        for (int i = 1; i <= 5000; ++i) {
            backlogExpectedRes.add(Tuple2.of(1, (long) i));
        }
        for (int i = 5001; i <= 10000; ++i) {
            realTimeExpectedRes.add(Tuple2.of(0, (long) i));
            realTimeExpectedRes.add(Tuple2.of(1, (long) i));
        }
        final List<Tuple2<Integer, Long>> result = runKeyedAggregation(env);
        assertThat(getKeySwitchCnt(result.subList(0, 10000), x -> x.f0)).isEqualTo(1);
        assertThat(result.subList(0, 10000))
                .containsExactlyInAnyOrderElementsOf(backlogExpectedRes);
        assertThat(result.subList(10000, 20000))
                .containsExactlyInAnyOrderElementsOf(realTimeExpectedRes);
    }

    @Test
    public void testKeyedWindowedAggregationWithBacklog() throws Exception {
        final Configuration config = new Configuration();
        config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);

        List<Tuple3<Integer, Long, Long>> expectedRes = new ArrayList<>();
        for (int i = 1; i <= 5000; ++i) {
            expectedRes.add(Tuple3.of(0, (long) i * 4 * 1000, 2L));
            expectedRes.add(Tuple3.of(1, (long) i * 4 * 1000, 2L));
        }

        assertThat(runKeyedWindowedAggregation(env))
                .containsExactlyInAnyOrderElementsOf(expectedRes);
    }

    @Test
    public void testKeyedWindowedAggregationWithBacklogParallelismOne() throws Exception {
        final Configuration config = new Configuration();
        config.set(CHECKPOINTING_INTERVAL_DURING_BACKLOG, Duration.ZERO);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        List<Tuple3<Integer, Long, Long>> backlogExpectedRes = new ArrayList<>();
        List<Tuple3<Integer, Long, Long>> realTimeExpectedRes = new ArrayList<>();

        for (int i = 1; i <= 2499; ++i) {
            backlogExpectedRes.add(Tuple3.of(0, (long) i * 4 * 1000, 2L));
        }
        for (int i = 1; i <= 2499; ++i) {
            backlogExpectedRes.add(Tuple3.of(1, (long) i * 4 * 1000, 2L));
        }
        for (int i = 2500; i <= 5000; ++i) {
            realTimeExpectedRes.add(Tuple3.of(0, (long) i * 4 * 1000, 2L));
            realTimeExpectedRes.add(Tuple3.of(1, (long) i * 4 * 1000, 2L));
        }

        final List<Tuple3<Integer, Long, Long>> result = runKeyedWindowedAggregation(env);
        assertThat(getKeySwitchCnt(result.subList(0, 4998), x -> x.f0)).isEqualTo(1);
        assertThat(result.subList(0, 4998)).containsExactlyInAnyOrderElementsOf(backlogExpectedRes);
        assertThat(result.subList(4998, 10000))
                .containsExactlyInAnyOrderElementsOf(realTimeExpectedRes);
    }

    private List<Tuple2<Integer, Long>> runKeyedAggregation(StreamExecutionEnvironment env)
            throws Exception {
        final DataGeneratorSource<Tuple2<Integer, Long>> historicalData =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Tuple2<Integer, Long>>)
                                value -> new Tuple2<>(value.intValue() % 2, 1L),
                        10000,
                        Types.TUPLE(Types.INT, Types.LONG));

        final DataGeneratorSource<Tuple2<Integer, Long>> realTimeData =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Tuple2<Integer, Long>>)
                                value -> new Tuple2<>(value.intValue() % 2, 1L),
                        10000,
                        Types.TUPLE(Types.INT, Types.LONG));

        final HybridSource<Tuple2<Integer, Long>> source =
                HybridSource.builder(historicalData).addSource(realTimeData).build();
        final SingleOutputStreamOperator<Tuple2<Integer, Long>> reduced =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                        .returns(Types.TUPLE(Types.INT, Types.LONG))
                        .setParallelism(1)
                        .keyBy(record -> record.f0)
                        .reduce(
                                (ReduceFunction<Tuple2<Integer, Long>>)
                                        (value1, value2) ->
                                                new Tuple2<>(value1.f0, value1.f1 + value2.f1));

        try (final CloseableIterator<Tuple2<Integer, Long>> iter = reduced.executeAndCollect()) {
            return CollectionUtil.iteratorToList(iter);
        }
    }

    private List<Tuple3<Integer, Long, Long>> runKeyedWindowedAggregation(
            StreamExecutionEnvironment env) throws Exception {
        final int backlogCnt = 10000;
        final DataGeneratorSource<Tuple3<Integer, Long, Long>> historicalData =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Tuple3<Integer, Long, Long>>)
                                value -> new Tuple3<>(value.intValue() % 2, value * 1000, 1L),
                        backlogCnt,
                        Types.TUPLE(Types.INT, Types.LONG));

        final DataGeneratorSource<Tuple3<Integer, Long, Long>> realTimeData =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Tuple3<Integer, Long, Long>>)
                                value ->
                                        new Tuple3<>(
                                                value.intValue() % 2,
                                                (value + backlogCnt) * 1000,
                                                1L),
                        10000,
                        Types.TUPLE(Types.INT, Types.LONG));

        final HybridSource<Tuple3<Integer, Long, Long>> source =
                HybridSource.builder(historicalData).addSource(realTimeData).build();
        final SingleOutputStreamOperator<Tuple3<Integer, Long, Long>> output =
                env.fromSource(
                                source,
                                WatermarkStrategy
                                        .<Tuple3<Integer, Long, Long>>forMonotonousTimestamps()
                                        .withTimestampAssigner((event, timestamp) -> event.f1),
                                "source")
                        .setParallelism(1)
                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG))
                        .keyBy(record -> record.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                        .aggregate(
                                new AggregateFunction<Tuple3<Integer, Long, Long>, Long, Long>() {
                                    @Override
                                    public Long createAccumulator() {
                                        return 0L;
                                    }

                                    @Override
                                    public Long add(
                                            Tuple3<Integer, Long, Long> value, Long accumulator) {
                                        return accumulator + value.f2;
                                    }

                                    @Override
                                    public Long getResult(Long accumulator) {
                                        return accumulator;
                                    }

                                    @Override
                                    public Long merge(Long a, Long b) {
                                        return a + b;
                                    }
                                },
                                (WindowFunction<
                                                Long,
                                                Tuple3<Integer, Long, Long>,
                                                Integer,
                                                TimeWindow>)
                                        (key, window, input, out) -> {
                                            for (Long i : input) {
                                                out.collect(Tuple3.of(key, window.getEnd(), i));
                                            }
                                        })
                        .returns(Types.TUPLE(Types.INT, Types.LONG, Types.LONG));

        List<Tuple3<Integer, Long, Long>> expectedRes = new ArrayList<>();
        for (int i = 1; i <= 5000; ++i) {
            expectedRes.add(Tuple3.of(0, (long) i * 4 * 1000, 2L));
            expectedRes.add(Tuple3.of(1, (long) i * 4 * 1000, 2L));
        }
        try (final CloseableIterator<Tuple3<Integer, Long, Long>> iter =
                output.executeAndCollect()) {
            return CollectionUtil.iteratorToList(iter);
        }
    }

    private <T, K> int getKeySwitchCnt(List<T> result, Function<T, K> keySelector) {
        final List<K> keys = result.stream().map(keySelector).collect(Collectors.toList());
        int count = 0;
        for (int i = 1; i < keys.size(); ++i) {
            if (keys.get(i - 1) != keys.get(i)) {
                count++;
            }
        }
        return count;
    }
}
