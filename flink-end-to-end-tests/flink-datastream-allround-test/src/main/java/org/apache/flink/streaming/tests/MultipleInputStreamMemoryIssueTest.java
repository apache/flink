/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeoutTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** MultipleInputStreamTask Memory issue test. */
public class MultipleInputStreamMemoryIssueTest {

    /**
     * List Aggregation.
     *
     * @param <IN>
     */
    static class ListAgg<IN> implements AggregateFunction<IN, List<IN>, List<IN>> {

        @Override
        public List<IN> createAccumulator() {
            return Lists.newArrayList();
        }

        @Override
        public List<IN> add(IN value, List<IN> accumulator) {
            accumulator.add(value);
            return accumulator;
        }

        @Override
        public List<IN> getResult(List<IN> accumulator) {
            return accumulator;
        }

        @Override
        public List<IN> merge(List<IN> a, List<IN> b) {
            List<IN> res = new ArrayList<IN>(a.size() + b.size());
            res.addAll(a);
            res.addAll(b);
            return res;
        }
    }

    public static void main(String[] args) {
        final StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.getConfig().setAutoWatermarkInterval(200);
        environment.getConfig().enableObjectReuse();
        environment.getConfig().enableClosureCleaner();
        environment.getConfig().setLatencyTrackingInterval(1000);

        DataStream<Tuple2<Integer, Integer>> source1 =
                environment.addSource(
                        new RichSourceFunction<Tuple2<Integer, Integer>>() {
                            private transient int ct = 0;
                            private transient boolean isRunning = true;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                ct = 0;
                                isRunning = true;
                            }

                            @Override
                            public void run(SourceContext<Tuple2<Integer, Integer>> ctx)
                                    throws Exception {
                                while (isRunning) {
                                    ct += 1;
                                    ctx.collectWithTimestamp(
                                            Tuple2.of(1, ct), System.currentTimeMillis());
                                    try {
                                        Thread.sleep(1);
                                    } catch (InterruptedException e) {
                                        // ignored
                                    }
                                }
                            }

                            @Override
                            public void cancel() {
                                isRunning = false;
                            }
                        });

        MapStateDescriptor<String, List<Tuple2<Integer, Integer>>> desc =
                new MapStateDescriptor<>(
                        "broadcast-state",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        new ListTypeInfo<>(
                                TypeInformation.of(new TypeHint<Tuple2<Integer, Integer>>() {})));

        BroadcastStream<List<Tuple2<Integer, Integer>>> source2 =
                environment
                        .addSource(
                                new RichSourceFunction<Tuple2<Integer, Integer>>() {

                                    private transient int ct = 0;
                                    private transient boolean isRunning = true;
                                    private transient boolean isIdle = false;
                                    private transient int acc = 0;

                                    @Override
                                    public void open(Configuration parameters) throws Exception {
                                        super.open(parameters);
                                        ct = 0;
                                        isRunning = true;
                                        isIdle = false;
                                        acc = 0;
                                    }

                                    @Override
                                    public void run(SourceContext<Tuple2<Integer, Integer>> ctx)
                                            throws Exception {
                                        while (isRunning) {
                                            if (ct < 10) {
                                                ctx.collectWithTimestamp(
                                                        Tuple2.of(2, acc),
                                                        System.currentTimeMillis());
                                                acc += 1;
                                            } else if (ct
                                                    > (Duration.ofSeconds(10).toMillis() / 100)) {
                                                // recover from idle
                                                //                                                ct
                                                // = 0;
                                                //
                                                // isIdle = false;
                                            } else if (!isIdle) {
                                                isIdle = true;
                                                ctx.markAsTemporarilyIdle();
                                            }
                                            ct += 1;
                                            try {
                                                Thread.sleep(100);
                                            } catch (InterruptedException e) {
                                                // ignored
                                            }
                                        }
                                    }

                                    @Override
                                    public void cancel() {
                                        isRunning = false;
                                    }
                                })
                        .windowAll(GlobalWindows.create())
                        .trigger(
                                PurgingTrigger.of(
                                        ProcessingTimeoutTrigger.of(
                                                CountTrigger.of(100),
                                                Duration.ofSeconds(1),
                                                true,
                                                true)))
                        .aggregate(new ListAgg<>())
                        .name("diagnosis-config-buffering")
                        .setParallelism(1)
                        .broadcast(desc);

        DataStream<Tuple2<Integer, Integer>> stream1 =
                source1.connect(source2)
                        .process(
                                new BroadcastProcessFunction<
                                        Tuple2<Integer, Integer>,
                                        List<Tuple2<Integer, Integer>>,
                                        Tuple2<Integer, Integer>>() {
                                    @Override
                                    public void processElement(
                                            Tuple2<Integer, Integer> value,
                                            ReadOnlyContext ctx,
                                            Collector<Tuple2<Integer, Integer>> out)
                                            throws Exception {
                                        out.collect(value);
                                    }

                                    @Override
                                    public void processBroadcastElement(
                                            List<Tuple2<Integer, Integer>> value,
                                            Context ctx,
                                            Collector<Tuple2<Integer, Integer>> out)
                                            throws Exception {
                                        System.out.println(
                                                String.format(
                                                        "broadcast %d ,[%s]",
                                                        value.size(),
                                                        value.stream()
                                                                .map(String::valueOf)
                                                                .collect(Collectors.joining(","))));
                                    }
                                })
                        .process(
                                new ProcessFunction<
                                        Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
                                    @Override
                                    public void processElement(
                                            Tuple2<Integer, Integer> value,
                                            ProcessFunction<
                                                                    Tuple2<Integer, Integer>,
                                                                    Tuple2<Integer, Integer>>
                                                            .Context
                                                    ctx,
                                            Collector<Tuple2<Integer, Integer>> out)
                                            throws Exception {
                                        out.collect(value);
                                    }
                                });

        stream1.addSink(new DiscardingSink<>()).setParallelism(10);

        try {
            environment.execute("dummy_multiple_input_test");
        } catch (Exception error) {
            error.printStackTrace();
        }
    }
}
