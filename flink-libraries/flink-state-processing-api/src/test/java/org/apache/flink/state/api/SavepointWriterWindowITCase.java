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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.utils.MaxWatermarkSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Test for writing savepoints to the {@code WindowOperator}. */
@SuppressWarnings("unchecked")
@RunWith(Parameterized.class)
public class SavepointWriterWindowITCase extends AbstractTestBase {

    private static final String UID = "uid";

    private static final Collection<String> WORDS =
            Arrays.asList("hello", "world", "hello", "everyone");

    private static final Iterable<? extends Tuple2<String, Integer>> STANDARD_MATCHER =
            Arrays.asList(Tuple2.of("hello", 2), Tuple2.of("world", 1), Tuple2.of("everyone", 1));

    private static final Iterable<? extends Tuple2<String, Integer>> EVICTOR_MATCHER =
            Arrays.asList(Tuple2.of("hello", 1), Tuple2.of("world", 1), Tuple2.of("everyone", 1));

    private static final TypeInformation<Tuple2<String, Integer>> TUPLE_TYPE_INFO =
            new TypeHint<Tuple2<String, Integer>>() {}.getTypeInfo();

    private static final List<Tuple3<String, WindowBootstrap, WindowStream>> SETUP_FUNCTIONS =
            Arrays.asList(
                    Tuple3.of(
                            "reduce",
                            transformation -> transformation.reduce(new Reducer()),
                            stream -> stream.reduce(new Reducer())),
                    Tuple3.of(
                            "aggregate",
                            transformation -> transformation.aggregate(new Aggregator()),
                            stream -> stream.aggregate(new Aggregator())),
                    Tuple3.of(
                            "apply",
                            transformation -> transformation.apply(new CustomWindowFunction()),
                            stream -> stream.apply(new CustomWindowFunction())),
                    Tuple3.of(
                            "process",
                            transformation ->
                                    transformation.process(new CustomProcessWindowFunction()),
                            stream -> stream.process(new CustomProcessWindowFunction())));

    private static final List<Tuple2<String, StateBackend>> STATE_BACKENDS =
            Arrays.asList(
                    Tuple2.of("HashMap", new HashMapStateBackend()),
                    Tuple2.of("EmbeddedRocksDB", new EmbeddedRocksDBStateBackend()));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> parameterList = new ArrayList<>();
        for (Tuple2<String, StateBackend> stateBackend : STATE_BACKENDS) {
            for (Tuple3<String, WindowBootstrap, WindowStream> setup : SETUP_FUNCTIONS) {
                Object[] parameters =
                        new Object[] {
                            stateBackend.f0 + ": " + setup.f0, setup.f1, setup.f2, stateBackend.f1
                        };
                parameterList.add(parameters);
            }
        }

        return parameterList;
    }

    private final WindowBootstrap windowBootstrap;

    private final WindowStream windowStream;

    private final StateBackend stateBackend;

    @SuppressWarnings("unused")
    public SavepointWriterWindowITCase(
            String ignore,
            WindowBootstrap windowBootstrap,
            WindowStream windowStream,
            StateBackend stateBackend) {
        this.windowBootstrap = windowBootstrap;
        this.windowStream = windowStream;
        this.stateBackend = stateBackend;
    }

    @Test
    public void testTumbleWindow() throws Exception {
        final String savepointPath = getTempDirPath(new AbstractID().toHexString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Tuple2<String, Integer>> bootstrapData =
                env.fromCollection(WORDS)
                        .map(word -> Tuple2.of(word, 1))
                        .returns(TUPLE_TYPE_INFO)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>noWatermarks()
                                        .withTimestampAssigner((record, ts) -> 2L));

        WindowedStateTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation =
                OperatorTransformation.bootstrapWith(bootstrapData)
                        .keyBy(tuple -> tuple.f0, Types.STRING)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)));

        SavepointWriter.newSavepoint(env, stateBackend, 128)
                .withOperator(
                        OperatorIdentifier.forUid(UID), windowBootstrap.bootstrap(transformation))
                .write(savepointPath);

        env.execute("write state");

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream =
                env.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
                        .returns(TUPLE_TYPE_INFO)
                        .keyBy(tuple -> tuple.f0)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)));

        DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
        CloseableIterator<Tuple2<String, Integer>> future = windowed.collectAsync();

        submitJob(savepointPath, env);

        assertThat(future)
                .toIterable()
                .as("Incorrect results from bootstrapped windows")
                .containsAll(STANDARD_MATCHER);
    }

    @Test
    public void testTumbleWindowWithEvictor() throws Exception {
        final String savepointPath = getTempDirPath(new AbstractID().toHexString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Tuple2<String, Integer>> bootstrapData =
                env.fromCollection(WORDS)
                        .map(word -> Tuple2.of(word, 1))
                        .returns(TUPLE_TYPE_INFO)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>noWatermarks()
                                        .withTimestampAssigner((record, ts) -> 2L));

        WindowedStateTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation =
                OperatorTransformation.bootstrapWith(bootstrapData)
                        .keyBy(tuple -> tuple.f0, Types.STRING)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                        .evictor(CountEvictor.of(1));

        SavepointWriter.newSavepoint(env, stateBackend, 128)
                .withOperator(
                        OperatorIdentifier.forUid(UID), windowBootstrap.bootstrap(transformation))
                .write(savepointPath);

        env.execute("write state");

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream =
                env.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
                        .returns(TUPLE_TYPE_INFO)
                        .keyBy(tuple -> tuple.f0)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                        .evictor(CountEvictor.of(1));

        DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
        CloseableIterator<Tuple2<String, Integer>> future = windowed.collectAsync();

        submitJob(savepointPath, env);

        assertThat(future)
                .toIterable()
                .as("Incorrect results from bootstrapped windows")
                .containsAll(EVICTOR_MATCHER);
    }

    @Test
    public void testSlideWindow() throws Exception {
        final String savepointPath = getTempDirPath(new AbstractID().toHexString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Tuple2<String, Integer>> bootstrapData =
                env.fromCollection(WORDS)
                        .map(word -> Tuple2.of(word, 1), TUPLE_TYPE_INFO)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>noWatermarks()
                                        .withTimestampAssigner((record, ts) -> 2L));

        WindowedStateTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation =
                OperatorTransformation.bootstrapWith(bootstrapData)
                        .keyBy(tuple -> tuple.f0, Types.STRING)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(5), Time.milliseconds(1)));

        SavepointWriter.newSavepoint(env, stateBackend, 128)
                .withOperator(
                        OperatorIdentifier.forUid(UID), windowBootstrap.bootstrap(transformation))
                .write(savepointPath);

        env.execute("write state");

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream =
                env.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
                        .returns(TUPLE_TYPE_INFO)
                        .keyBy(tuple -> tuple.f0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(5), Time.milliseconds(1)));

        DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
        CloseableIterator<Tuple2<String, Integer>> future = windowed.collectAsync();

        submitJob(savepointPath, env);

        assertThat(future)
                .toIterable()
                .as("Incorrect results from bootstrapped windows")
                .containsAll(STANDARD_MATCHER);
    }

    @Test
    public void testSlideWindowWithEvictor() throws Exception {
        final String savepointPath = getTempDirPath(new AbstractID().toHexString());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStateBackend(stateBackend);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        DataStream<Tuple2<String, Integer>> bootstrapData =
                env.fromCollection(WORDS)
                        .map(word -> Tuple2.of(word, 1))
                        .returns(TUPLE_TYPE_INFO)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Tuple2<String, Integer>>noWatermarks()
                                        .withTimestampAssigner((record, ts) -> 2L));

        WindowedStateTransformation<Tuple2<String, Integer>, String, TimeWindow> transformation =
                OperatorTransformation.bootstrapWith(bootstrapData)
                        .keyBy(tuple -> tuple.f0, Types.STRING)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(5), Time.milliseconds(1)))
                        .evictor(CountEvictor.of(1));

        SavepointWriter.newSavepoint(env, stateBackend, 128)
                .withOperator(
                        OperatorIdentifier.forUid(UID), windowBootstrap.bootstrap(transformation))
                .write(savepointPath);

        env.execute("write state");

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream =
                env.addSource(new MaxWatermarkSource<Tuple2<String, Integer>>())
                        .returns(TUPLE_TYPE_INFO)
                        .keyBy(tuple -> tuple.f0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.milliseconds(5), Time.milliseconds(1)))
                        .evictor(CountEvictor.of(1));

        DataStream<Tuple2<String, Integer>> windowed = windowStream.window(stream).uid(UID);
        CloseableIterator<Tuple2<String, Integer>> future = windowed.collectAsync();

        submitJob(savepointPath, env);

        assertThat(future)
                .toIterable()
                .as("Incorrect results from bootstrapped windows")
                .containsAll(EVICTOR_MATCHER);
    }

    private void submitJob(String savepointPath, StreamExecutionEnvironment sEnv) throws Exception {
        StreamGraph streamGraph = sEnv.getStreamGraph();
        streamGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, true));

        sEnv.execute(streamGraph);
    }

    private static class Reducer implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            return Tuple2.of(value1.f0, value1.f1 + value2.f1);
        }
    }

    private static class Aggregator
            implements AggregateFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return null;
        }

        @Override
        public Tuple2<String, Integer> add(
                Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
            if (accumulator == null) {
                return Tuple2.of(value.f0, value.f1);
            }

            accumulator.f1 += value.f1;
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            a.f1 += b.f1;
            return a;
        }
    }

    private static class CustomWindowFunction
            implements WindowFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void apply(
                String s,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out) {
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            Tuple2<String, Integer> acc = iterator.next();

            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                acc.f1 += next.f1;
            }

            out.collect(acc);
        }
    }

    private static class CustomProcessWindowFunction
            extends ProcessWindowFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {

        @Override
        public void process(
                String s,
                Context context,
                Iterable<Tuple2<String, Integer>> elements,
                Collector<Tuple2<String, Integer>> out) {
            Iterator<Tuple2<String, Integer>> iterator = elements.iterator();
            Tuple2<String, Integer> acc = iterator.next();

            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                acc.f1 += next.f1;
            }

            out.collect(acc);
        }
    }

    @FunctionalInterface
    private interface WindowBootstrap {
        StateBootstrapTransformation<Tuple2<String, Integer>> bootstrap(
                WindowedStateTransformation<Tuple2<String, Integer>, String, TimeWindow>
                        transformation);
    }

    @FunctionalInterface
    private interface WindowStream {
        SingleOutputStreamOperator<Tuple2<String, Integer>> window(
                WindowedStream<Tuple2<String, Integer>, String, TimeWindow> stream);
    }
}
