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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * These tests verify that the api calls on {@link WindowedStream} instantiate the correct window
 * operator.
 *
 * <p>We also create a test harness and push one element into the operator to verify that we get
 * some output.
 */
@SuppressWarnings("serial")
class WindowTranslationTest {

    // ------------------------------------------------------------------------
    //  Rich Pre-Aggregation Functions
    // ------------------------------------------------------------------------

    /**
     * .reduce() does not support RichReduceFunction, since the reduce function is used internally
     * in a {@code ReducingState}.
     */
    @Test
    void testReduceWithRichReducerFails() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        assertThatThrownBy(
                        () ->
                                source.keyBy(0)
                                        .window(
                                                SlidingEventTimeWindows.of(
                                                        Time.of(1, TimeUnit.SECONDS),
                                                        Time.of(100, TimeUnit.MILLISECONDS)))
                                        .reduce(
                                                new RichReduceFunction<Tuple2<String, Integer>>() {

                                                    @Override
                                                    public Tuple2<String, Integer> reduce(
                                                            Tuple2<String, Integer> value1,
                                                            Tuple2<String, Integer> value2) {
                                                        return null;
                                                    }
                                                }))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * .aggregate() does not support RichAggregateFunction, since the AggregationFunction is used
     * internally in a {@code AggregatingState}.
     */
    @Test
    void testAggregateWithRichFunctionFails() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        assertThatThrownBy(
                        () ->
                                source.keyBy(0)
                                        .window(
                                                SlidingEventTimeWindows.of(
                                                        Time.of(1, TimeUnit.SECONDS),
                                                        Time.of(100, TimeUnit.MILLISECONDS)))
                                        .aggregate(new DummyRichAggregationFunction<>()))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // ------------------------------------------------------------------------
    //  Merging Windows Support
    // ------------------------------------------------------------------------

    @Test
    void testMergingAssignerWithNonMergingTriggerFails() {
        // verify that we check for trigger compatibility

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<String, String, TimeWindow> windowedStream =
                env.fromData("Hello", "Ciao")
                        .keyBy(
                                new KeySelector<String, String>() {
                                    private static final long serialVersionUID =
                                            598309916882894293L;

                                    @Override
                                    public String getKey(String value) throws Exception {
                                        return value;
                                    }
                                })
                        .window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        assertThatThrownBy(
                        () ->
                                windowedStream.trigger(
                                        new Trigger<String, TimeWindow>() {
                                            private static final long serialVersionUID =
                                                    6558046711583024443L;

                                            @Override
                                            public TriggerResult onElement(
                                                    String element,
                                                    long timestamp,
                                                    TimeWindow window,
                                                    TriggerContext ctx)
                                                    throws Exception {
                                                return null;
                                            }

                                            @Override
                                            public TriggerResult onProcessingTime(
                                                    long time,
                                                    TimeWindow window,
                                                    TriggerContext ctx)
                                                    throws Exception {
                                                return null;
                                            }

                                            @Override
                                            public TriggerResult onEventTime(
                                                    long time,
                                                    TimeWindow window,
                                                    TriggerContext ctx)
                                                    throws Exception {
                                                return null;
                                            }

                                            @Override
                                            public boolean canMerge() {
                                                return false;
                                            }

                                            @Override
                                            public void clear(TimeWindow window, TriggerContext ctx)
                                                    throws Exception {}
                                        }))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testMergingWindowsWithEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source = env.fromData(1, 2);

        DataStream<String> window1 =
                source.keyBy(
                                new KeySelector<Integer, String>() {
                                    @Override
                                    public String getKey(Integer value) throws Exception {
                                        return value.toString();
                                    }
                                })
                        .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                        .evictor(CountEvictor.of(5))
                        .process(new TestProcessWindowFunction());

        final OneInputTransformation<Integer, String> transform =
                (OneInputTransformation<Integer, String>) window1.getTransformation();
        final OneInputStreamOperator<Integer, String> operator = transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Integer, ?, ?, ?> winOperator =
                (WindowOperator<String, Integer, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(EventTimeSessionWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator, winOperator.getKeySelector(), BasicTypeInfo.STRING_TYPE_INFO, 1);
    }

    // ------------------------------------------------------------------------
    //  Reduce Translation Tests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .reduce(new DummyReducer());

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(
                                SlidingProcessingTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .reduce(new DummyReducer());

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(SlidingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithWindowFunctionEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .reduce(
                                reducer,
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithWindowFunctionProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .reduce(
                                new DummyReducer(),
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String tuple,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithProcesWindowFunctionEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .reduce(
                                reducer,
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithProcessWindowFunctionProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .reduce(
                                new DummyReducer(),
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String tuple,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    /** Test for the deprecated .apply(Reducer, WindowFunction). */
    @Test
    @SuppressWarnings("rawtypes")
    void testApplyWithPreReducerEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .apply(
                                reducer,
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    /** Test for the deprecated .apply(Reducer, WindowFunction). */
    @Test
    @SuppressWarnings("rawtypes")
    void testApplyWithPreReducerAndEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple3<String, String, Integer>> window =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .evictor(CountEvictor.of(100))
                        .apply(
                                reducer,
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple3<String, String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple3<String, String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(new Tuple3<>(in.f0, in.f0, in.f1));
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple3<String, String, Integer>>)
                        window.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple3<String, String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    // ------------------------------------------------------------------------
    //  Aggregate Translation Tests
    // ------------------------------------------------------------------------

    @Test
    void testAggregateEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<Integer> window1 =
                source.keyBy(new Tuple3KeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .aggregate(new DummyAggregationFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, Integer> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, Integer>)
                        window1.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, Integer> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<Integer> window1 =
                source.keyBy(new Tuple3KeySelector())
                        .window(
                                SlidingProcessingTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .aggregate(new DummyAggregationFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, Integer> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, Integer>)
                        window1.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, Integer> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(SlidingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateWithWindowFunctionEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<String> window =
                source.keyBy(new Tuple3KeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new DummyAggregationFunction(), new TestWindowFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, String> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, String>)
                        window.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, String> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateWithWindowFunctionProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<String> window =
                source.keyBy(new Tuple3KeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new DummyAggregationFunction(), new TestWindowFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, String> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, String>)
                        window.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, String> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateWithProcessWindowFunctionEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<String> window =
                source.keyBy(new Tuple3KeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new DummyAggregationFunction(), new TestProcessWindowFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, String> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, String>)
                        window.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, String> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateWithProcessWindowFunctionProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<String> window =
                source.keyBy(new Tuple3KeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .aggregate(new DummyAggregationFunction(), new TestProcessWindowFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, String> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, String>)
                        window.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, String> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(AggregatingStateDescriptor.class);

        processElementAndEnsureOutput(
                operator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    // ------------------------------------------------------------------------
    //  Apply Translation Tests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("rawtypes")
    void testApplyEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testApplyProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testProcessEventTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .process(
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testProcessProcessingTime() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingProcessingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .process(
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(ProcessingTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner())
                .isInstanceOf(TumblingProcessingTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithCustomTrigger() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .trigger(CountTrigger.of(1))
                        .reduce(reducer);

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(CountTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ReducingStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testApplyWithCustomTrigger() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .trigger(CountTrigger.of(1))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(CountTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testProcessWithCustomTrigger() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .trigger(CountTrigger.of(1))
                        .process(
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple2<String, Integer>, ?, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(CountTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .evictor(CountEvictor.of(100))
                        .reduce(reducer);

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(EvictingWindowOperator.class);
        EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator =
                (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getEvictor()).isInstanceOf(CountEvictor.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testReduceWithEvictorAndProcessFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .evictor(CountEvictor.of(100))
                        .reduce(
                                reducer,
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        Tuple,
                                        TimeWindow>() {
                                    @Override
                                    public void process(
                                            Tuple tuple,
                                            Context context,
                                            Iterable<Tuple2<String, Integer>> elements,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : elements) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(EvictingWindowOperator.class);
        EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator =
                (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getEvictor()).isInstanceOf(CountEvictor.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    void testAggregateWithEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<Integer> window1 =
                source.keyBy(new Tuple3KeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .evictor(CountEvictor.of(100))
                        .aggregate(new DummyAggregationFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, Integer> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, Integer>)
                        window1.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, Integer> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    void testAggregateWithEvictorAndProcessFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple3<String, String, Integer>> source =
                env.fromData(Tuple3.of("hello", "hallo", 1), Tuple3.of("hello", "hallo", 2));

        DataStream<String> window1 =
                source.keyBy(new Tuple3KeySelector())
                        .window(
                                SlidingEventTimeWindows.of(
                                        Time.of(1, TimeUnit.SECONDS),
                                        Time.of(100, TimeUnit.MILLISECONDS)))
                        .evictor(CountEvictor.of(100))
                        .aggregate(new DummyAggregationFunction(), new TestProcessWindowFunction());

        final OneInputTransformation<Tuple3<String, String, Integer>, String> transform =
                (OneInputTransformation<Tuple3<String, String, Integer>, String>)
                        window1.getTransformation();

        final OneInputStreamOperator<Tuple3<String, String, Integer>, String> operator =
                transform.getOperator();

        assertThat(operator).isInstanceOf(WindowOperator.class);
        WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?> winOperator =
                (WindowOperator<String, Tuple3<String, String, Integer>, ?, ?, ?>) operator;

        assertThat(winOperator.getTrigger()).isInstanceOf(EventTimeTrigger.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(SlidingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple3<>("hello", "hallo", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testApplyWithEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .trigger(CountTrigger.of(1))
                        .evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            String key,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(EvictingWindowOperator.class);
        EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator =
                (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(CountTrigger.class);
        assertThat(winOperator.getEvictor()).isInstanceOf(TimeEvictor.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testProcessWithEvictor() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source =
                env.fromData(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(new TupleKeySelector())
                        .window(TumblingEventTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
                        .trigger(CountTrigger.of(1))
                        .evictor(TimeEvictor.of(Time.of(100, TimeUnit.MILLISECONDS)))
                        .process(
                                new ProcessWindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void process(
                                            String key,
                                            Context ctx,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {
                                        for (Tuple2<String, Integer> in : values) {
                                            out.collect(in);
                                        }
                                    }
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator =
                transform.getOperator();
        assertThat(operator).isInstanceOf(EvictingWindowOperator.class);
        EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?> winOperator =
                (EvictingWindowOperator<String, Tuple2<String, Integer>, ?, ?>) operator;
        assertThat(winOperator.getTrigger()).isInstanceOf(CountTrigger.class);
        assertThat(winOperator.getEvictor()).isInstanceOf(TimeEvictor.class);
        assertThat(winOperator.getWindowAssigner()).isInstanceOf(TumblingEventTimeWindows.class);
        assertThat(winOperator.getStateDescriptor()).isInstanceOf(ListStateDescriptor.class);

        processElementAndEnsureOutput(
                winOperator,
                winOperator.getKeySelector(),
                BasicTypeInfo.STRING_TYPE_INFO,
                new Tuple2<>("hello", 1));
    }

    /**
     * Ensure that we get some output from the given operator when pushing in an element and setting
     * watermark and processing time to {@code Long.MAX_VALUE}.
     */
    private static <K, IN, OUT> void processElementAndEnsureOutput(
            OneInputStreamOperator<IN, OUT> operator,
            KeySelector<IN, K> keySelector,
            TypeInformation<K> keyType,
            IN element)
            throws Exception {

        KeyedOneInputStreamOperatorTestHarness<K, IN, OUT> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(operator, keySelector, keyType);

        if (operator instanceof OutputTypeConfigurable) {
            // use a dummy type since window functions just need the ExecutionConfig
            // this is also only needed for Fold, which we're getting rid off soon.
            ((OutputTypeConfigurable) operator)
                    .setOutputType(BasicTypeInfo.STRING_TYPE_INFO, new ExecutionConfig());
        }

        testHarness.open();

        testHarness.setProcessingTime(0);
        testHarness.processWatermark(Long.MIN_VALUE);

        testHarness.processElement(new StreamRecord<>(element, 0));

        // provoke any processing-time/event-time triggers
        testHarness.setProcessingTime(Long.MAX_VALUE);
        testHarness.processWatermark(Long.MAX_VALUE);

        // we at least get the two watermarks and should also see an output element
        assertThat(testHarness.getOutput()).hasSizeGreaterThanOrEqualTo(3);

        testHarness.close();
    }

    // ------------------------------------------------------------------------
    //  UDFs
    // ------------------------------------------------------------------------

    private static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return value1;
        }
    }

    private static class DummyAggregationFunction
            implements AggregateFunction<
                    Tuple3<String, String, Integer>, Tuple2<String, Integer>, Integer> {

        @Override
        public Tuple2<String, Integer> createAccumulator() {
            return new Tuple2<>("", 0);
        }

        @Override
        public Tuple2<String, Integer> add(
                Tuple3<String, String, Integer> value, Tuple2<String, Integer> accumulator) {
            accumulator.f0 = value.f0;
            accumulator.f1 = value.f2;
            return accumulator;
        }

        @Override
        public Integer getResult(Tuple2<String, Integer> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
            return a;
        }
    }

    private static class DummyRichAggregationFunction<T> extends RichAggregateFunction<T, T, T> {

        @Override
        public T createAccumulator() {
            return null;
        }

        @Override
        public T add(T value, T accumulator) {
            return accumulator;
        }

        @Override
        public T getResult(T accumulator) {
            return accumulator;
        }

        @Override
        public T merge(T a, T b) {
            return a;
        }
    }

    private static class TestWindowFunction
            implements WindowFunction<Integer, String, String, TimeWindow> {

        @Override
        public void apply(
                String key, TimeWindow window, Iterable<Integer> values, Collector<String> out)
                throws Exception {

            for (Integer in : values) {
                out.collect(in.toString());
            }
        }
    }

    private static class TestProcessWindowFunction
            extends ProcessWindowFunction<Integer, String, String, TimeWindow> {

        @Override
        public void process(
                String key, Context ctx, Iterable<Integer> values, Collector<String> out)
                throws Exception {

            for (Integer in : values) {
                out.collect(in.toString());
            }
        }
    }

    private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class Tuple3KeySelector
            implements KeySelector<Tuple3<String, String, Integer>, String> {

        @Override
        public String getKey(Tuple3<String, String, Integer> value) throws Exception {
            return value.f0;
        }
    }
}
