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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * These tests verify that the api calls on {@link WindowedStream} that use the "time" shortcut
 * instantiate the correct window operator.
 */
public class TimeWindowTranslationTest {

    /**
     * Verifies that calls to timeWindow() instantiate a regular windowOperator instead of an
     * aligned one.
     */
    @Test
    public void testAlignedWindowDeprecation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStream<Tuple2<String, Integer>> source =
                env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DummyReducer reducer = new DummyReducer();

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .timeWindow(
                                Time.of(1000, TimeUnit.MILLISECONDS),
                                Time.of(100, TimeUnit.MILLISECONDS))
                        .reduce(reducer);

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        Assert.assertTrue(operator1 instanceof WindowOperator);

        DataStream<Tuple2<String, Integer>> window2 =
                source.keyBy(0)
                        .timeWindow(Time.of(1000, TimeUnit.MILLISECONDS))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        Tuple,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            Tuple tuple,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {}
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform2 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window2.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator2 =
                transform2.getOperator();
        Assert.assertTrue(operator2 instanceof WindowOperator);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testReduceEventTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Tuple2<String, Integer>> source =
                env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .timeWindow(
                                Time.of(1000, TimeUnit.MILLISECONDS),
                                Time.of(100, TimeUnit.MILLISECONDS))
                        .reduce(new DummyReducer());

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        Assert.assertTrue(operator1 instanceof WindowOperator);
        WindowOperator winOperator1 = (WindowOperator) operator1;
        Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
        Assert.assertTrue(winOperator1.getWindowAssigner() instanceof SlidingEventTimeWindows);
        Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ReducingStateDescriptor);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testApplyEventTimeWindows() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Tuple2<String, Integer>> source =
                env.fromElements(Tuple2.of("hello", 1), Tuple2.of("hello", 2));

        DataStream<Tuple2<String, Integer>> window1 =
                source.keyBy(0)
                        .timeWindow(Time.of(1000, TimeUnit.MILLISECONDS))
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        Tuple,
                                        TimeWindow>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public void apply(
                                            Tuple tuple,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> values,
                                            Collector<Tuple2<String, Integer>> out)
                                            throws Exception {}
                                });

        OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>> transform1 =
                (OneInputTransformation<Tuple2<String, Integer>, Tuple2<String, Integer>>)
                        window1.getTransformation();
        OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator1 =
                transform1.getOperator();
        Assert.assertTrue(operator1 instanceof WindowOperator);
        WindowOperator winOperator1 = (WindowOperator) operator1;
        Assert.assertTrue(winOperator1.getTrigger() instanceof EventTimeTrigger);
        Assert.assertTrue(winOperator1.getWindowAssigner() instanceof TumblingEventTimeWindows);
        Assert.assertTrue(winOperator1.getStateDescriptor() instanceof ListStateDescriptor);
    }

    // ------------------------------------------------------------------------
    //  UDFs
    // ------------------------------------------------------------------------

    private static class DummyReducer implements ReduceFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return value1;
        }
    }
}
