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

package org.apache.flink.test.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeWindowPreconditions;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** ITCase that event time windows have a preceding event time watermark generator. */
public class ValidEventTimeWindowUsageITCase {

    private static final String EXPECTED_EXCEPTION_MSG =
            "Cannot use an EventTime window with a preceding water mark generator which does not ingest event times";

    private static final SlidingEventTimeWindows SLIDING_EVENT_TIME_WINDOW =
            SlidingEventTimeWindows.of(Time.milliseconds(1), Time.milliseconds(10));
    private static final TumblingEventTimeWindows TUMBLING_EVENT_TIME_WINDOW =
            TumblingEventTimeWindows.of(Time.milliseconds(1));

    @Test
    public void
            testKeyedWindow_SlidingEventTimeWindowThrowsExceptionIfNoPrecedingWatermarkGeneratorPresent() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Exception exception =
                assertThrows(
                        EventTimeWindowPreconditions.IllegalUseOfEventTimeWindowException.class,
                        () -> {
                            sourceNoWatermarks(env)
                                    .keyBy(t -> t.f1)
                                    .window(SLIDING_EVENT_TIME_WINDOW);
                        });

        assertTrue(exception.getMessage().contains(EXPECTED_EXCEPTION_MSG));
    }

    @Test
    public void
            testKeyedWindow_TumblingEventTimeWindowThrowsExceptionIfNoPrecedingWatermarkGeneratorPresent() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Exception exception =
                assertThrows(
                        EventTimeWindowPreconditions.IllegalUseOfEventTimeWindowException.class,
                        () -> {
                            sourceNoWatermarks(env)
                                    .keyBy(t -> t.f1)
                                    .window(TUMBLING_EVENT_TIME_WINDOW);
                        });

        assertTrue(exception.getMessage().contains(EXPECTED_EXCEPTION_MSG));
    }

    @Test
    public void
            testWindowAll_SlidingEventTimeWindowThrowsExceptionIfNoPrecedingWatermarkGeneratorPresent() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Exception exception =
                assertThrows(
                        EventTimeWindowPreconditions.IllegalUseOfEventTimeWindowException.class,
                        () -> {
                            sourceNoWatermarks(env).windowAll(SLIDING_EVENT_TIME_WINDOW);
                        });

        assertTrue(exception.getMessage().contains(EXPECTED_EXCEPTION_MSG));
    }

    @Test
    public void
            testWindowAll_TumblingEventTimeWindowThrowsExceptionIfNoPrecedingWatermarkGeneratorPresent() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Exception exception =
                assertThrows(
                        EventTimeWindowPreconditions.IllegalUseOfEventTimeWindowException.class,
                        () -> {
                            sourceNoWatermarks(env).windowAll(TUMBLING_EVENT_TIME_WINDOW);
                        });

        assertTrue(exception.getMessage().contains(EXPECTED_EXCEPTION_MSG));
    }

    @Test
    public void testKeyedStream_monotonousTimestampsIsValidEventTime() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        WatermarkStrategy<Tuple2<Long, Integer>> waStrat =
                WatermarkStrategy.forMonotonousTimestamps();

        sourceNoWatermarks(env)
                .assignTimestampsAndWatermarks(waStrat)
                .keyBy(t -> t.f1)
                .window(SLIDING_EVENT_TIME_WINDOW);

        sourceNoWatermarks(env)
                .assignTimestampsAndWatermarks(waStrat)
                .keyBy(t -> t.f1)
                .window(TUMBLING_EVENT_TIME_WINDOW);
    }

    @Test
    public void testKeyedStream_BoundedOutOfOrdernessIsValidEventTime() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        WatermarkStrategy<Tuple2<Long, Integer>> waStrat =
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(1));

        sourceNoWatermarks(env)
                .assignTimestampsAndWatermarks(waStrat)
                .keyBy(t -> t.f1)
                .window(SLIDING_EVENT_TIME_WINDOW);

        sourceNoWatermarks(env)
                .assignTimestampsAndWatermarks(waStrat)
                .keyBy(t -> t.f1)
                .window(TUMBLING_EVENT_TIME_WINDOW);
    }

    private SingleOutputStreamOperator<Tuple2<Long, Integer>> sourceNoWatermarks(
            StreamExecutionEnvironment env) {
        DataStreamSource<Tuple2<Long, Integer>> source = env.addSource(new EventSource());

        return source.assignTimestampsAndWatermarks(
                // Switch noWatermarks() to forMonotonousTimestamps()
                // and values are being printed.
                WatermarkStrategy.<Tuple2<Long, Integer>>noWatermarks()
                        .withTimestampAssigner((t, timestamp) -> t.f0));
    }

    private static class EventSource implements SourceFunction<Tuple2<Long, Integer>> {
        @Override
        public void run(SourceFunction.SourceContext<Tuple2<Long, Integer>> sourceContext)
                throws Exception {
            int i = 0;
            while (i < 5) {
                Tuple2<Long, Integer> tuple = Tuple2.of(System.currentTimeMillis(), i++ % 10);
                sourceContext.collect(tuple);
            }
        }

        @Override
        public void cancel() {}
    }
}
