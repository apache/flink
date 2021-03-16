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

package org.apache.flink.api.datastream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@code DataStream} reduce function.
 *
 * <p><b>Important:</b> This test does not use a shared {@code MiniCluster} to validate collection
 * on bounded streams after the Flink session has completed.
 */
public class DataStreamReduceFunctionTestITCase {

    private List<Tuple3<String, Integer, Long>> generateTestData() {
        List<Tuple3<String, Integer, Long>> data = new ArrayList<>();
        data.add(new Tuple3<>("A", 1, 0L));
        data.add(new Tuple3<>("A", 2, 10L));
        data.add(new Tuple3<>("A", 3, 20L));
        data.add(new Tuple3<>("B", 4, 30L));
        data.add(new Tuple3<>("B", 5, 40L));
        return data;
    }

    @Test
    public void testNoElementInputForReduceApplyAllWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, Long>> list =
                env.fromCollection(generateTestData())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(0))
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<
                                                        Tuple3<String, Integer, Long>>() {
                                                    @Override
                                                    public long extractTimestamp(
                                                            Tuple3<String, Integer, Long> element,
                                                            long recordTimestamp) {
                                                        return element.f1;
                                                    }
                                                }))
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .evictor(TimeEvictor.<TimeWindow>of(Time.seconds(0)))
                        .sum(1)
                        .executeAndCollect(5);

        Assert.assertEquals(list.size(), 0);
    }

    @Test
    public void testNormalInputForReduceApplyAllWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, Long>> list =
                env.fromCollection(generateTestData())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(0))
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<
                                                        Tuple3<String, Integer, Long>>() {
                                                    @Override
                                                    public long extractTimestamp(
                                                            Tuple3<String, Integer, Long> element,
                                                            long recordTimestamp) {
                                                        return element.f1;
                                                    }
                                                }))
                        .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .sum(1)
                        .executeAndCollect(5);

        Assert.assertEquals(list.size(), 1);
        Assert.assertNotNull(list.get(0));
    }

    @Test
    public void testNoElementInputForReduceApplyWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, Long>> list =
                env.fromCollection(generateTestData())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(0))
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<
                                                        Tuple3<String, Integer, Long>>() {
                                                    @Override
                                                    public long extractTimestamp(
                                                            Tuple3<String, Integer, Long> element,
                                                            long recordTimestamp) {
                                                        return element.f1;
                                                    }
                                                }))
                        .keyBy(0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .evictor(TimeEvictor.<TimeWindow>of(Time.seconds(0)))
                        .sum(1)
                        .executeAndCollect(5);

        Assert.assertEquals(list.size(), 0);
    }

    @Test
    public void testNormalInputForReduceApplyWindowFunction() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple3<String, Integer, Long>> list =
                env.fromCollection(generateTestData())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple3<String, Integer, Long>>forBoundedOutOfOrderness(
                                                Duration.ofSeconds(0))
                                        .withTimestampAssigner(
                                                new SerializableTimestampAssigner<
                                                        Tuple3<String, Integer, Long>>() {
                                                    @Override
                                                    public long extractTimestamp(
                                                            Tuple3<String, Integer, Long> element,
                                                            long recordTimestamp) {
                                                        return element.f1;
                                                    }
                                                }))
                        .keyBy(0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .sum(1)
                        .executeAndCollect(5);

        Assert.assertEquals(list.size(), 2);
    }
}
