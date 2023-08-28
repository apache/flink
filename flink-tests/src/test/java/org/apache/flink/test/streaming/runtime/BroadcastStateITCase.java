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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** ITCase for the {@link org.apache.flink.api.common.state.BroadcastState}. */
public class BroadcastStateITCase extends AbstractTestBase {

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testKeyedWithBroadcastTranslation() throws Exception {

        final MapStateDescriptor<Long, String> utterDescriptor =
                new MapStateDescriptor<>(
                        "broadcast-state",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        final Map<Long, String> expected = new HashMap<>();
        expected.put(0L, "test:0");
        expected.put(1L, "test:1");
        expected.put(2L, "test:2");
        expected.put(3L, "test:3");
        expected.put(4L, "test:4");
        expected.put(5L, "test:5");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Long> srcOne =
                env.generateSequence(0L, 5L)
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<Long>() {

                                    private static final long serialVersionUID =
                                            -8500904795760316195L;

                                    @Override
                                    public long extractTimestamp(
                                            Long element, long previousElementTimestamp) {
                                        return element;
                                    }
                                })
                        .keyBy((KeySelector<Long, Long>) value -> value);

        final DataStream<String> srcTwo =
                env.fromCollection(expected.values())
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<String>() {

                                    private static final long serialVersionUID =
                                            -2148318224248467213L;

                                    @Override
                                    public long extractTimestamp(
                                            String element, long previousElementTimestamp) {
                                        return Long.parseLong(element.split(":")[1]);
                                    }
                                });

        final BroadcastStream<String> broadcast = srcTwo.broadcast(utterDescriptor);

        // the timestamp should be high enough to trigger the timer after all the elements arrive.
        final DataStream<String> output =
                srcOne.connect(broadcast)
                        .process(new TestKeyedBroadcastProcessFunction(100000L, expected));

        output.addSink(new TestSink(expected.size())).setParallelism(1);
        env.execute();
    }

    @Test
    public void testBroadcastTranslation() throws Exception {

        final MapStateDescriptor<Long, String> utterDescriptor =
                new MapStateDescriptor<>(
                        "broadcast-state",
                        BasicTypeInfo.LONG_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        final Map<Long, String> expected = new HashMap<>();
        expected.put(0L, "test:0");
        expected.put(1L, "test:1");
        expected.put(2L, "test:2");
        expected.put(3L, "test:3");
        expected.put(4L, "test:4");
        expected.put(5L, "test:5");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Long> srcOne =
                env.generateSequence(0L, 5L)
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<Long>() {

                                    private static final long serialVersionUID =
                                            -8500904795760316195L;

                                    @Override
                                    public long extractTimestamp(
                                            Long element, long previousElementTimestamp) {
                                        return element;
                                    }
                                });

        final DataStream<String> srcTwo =
                env.fromCollection(expected.values())
                        .assignTimestampsAndWatermarks(
                                new CustomWmEmitter<String>() {

                                    private static final long serialVersionUID =
                                            -2148318224248467213L;

                                    @Override
                                    public long extractTimestamp(
                                            String element, long previousElementTimestamp) {
                                        return Long.parseLong(element.split(":")[1]);
                                    }
                                });

        final BroadcastStream<String> broadcast = srcTwo.broadcast(utterDescriptor);

        // the timestamp should be high enough to trigger the timer after all the elements arrive.
        final DataStream<String> output =
                srcOne.connect(broadcast).process(new TestBroadcastProcessFunction());

        output.addSink(new TestSink(0)).setParallelism(1);
        env.execute();
    }

    private static class TestSink extends RichSinkFunction<String> {

        private static final long serialVersionUID = 7252508825104554749L;

        private final int expectedOutputCounter;

        private int outputCounter;

        TestSink(int expectedOutputCounter) {
            this.expectedOutputCounter = expectedOutputCounter;
            this.outputCounter = 0;
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            outputCounter++;
        }

        @Override
        public void close() throws Exception {
            super.close();

            // make sure that all the timers fired
            assertEquals(expectedOutputCounter, outputCounter);
        }
    }

    private abstract static class CustomWmEmitter<T>
            implements AssignerWithPunctuatedWatermarks<T> {

        private static final long serialVersionUID = -5187335197674841233L;

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp) {
            return new Watermark(extractedTimestamp);
        }
    }

    /**
     * A {@link KeyedBroadcastProcessFunction} which on the broadcast side puts elements in the
     * broadcast state while on the non-broadcast side, it sets a timer to fire at some point in the
     * future. Finally, when the onTimer method is called (i.e. when the timer fires), we verify
     * that the result is the expected one.
     */
    private static class TestKeyedBroadcastProcessFunction
            extends KeyedBroadcastProcessFunction<Long, Long, String, String> {

        private static final long serialVersionUID = 7616910653561100842L;

        private final Map<Long, String> expectedState;
        private final Map<Long, Long> timerToExpectedKey = new HashMap<>();

        private long nextTimerTimestamp;

        private transient MapStateDescriptor<Long, String> descriptor;

        TestKeyedBroadcastProcessFunction(
                final long initialTimerTimestamp, final Map<Long, String> expectedBroadcastState) {
            expectedState = expectedBroadcastState;
            nextTimerTimestamp = initialTimerTimestamp;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            descriptor =
                    new MapStateDescriptor<>(
                            "broadcast-state",
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);
        }

        @Override
        public void processElement(Long value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {
            long currentTime = nextTimerTimestamp;
            nextTimerTimestamp++;
            ctx.timerService().registerEventTimeTimer(currentTime);
            timerToExpectedKey.put(currentTime, value);
        }

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<String> out)
                throws Exception {
            long key = Long.parseLong(value.split(":")[1]);
            ctx.getBroadcastState(descriptor).put(key, value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertEquals(timerToExpectedKey.get(timestamp), ctx.getCurrentKey());

            Map<Long, String> map = new HashMap<>();
            for (Map.Entry<Long, String> entry :
                    ctx.getBroadcastState(descriptor).immutableEntries()) {
                map.put(entry.getKey(), entry.getValue());
            }

            assertEquals(expectedState, map);

            out.collect(Long.toString(timestamp));
        }
    }

    /**
     * This doesn't do much but we use it to verify that translation of non-keyed broadcast connect
     * works.
     */
    private static class TestBroadcastProcessFunction
            extends BroadcastProcessFunction<Long, String, String> {

        private static final long serialVersionUID = 7616910653561100842L;

        private transient MapStateDescriptor<Long, String> descriptor;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            descriptor =
                    new MapStateDescriptor<>(
                            "broadcast-state",
                            BasicTypeInfo.LONG_TYPE_INFO,
                            BasicTypeInfo.STRING_TYPE_INFO);
        }

        @Override
        public void processElement(Long value, ReadOnlyContext ctx, Collector<String> out)
                throws Exception {}

        @Override
        public void processBroadcastElement(String value, Context ctx, Collector<String> out)
                throws Exception {
            long key = Long.parseLong(value.split(":")[1]);
            ctx.getBroadcastState(descriptor).put(key, value);
        }
    }
}
