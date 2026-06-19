/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.datastream.extension.eventtime;

import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputNonPartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.function.OneInputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoInputNonBroadcastEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.function.TwoOutputEventTimeStreamProcessFunction;
import org.apache.flink.datastream.api.extension.eventtime.timer.EventTimeManager;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** This ITCase class tests the behavior of {@link EventTimeExtension}. */
class EventTimeExtensionITCase implements Serializable {
    private ExecutionEnvironment env;
    private List<Tuple2<Long, String>> inputRecords =
            List.of(Tuple2.of(1L, "a"), Tuple2.of(2L, "b"), Tuple2.of(3L, "c"));
    private List<Long> inputEventTimes =
            inputRecords.stream().map(x -> x.f0).collect(Collectors.toList());

    @BeforeEach
    void before() throws Exception {
        env = ExecutionEnvironment.getInstance();
    }

    @AfterEach
    void after() {
        // one input
        TestOneInputStreamProcessFunction.receivedRecords.clear();
        TestOneInputStreamProcessFunction.receivedEventTimes.clear();
        TestOneInputEventTimeStreamProcessFunction.receivedRecords.clear();
        TestOneInputEventTimeStreamProcessFunction.receivedEventTimes.clear();
        TestOneInputEventTimeStreamProcessFunction.invokedTimerTimes.clear();

        // two output
        TestTwoOutputStreamProcessFunction.receivedRecords.clear();
        TestTwoOutputStreamProcessFunction.receivedEventTimes.clear();
        TestTwoOutputEventTimeStreamProcessFunction.receivedRecords.clear();
        TestTwoOutputEventTimeStreamProcessFunction.receivedEventTimes.clear();
        TestTwoOutputEventTimeStreamProcessFunction.invokedTimerTimes.clear();

        // two input broadcast
        TestTwoInputBroadcastStreamProcessFunction.receivedRecords.clear();
        TestTwoInputBroadcastStreamProcessFunction.receivedEventTimes.clear();
        TestTwoInputBroadcastEventTimeStreamProcessFunction.receivedRecords.clear();
        TestTwoInputBroadcastEventTimeStreamProcessFunction.receivedEventTimes.clear();

        // two input non-broadcast
        TestTwoInputNonBroadcastStreamProcessFunction.receivedRecords.clear();
        TestTwoInputNonBroadcastStreamProcessFunction.receivedEventTimes.clear();
        TestTwoInputNonBroadcastEventTimeStreamProcessFunction.receivedRecords.clear();
        TestTwoInputNonBroadcastEventTimeStreamProcessFunction.receivedEventTimes.clear();
        TestTwoInputNonBroadcastEventTimeStreamProcessFunction.invokedTimerTimes.clear();
    }

    @Test
    void testWatermarkGeneratorGenerateEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        source.process(new TestOneInputStreamProcessFunction(true));
        env.execute("testWatermarkGeneratorGenerateEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    // ============== test one input =================

    @Test
    void testOneInputProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        source.process(new TestOneInputStreamProcessFunction(false))
                .process(new TestOneInputStreamProcessFunction(true));
        env.execute("testOneInputProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    @Test
    void testOneInputEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer()
            throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        source.keyBy(x -> x.f0)
                .process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestOneInputEventTimeStreamProcessFunction(true)));
        env.execute(
                "testOneInputEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer");

        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestOneInputEventTimeStreamProcessFunction.invokedTimerTimes)
                .containsExactlyElementsOf(
                        inputEventTimes.stream().map(x -> x + 1).collect(Collectors.toList()));
    }

    @Test
    void testOneInputEventTimeProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        source.process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestOneInputEventTimeStreamProcessFunction(false)))
                .keyBy(x -> x.f0)
                .process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestOneInputEventTimeStreamProcessFunction(true)));
        env.execute("testOneInputEventTimeProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestOneInputEventTimeStreamProcessFunction.invokedTimerTimes)
                .containsExactlyElementsOf(
                        inputEventTimes.stream().map(x -> x + 1).collect(Collectors.toList()));
    }

    // ============== test two output =================

    @Test
    void testTwoOutputProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<
                        Tuple2<Long, String>, Tuple2<Long, String>>
                twoOutputStream = source.process(new TestTwoOutputStreamProcessFunction(false));
        twoOutputStream.getFirst().process(new TestOneInputStreamProcessFunction(true));
        twoOutputStream
                .getFirst()
                .keyBy(x -> x.f0)
                .process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestOneInputEventTimeStreamProcessFunction(true)));
        env.execute("testTwoOutputProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    @Test
    void testTwoOutputEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer()
            throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        source.keyBy(x -> x.f0)
                .process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestTwoOutputEventTimeStreamProcessFunction(true)));
        env.execute(
                "testTwoOutputEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer");

        assertThat(TestTwoOutputEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestTwoOutputEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestTwoOutputEventTimeStreamProcessFunction.invokedTimerTimes)
                .containsExactlyElementsOf(
                        inputEventTimes.stream().map(x -> x + 1).collect(Collectors.toList()));
    }

    @Test
    void testTwoOutputEventTimeProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<
                        Tuple2<Long, String>, Tuple2<Long, String>>
                twoOutputStream =
                        source.process(
                                EventTimeExtension.wrapProcessFunction(
                                        new TestTwoOutputEventTimeStreamProcessFunction(false)));
        twoOutputStream.getFirst().process(new TestOneInputStreamProcessFunction(true));
        twoOutputStream
                .getFirst()
                .keyBy(x -> x.f0)
                .process(
                        EventTimeExtension.wrapProcessFunction(
                                new TestOneInputEventTimeStreamProcessFunction(true)));
        env.execute("testTwoOutputEventTimeProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    // ============== test two input broadcast =================

    @Test
    void testTwoInputBroadcastProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.broadcast()
                .connectAndProcess(source2, new TestTwoInputBroadcastStreamProcessFunction(false))
                .process(new TestOneInputStreamProcessFunction(true));
        env.execute("testTwoInputBroadcastProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    @Test
    void testTwoInputBroadcastEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer()
            throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.broadcast()
                .connectAndProcess(
                        source2,
                        EventTimeExtension.wrapProcessFunction(
                                new TestTwoInputBroadcastEventTimeStreamProcessFunction(true)));
        env.execute(
                "testTwoInputBroadcastEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer");

        assertThat(TestTwoInputBroadcastEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestTwoInputBroadcastEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    @Test
    void testTwoInputBroadcastEventTimeProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.broadcast()
                .connectAndProcess(
                        source2,
                        EventTimeExtension.wrapProcessFunction(
                                new TestTwoInputBroadcastEventTimeStreamProcessFunction(false)))
                .process(new TestOneInputStreamProcessFunction(true));
        env.execute("testTwoInputBroadcastEventTimeProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    // ============== test two input non-broadcast =================

    @Test
    void testTwoInputNonBroadcastProcessFunctionForwardEventTimeWatermark() throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.connectAndProcess(source2, new TestTwoInputNonBroadcastStreamProcessFunction(false))
                .process(new TestOneInputStreamProcessFunction(true));
        env.execute("testTwoInputNonBroadcastProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    @Test
    void testTwoInputNonBroadcastEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer()
            throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.keyBy(x -> x.f0)
                .connectAndProcess(
                        source2.keyBy(x -> x.f0),
                        EventTimeExtension.wrapProcessFunction(
                                new TestTwoInputNonBroadcastEventTimeStreamProcessFunction(true)));
        env.execute(
                "testTwoInputNonBroadcastEventTimeProcessFunctionReceiveEventTimeWatermarkAndRegisterTimer");

        assertThat(TestTwoInputNonBroadcastEventTimeStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestTwoInputNonBroadcastEventTimeStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
        assertThat(TestTwoInputNonBroadcastEventTimeStreamProcessFunction.invokedTimerTimes)
                .containsExactlyElementsOf(
                        inputEventTimes.stream().map(x -> x + 1).collect(Collectors.toList()));
    }

    @Test
    void testTwoInputNonBroadcastEventTimeProcessFunctionForwardEventTimeWatermark()
            throws Exception {
        NonKeyedPartitionStream<Tuple2<Long, String>> source1 = getSourceWithWatermarkGenerator();
        NonKeyedPartitionStream<Tuple2<Long, String>> source2 = getSourceWithWatermarkGenerator();
        source1.keyBy(x -> x.f0)
                .connectAndProcess(
                        source2.keyBy(x -> x.f0),
                        EventTimeExtension.wrapProcessFunction(
                                new TestTwoInputNonBroadcastEventTimeStreamProcessFunction(false)))
                .process(new TestOneInputStreamProcessFunction(true));
        env.execute("testTwoInputNonBroadcastEventTimeProcessFunctionForwardEventTimeWatermark");

        assertThat(TestOneInputStreamProcessFunction.receivedRecords)
                .containsExactlyElementsOf(inputRecords);
        assertThat(TestOneInputStreamProcessFunction.receivedEventTimes)
                .containsExactlyElementsOf(inputEventTimes);
    }

    private static class TestOneInputStreamProcessFunction
            implements OneInputStreamProcessFunction<Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;

        public TestOneInputStreamProcessFunction(boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            if (EventTimeExtension.isEventTimeWatermark(watermark)
                    && needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(((LongWatermark) watermark).getValue());
            }
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public void processRecord(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
            }
            output.collect(record);
        }
    }

    private static class TestOneInputEventTimeStreamProcessFunction
            implements OneInputEventTimeStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> invokedTimerTimes = new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;
        private EventTimeManager eventTimeManager;

        public TestOneInputEventTimeStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public void processRecord(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
                eventTimeManager.registerTimer(record.f0 + 1);
            }
            output.collect(record);
        }

        @Override
        public void onEventTimeWatermark(
                long watermarkTimestamp,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(watermarkTimestamp);
            }
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public void onEventTimer(
                long timestamp,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx) {
            if (needCollectReceivedWatermarkAndRecord) {
                invokedTimerTimes.add(timestamp);
            }
        }
    }

    private static class TestTwoOutputStreamProcessFunction
            implements TwoOutputStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;

        public TestTwoOutputStreamProcessFunction(boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputNonPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx) {
            if (EventTimeExtension.isEventTimeWatermark(watermark)
                    && needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(((LongWatermark) watermark).getValue());
            }
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public void processRecord(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
            }
            output1.collect(record);
            output2.collect(record);
        }
    }

    private static class TestTwoOutputEventTimeStreamProcessFunction
            implements TwoOutputEventTimeStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> invokedTimerTimes = new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;
        private EventTimeManager eventTimeManager;

        public TestTwoOutputEventTimeStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public void processRecord(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
                eventTimeManager.registerTimer(record.f0 + 1);
            }
            output1.collect(record);
            output2.collect(record);
        }

        @Override
        public WatermarkHandlingResult onWatermark(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputNonPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public void onEventTimeWatermark(
                long watermarkTimestamp,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputNonPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(watermarkTimestamp);
            }
        }

        @Override
        public void onEventTimer(
                long timestamp,
                Collector<Tuple2<Long, String>> output1,
                Collector<Tuple2<Long, String>> output2,
                TwoOutputPartitionedContext<Tuple2<Long, String>, Tuple2<Long, String>> ctx) {
            if (needCollectReceivedWatermarkAndRecord) {
                invokedTimerTimes.add(timestamp);
            }
        }
    }

    private static class TestTwoInputBroadcastStreamProcessFunction
            implements TwoInputBroadcastStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;

        public TestTwoInputBroadcastStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void processRecordFromNonBroadcastInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
            }
            output.collect(record);
        }

        @Override
        public void processRecordFromBroadcastInput(
                Tuple2<Long, String> record, NonPartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            // do nothing
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromBroadcastInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            if (EventTimeExtension.isEventTimeWatermark(watermark)
                    && needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(((LongWatermark) watermark).getValue());
            }
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromNonBroadcastInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            if (EventTimeExtension.isEventTimeWatermark(watermark)
                    && needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(((LongWatermark) watermark).getValue());
            }
            return WatermarkHandlingResult.PEEK;
        }
    }

    private static class TestTwoInputBroadcastEventTimeStreamProcessFunction
            implements TwoInputBroadcastEventTimeStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;
        private EventTimeManager eventTimeManager;

        public TestTwoInputBroadcastEventTimeStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public void processRecordFromNonBroadcastInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
            }
            output.collect(record);
        }

        @Override
        public void processRecordFromBroadcastInput(
                Tuple2<Long, String> record, NonPartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            // do nothing
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromBroadcastInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromNonBroadcastInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public void onEventTimeWatermark(
                long watermarkTimestamp,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(watermarkTimestamp);
            }
        }

        @Override
        public void onEventTimer(
                long timestamp,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx) {
            throw new UnsupportedOperationException("This function shouldn't be invoked.");
        }
    }

    private static class TestTwoInputNonBroadcastStreamProcessFunction
            implements TwoInputNonBroadcastStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;

        public TestTwoInputNonBroadcastStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void processRecordFromFirstInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
            }
            output.collect(record);
        }

        @Override
        public void processRecordFromSecondInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {}

        @Override
        public WatermarkHandlingResult onWatermarkFromFirstInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            if (EventTimeExtension.isEventTimeWatermark(watermark)
                    && needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(((LongWatermark) watermark).getValue());
            }
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromSecondInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            return WatermarkHandlingResult.PEEK;
        }
    }

    private static class TestTwoInputNonBroadcastEventTimeStreamProcessFunction
            implements TwoInputNonBroadcastEventTimeStreamProcessFunction<
                    Tuple2<Long, String>, Tuple2<Long, String>, Tuple2<Long, String>> {
        public static ConcurrentLinkedQueue<Tuple2<Long, String>> receivedRecords =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> receivedEventTimes =
                new ConcurrentLinkedQueue<>();
        public static ConcurrentLinkedQueue<Long> invokedTimerTimes = new ConcurrentLinkedQueue<>();
        private boolean needCollectReceivedWatermarkAndRecord;
        private EventTimeManager eventTimeManager;

        public TestTwoInputNonBroadcastEventTimeStreamProcessFunction(
                boolean needCollectReceivedWatermarkAndRecord) {
            this.needCollectReceivedWatermarkAndRecord = needCollectReceivedWatermarkAndRecord;
        }

        @Override
        public void initEventTimeProcessFunction(EventTimeManager eventTimeManager) {
            this.eventTimeManager = eventTimeManager;
        }

        @Override
        public void onEventTimeWatermark(
                long watermarkTimestamp,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedEventTimes.add(watermarkTimestamp);
            }
        }

        @Override
        public void onEventTimer(
                long timestamp,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx) {
            if (needCollectReceivedWatermarkAndRecord) {
                invokedTimerTimes.add(timestamp);
            }
        }

        @Override
        public void processRecordFromFirstInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            if (needCollectReceivedWatermarkAndRecord) {
                receivedRecords.add(record);
                eventTimeManager.registerTimer(record.f0 + 1);
            }
            output.collect(record);
        }

        @Override
        public void processRecordFromSecondInput(
                Tuple2<Long, String> record,
                Collector<Tuple2<Long, String>> output,
                PartitionedContext<Tuple2<Long, String>> ctx)
                throws Exception {
            // do nothing
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromFirstInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }

        @Override
        public WatermarkHandlingResult onWatermarkFromSecondInput(
                Watermark watermark,
                Collector<Tuple2<Long, String>> output,
                NonPartitionedContext<Tuple2<Long, String>> ctx) {
            assertThat(EventTimeExtension.isEventTimeWatermark(watermark)).isFalse();
            return WatermarkHandlingResult.PEEK;
        }
    }

    private NonKeyedPartitionStream<Tuple2<Long, String>> getSourceWithWatermarkGenerator() {
        NonKeyedPartitionStream<Tuple2<Long, String>> source =
                env.fromSource(DataStreamV2SourceUtils.fromData(inputRecords), "Source")
                        .withParallelism(1);

        return source.process(
                EventTimeExtension.<Tuple2<Long, String>>newWatermarkGeneratorBuilder(
                                event -> event.f0)
                        .perEventWatermark()
                        .buildAsProcessFunction());
    }
}
