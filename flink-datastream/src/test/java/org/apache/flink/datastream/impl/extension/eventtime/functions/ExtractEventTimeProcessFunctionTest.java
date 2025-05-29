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

package org.apache.flink.datastream.impl.extension.eventtime.functions;

import org.apache.flink.api.common.watermark.BoolWatermark;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeExtractor;
import org.apache.flink.datastream.api.extension.eventtime.strategy.EventTimeWatermarkStrategy;
import org.apache.flink.datastream.impl.operators.ProcessOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.watermark.WatermarkUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ExtractEventTimeProcessFunction}. */
public class ExtractEventTimeProcessFunctionTest {

    @Test
    void testDoNotGenerateEventTimeWatermark() throws Exception {
        EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStartegy =
                new EventTimeWatermarkStrategy<>(
                        (EventTimeExtractor<Tuple2<Long, String>>) event -> event.f0,
                        EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.NO_WATERMARK,
                        Duration.ZERO,
                        Duration.ZERO,
                        Duration.ZERO);
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
                operatorTestHarness = getOperatorTestHarness(watermarkStartegy);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345678L, "hello")));
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput());
        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345679L, "hello")));
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput());

        operatorTestHarness.close();
    }

    @Test
    void testGenerateEventTimeWatermarkPerEvent() throws Exception {
        EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStartegy =
                new EventTimeWatermarkStrategy<>(
                        (EventTimeExtractor<Tuple2<Long, String>>) event -> event.f0,
                        EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT,
                        Duration.ZERO,
                        Duration.ZERO,
                        Duration.ZERO);
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
                operatorTestHarness = getOperatorTestHarness(watermarkStartegy);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345678L, "hello")));
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345679L, "hello")));
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L, 12345679L);

        operatorTestHarness.close();
    }

    @Test
    void testGenerateEventTimeWatermarkPeriodic() throws Exception {
        EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStartegy =
                new EventTimeWatermarkStrategy<>(
                        (EventTimeExtractor<Tuple2<Long, String>>) event -> event.f0,
                        EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PERIODIC,
                        Duration.ofMillis(200),
                        Duration.ZERO,
                        Duration.ZERO);
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
                operatorTestHarness = getOperatorTestHarness(watermarkStartegy);

        operatorTestHarness.getProcessingTimeService().advance(200);
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput());

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345678L, "hello")));
        operatorTestHarness.getProcessingTimeService().advance(200);
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345679L, "hello")));
        operatorTestHarness.getProcessingTimeService().advance(200);
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L, 12345679L);

        operatorTestHarness.getProcessingTimeService().advance(1000);
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L, 12345679L);

        operatorTestHarness.close();
    }

    @Test
    void testGenerateEventTimeWatermarkWithOutOfOrderTime() throws Exception {
        EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStartegy =
                new EventTimeWatermarkStrategy<>(
                        (EventTimeExtractor<Tuple2<Long, String>>) event -> event.f0,
                        EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT,
                        Duration.ZERO,
                        Duration.ZERO,
                        Duration.ofMillis(100));
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
                operatorTestHarness = getOperatorTestHarness(watermarkStartegy);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345678L, "hello")));
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L - 100);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345679L, "hello")));
        checkOutputEventTimeWatermarks(
                operatorTestHarness.getOutput(), 12345678L - 100, 12345679L - 100);

        operatorTestHarness.close();
    }

    @Test
    void testGenerateIdleStatusWatermark() throws Exception {
        EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStartegy =
                new EventTimeWatermarkStrategy<>(
                        (EventTimeExtractor<Tuple2<Long, String>>) event -> event.f0,
                        EventTimeWatermarkStrategy.EventTimeWatermarkGenerateMode.PER_EVENT,
                        Duration.ZERO,
                        Duration.ofMillis(200),
                        Duration.ZERO);
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
                operatorTestHarness = getOperatorTestHarness(watermarkStartegy);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345678L, "hello")));
        checkOutputIdleStatusWatermarks(operatorTestHarness.getOutput());
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L);

        // simulate time advancing multiple times and thus triggering the timer multiple times,
        // otherwise {@link ExtractEventTimeProcessFunction.onProcessingTime} will get the wrong
        // time when judging the idle in the timer
        operatorTestHarness.getProcessingTimeService().advance(200);
        operatorTestHarness.getProcessingTimeService().advance(200);
        operatorTestHarness.getProcessingTimeService().advance(200);
        operatorTestHarness.getProcessingTimeService().advance(200);
        operatorTestHarness.getProcessingTimeService().advance(200);
        checkOutputIdleStatusWatermarks(operatorTestHarness.getOutput(), true);

        operatorTestHarness.processElement(new StreamRecord<>(new Tuple2<>(12345679L, "hello")));
        checkOutputIdleStatusWatermarks(operatorTestHarness.getOutput(), true, false);
        checkOutputEventTimeWatermarks(operatorTestHarness.getOutput(), 12345678L, 12345679L);

        operatorTestHarness.close();
    }

    private OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>>
            getOperatorTestHarness(
                    EventTimeWatermarkStrategy<Tuple2<Long, String>> watermarkStrategy)
                    throws Exception {
        ExtractEventTimeProcessFunction<Tuple2<Long, String>> processFunction =
                new ExtractEventTimeProcessFunction<>(watermarkStrategy);
        ProcessOperator<Tuple2<Long, String>, Tuple2<Long, String>> processOperator =
                new ProcessOperator<>(processFunction);
        OneInputStreamOperatorTestHarness<Tuple2<Long, String>, Tuple2<Long, String>> testHarness =
                new OneInputStreamOperatorTestHarness<>(processOperator);
        Set<WatermarkDeclaration> watermarkDeclarations =
                (Set<WatermarkDeclaration>) processFunction.declareWatermarks();
        byte[] serializedWatermarkDeclarations =
                InstantiationUtil.serializeObject(
                        WatermarkUtils.convertToInternalWatermarkDeclarations(
                                watermarkDeclarations));
        testHarness.getStreamConfig().setWatermarkDeclarations(serializedWatermarkDeclarations);
        testHarness.open();
        return testHarness;
    }

    private void checkOutputEventTimeWatermarks(
            ConcurrentLinkedQueue<Object> output, Long... expectedEventTimeWatermarks) {
        List<Long> actualEventTimeWatermarks =
                output.stream()
                        .filter(
                                object ->
                                        object instanceof WatermarkEvent
                                                && EventTimeExtension.isEventTimeWatermark(
                                                        ((WatermarkEvent) object).getWatermark()))
                        .map(
                                watermarkEvent ->
                                        ((LongWatermark)
                                                        ((WatermarkEvent) watermarkEvent)
                                                                .getWatermark())
                                                .getValue())
                        .collect(Collectors.toList());
        assertThat(actualEventTimeWatermarks).containsExactly(expectedEventTimeWatermarks);
    }

    private void checkOutputIdleStatusWatermarks(
            ConcurrentLinkedQueue<Object> output, Boolean... expectedIdleStatusWatermarks) {
        List<Boolean> actualEventTimeWatermarks =
                output.stream()
                        .filter(
                                object ->
                                        object instanceof WatermarkEvent
                                                && EventTimeExtension.isIdleStatusWatermark(
                                                        ((WatermarkEvent) object).getWatermark()))
                        .map(
                                watermarkEvent ->
                                        ((BoolWatermark)
                                                        ((WatermarkEvent) watermarkEvent)
                                                                .getWatermark())
                                                .getValue())
                        .collect(Collectors.toList());
        assertThat(actualEventTimeWatermarks).containsExactly(expectedIdleStatusWatermarks);
    }
}
