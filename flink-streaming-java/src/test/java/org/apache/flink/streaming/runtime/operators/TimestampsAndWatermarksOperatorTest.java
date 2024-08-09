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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;

import static org.apache.flink.streaming.util.StreamRecordMatchers.streamRecord;
import static org.apache.flink.streaming.util.WatermarkMatchers.legacyWatermark;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for {@link TimestampsAndWatermarksOperator}. */
class TimestampsAndWatermarksOperatorTest {

    private static final long AUTO_WATERMARK_INTERVAL = 50L;

    @Test
    void inputWatermarksAreNotForwarded() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processWatermark(createLegacyWatermark(42L));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void inputStatusesAreNotForwarded() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processWatermarkStatus(WatermarkStatus.IDLE);
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void longMaxInputWatermarkIsForwarded() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processWatermark(createLegacyWatermark(Long.MAX_VALUE));

        assertThat(pollNextLegacyWatermark(testHarness))
                .is(matching(legacyWatermark(Long.MAX_VALUE)));
    }

    @Test
    void periodicWatermarksEmitOnPeriodicEmitStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(2L, 2L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(1L)));

        testHarness.processElement(new StreamRecord<>(4L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL * 2);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(4L, 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(3L)));
    }

    @Test
    void periodicWatermarksBatchMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createBatchHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(2L, 2L)));
        assertThat(pollNextLegacyWatermark(testHarness)).isNull();

        testHarness.processElement(new StreamRecord<>(4L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL * 2);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(4L, 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).isNull();
    }

    @Test
    void periodicWatermarksOnlyEmitOnPeriodicEmitStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(2L, 1));

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(2L, 2L)));
        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void periodicWatermarksDoNotRegressStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        testHarness.processElement(new StreamRecord<>(4L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(4L, 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(3L)));

        testHarness.processElement(new StreamRecord<>(2L, 1));
        testHarness.setProcessingTime(AUTO_WATERMARK_INTERVAL);

        assertThat(pollNextStreamRecord(testHarness)).is(matching(streamRecord(2L, 2L)));
        assertThat(testHarness.getOutput()).isEmpty();
    }

    @Test
    void punctuatedWatermarksEmitImmediatelyStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness =
                        createTestHarness(
                                WatermarkStrategy.forGenerator(
                                                (ctx) -> new PunctuatedWatermarkGenerator())
                                        .withTimestampAssigner((ctx) -> new TupleExtractor()));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 2L), 2L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(2L)));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 4L), 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(4L)));
    }

    @Test
    void punctuatedWatermarksBatchMode() throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness =
                        createBatchHarness(
                                WatermarkStrategy.forGenerator(
                                                (ctx) -> new PunctuatedWatermarkGenerator())
                                        .withTimestampAssigner((ctx) -> new TupleExtractor()));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 2L), 2L)));
        assertThat(pollNextLegacyWatermark(testHarness)).isNull();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 4L), 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).isNull();
    }

    @Test
    void punctuatedWatermarksDoNotRegressStreamMode() throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness =
                        createTestHarness(
                                WatermarkStrategy.forGenerator(
                                                (ctx) -> new PunctuatedWatermarkGenerator())
                                        .withTimestampAssigner((ctx) -> new TupleExtractor()));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 4L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 4L), 4L)));
        assertThat(pollNextLegacyWatermark(testHarness)).is(matching(legacyWatermark(4L)));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>(true, 2L), 1));

        assertThat(pollNextStreamRecord(testHarness))
                .is(matching(streamRecord(new Tuple2<>(true, 2L), 2L)));
        assertThat(testHarness.getOutput()).isEmpty();
    }

    /** Negative timestamps also must be correctly forwarded. */
    @Test
    void testNegativeTimestamps() throws Exception {

        OneInputStreamOperatorTestHarness<Long, Long> testHarness =
                createTestHarness(
                        WatermarkStrategy.forGenerator((ctx) -> new NeverWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new LongExtractor()));

        long[] values = {Long.MIN_VALUE, -1L, 0L, 1L, 2L, 3L, Long.MAX_VALUE};

        for (long value : values) {
            testHarness.processElement(new StreamRecord<>(value));
        }

        for (long value : values) {
            assertThat(pollNextStreamRecord(testHarness).getTimestamp()).isEqualTo(value);
        }
    }

    @Test
    void watermarksWithIdlenessUnderBackpressure() throws Exception {
        long idleTimeout = 100;

        TimestampsAndWatermarksOperator<Tuple2<Boolean, Long>> operator =
                new TimestampsAndWatermarksOperator<>(
                        WatermarkStrategy.forGenerator((ctx) -> new PunctuatedWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new TupleExtractor())
                                .withIdleness(Duration.ofMillis(idleTimeout)),
                        true);

        OneInputStreamOperatorTestHarness<Tuple2<Boolean, Long>, Tuple2<Boolean, Long>>
                testHarness = new OneInputStreamOperatorTestHarness<>(operator);
        testHarness.open();

        TaskIOMetricGroup taskIOMetricGroup =
                testHarness.getEnvironment().getMetricGroup().getIOMetricGroup();
        taskIOMetricGroup.getHardBackPressuredTimePerSecond().markStart();

        for (int i = 0; i < 10; i++) {
            testHarness.advanceTime(idleTimeout);
        }
        assertThat(testHarness.getOutput()).isEmpty();

        taskIOMetricGroup.getHardBackPressuredTimePerSecond().markEnd();
        taskIOMetricGroup.getSoftBackPressuredTimePerSecond().markStart();

        for (int i = 10; i < 20; i++) {
            testHarness.advanceTime(idleTimeout);
        }
        assertThat(testHarness.getOutput()).isEmpty();

        taskIOMetricGroup.getSoftBackPressuredTimePerSecond().markEnd();

        for (int i = 20; i < 30; i++) {
            testHarness.advanceTime(idleTimeout);
        }
        assertThat(testHarness.getOutput()).containsExactly(WatermarkStatus.IDLE);
    }

    private static <T> OneInputStreamOperatorTestHarness<T, T> createTestHarness(
            WatermarkStrategy<T> watermarkStrategy) throws Exception {

        final TimestampsAndWatermarksOperator<T> operator =
                new TimestampsAndWatermarksOperator<>(watermarkStrategy, true);

        OneInputStreamOperatorTestHarness<T, T> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.getExecutionConfig().setAutoWatermarkInterval(AUTO_WATERMARK_INTERVAL);

        testHarness.open();

        return testHarness;
    }

    private static <T> OneInputStreamOperatorTestHarness<T, T> createBatchHarness(
            WatermarkStrategy<T> watermarkStrategy) throws Exception {

        final TimestampsAndWatermarksOperator<T> operator =
                new TimestampsAndWatermarksOperator<>(watermarkStrategy, false);

        OneInputStreamOperatorTestHarness<T, T> testHarness =
                new OneInputStreamOperatorTestHarness<>(operator);

        testHarness.open();

        return testHarness;
    }

    @SuppressWarnings("unchecked")
    private static <T> StreamRecord<T> pollNextStreamRecord(
            OneInputStreamOperatorTestHarness<?, T> testHarness) {
        return (StreamRecord<T>) testHarness.getOutput().poll();
    }

    private static org.apache.flink.streaming.api.watermark.Watermark pollNextLegacyWatermark(
            OneInputStreamOperatorTestHarness<?, ?> testHarness) {
        return (org.apache.flink.streaming.api.watermark.Watermark) testHarness.getOutput().poll();
    }

    private static org.apache.flink.streaming.api.watermark.Watermark createLegacyWatermark(
            long timestamp) {
        return new org.apache.flink.streaming.api.watermark.Watermark(timestamp);
    }

    private static class LongExtractor implements TimestampAssigner<Long> {
        @Override
        public long extractTimestamp(Long element, long recordTimestamp) {
            return element;
        }
    }

    private static class TupleExtractor implements TimestampAssigner<Tuple2<Boolean, Long>> {
        @Override
        public long extractTimestamp(Tuple2<Boolean, Long> element, long recordTimestamp) {
            return element.f1;
        }
    }

    /**
     * A {@link WatermarkGenerator} that doesn't enforce the watermark invariant by itself. If a
     * record with a lower timestamp than the previous high timestamp comes in the output watermark
     * regressed.
     */
    private static class PeriodicWatermarkGenerator
            implements WatermarkGenerator<Long>, Serializable {

        private long currentWatermark = Long.MIN_VALUE;

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
            currentWatermark = eventTimestamp;
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            long effectiveWatermark =
                    currentWatermark == Long.MIN_VALUE ? Long.MIN_VALUE : currentWatermark - 1;
            output.emitWatermark(new Watermark(effectiveWatermark));
        }
    }

    /**
     * A {@link WatermarkGenerator} that doesn't enforce the watermark invariant by itself. If a
     * record with a lower timestamp than the previous high timestamp comes in the output watermark
     * regressed.
     */
    private static class PunctuatedWatermarkGenerator
            implements WatermarkGenerator<Tuple2<Boolean, Long>>, Serializable {
        @Override
        public void onEvent(
                Tuple2<Boolean, Long> event, long eventTimestamp, WatermarkOutput output) {
            if (event.f0) {
                output.emitWatermark(new Watermark(event.f1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }

    private static class NeverWatermarkGenerator implements WatermarkGenerator<Long>, Serializable {

        @Override
        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {}

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {}
    }
}
