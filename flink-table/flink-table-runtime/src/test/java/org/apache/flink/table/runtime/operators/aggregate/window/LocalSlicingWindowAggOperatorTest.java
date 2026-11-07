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

package org.apache.flink.table.runtime.operators.aggregate.window;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.source.CollectingDataOutput;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.aggregate.window.SlicingWindowAggOperatorTest.SlicingSumAndCountAggsFunction;
import org.apache.flink.table.runtime.operators.aggregate.window.buffers.RecordsWindowBuffer;
import org.apache.flink.table.runtime.operators.aggregate.window.combines.LocalAggCombiner;
import org.apache.flink.table.runtime.operators.window.tvf.common.WindowAggOperator;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.tvf.slicing.SliceAssigners;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataTestUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.apache.flink.table.data.TimestampData.fromEpochMillis;
import static org.apache.flink.table.runtime.operators.aggregate.window.WindowAggOperatorTestBase.createGeneratedAggsHandle;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link LocalSlicingWindowAggOperator}. */
class LocalSlicingWindowAggOperatorTest {

    private static final LogicalType[] INPUT_TYPES = {
        VarCharType.STRING_TYPE, new IntType(), new TimestampType(3)
    };
    private static final LogicalType[] OUTPUT_TYPES = {
        VarCharType.STRING_TYPE, new BigIntType(), new BigIntType(), new BigIntType()
    };
    private static final SliceAssigner ASSIGNER =
            SliceAssigners.tumbling(2, ZoneOffset.UTC, Duration.ofSeconds(10));

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFlushBeforeIdle(boolean checkpointBeforeIdle) throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness()) {
            harness.processElement(insertRecord("key", 100, fromEpochMillis(1000)));
            harness.processElement(insertRecord("key", 1, fromEpochMillis(2000)));
            harness.processWatermark(new Watermark(0));
            assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(0));
            assertThat(harness.getOutput()).isEmpty();

            if (checkpointBeforeIdle) {
                harness.prepareSnapshotPreBarrier(1);
            }
            harness.processWatermarkStatus(WatermarkStatus.IDLE);

            assertRecord(harness, "key", 101L, 2L, 10000L);
            assertThat(harness.getOutput().poll()).isEqualTo(WatermarkStatus.IDLE);
            assertThat(harness.getOutput()).isEmpty();

            harness.prepareSnapshotPreBarrier(2);
            harness.processWatermark(new Watermark(9999));
            assertThat(harness.getOutput()).containsExactly(new Watermark(9999));
        }
    }

    @Test
    void testFlushAllWindowsBeforeIdle() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness()) {
            harness.processElement(insertRecord("key", 10, fromEpochMillis(1000)));
            harness.processElement(insertRecord("key", 20, fromEpochMillis(11000)));
            harness.processWatermarkStatus(WatermarkStatus.IDLE);

            assertThat(harness.getOutput()).hasSize(3);
            assertThat(Arrays.asList(pollRecord(harness), pollRecord(harness)))
                    .containsExactlyInAnyOrder(
                            insertRecord("key", 10L, 1L, 10000L).getValue(),
                            insertRecord("key", 20L, 1L, 20000L).getValue());
            assertThat(harness.getOutput().poll()).isEqualTo(WatermarkStatus.IDLE);
        }
    }

    @Test
    void testActiveDoesNotFlushAndNextIdleFlushesNewRecords() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness()) {
            harness.processElement(insertRecord("key", 10, fromEpochMillis(1000)));
            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            assertRecord(harness, "key", 10L, 1L, 10000L);
            assertThat(harness.getOutput().poll()).isEqualTo(WatermarkStatus.IDLE);

            harness.processElement(insertRecord("key", 20, fromEpochMillis(2000)));
            harness.processWatermarkStatus(WatermarkStatus.ACTIVE);
            assertThat(harness.getOutput().poll()).isEqualTo(WatermarkStatus.ACTIVE);
            assertThat(harness.getOutput()).isEmpty();

            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            assertRecord(harness, "key", 20L, 1L, 10000L);
            assertThat(harness.getOutput().poll()).isEqualTo(WatermarkStatus.IDLE);
            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);
        }
    }

    @Test
    void testEmptyBufferAndRepeatedIdle() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness()) {
            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            assertThat(harness.getOutput())
                    .containsExactly(WatermarkStatus.IDLE, WatermarkStatus.IDLE);
        }
    }

    @Test
    void testFlushFailureDoesNotForwardIdle() throws Exception {
        final SlicingSumAndCountAggsFunction aggregator =
                new SlicingSumAndCountAggsFunction(ASSIGNER) {
                    @Override
                    public void accumulate(RowData inputRow) throws Exception {
                        throw new IOException("Failed to aggregate buffered records");
                    }
                };
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(aggregator)) {
            harness.processElement(insertRecord("key", 101, fromEpochMillis(1000)));
            assertThatThrownBy(() -> harness.processWatermarkStatus(WatermarkStatus.IDLE))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Failed to aggregate buffered records");
            assertThat(harness.getOutput()).isEmpty();
        }
    }

    @Test
    void testIdleChannelContributesBeforeGlobalWindowFires() throws Exception {
        final StatusWatermarkValve valve = new StatusWatermarkValve(2);
        final CollectingDataOutput<RowData> valveOutput = new CollectingDataOutput<>();
        try (OneInputStreamOperatorTestHarness<RowData, RowData> active = createHarness();
                OneInputStreamOperatorTestHarness<RowData, RowData> idle = createHarness();
                OneInputStreamOperatorTestHarness<RowData, RowData> global =
                        createGlobalHarness()) {
            idle.processElement(insertRecord("key", 101, fromEpochMillis(1000)));
            idle.processWatermark(new Watermark(0));
            forwardOutput(idle, 1, valve, valveOutput, global);

            active.processElement(insertRecord("key", 10, fromEpochMillis(1000)));
            active.processElement(insertRecord("key", 10, fromEpochMillis(4000)));
            active.processElement(insertRecord("key", 10, fromEpochMillis(8000)));
            active.processWatermark(new Watermark(13000));
            forwardOutput(active, 0, valve, valveOutput, global);
            assertThat(global.getOutput()).containsExactly(new Watermark(0));
            global.getOutput().clear();

            idle.processWatermarkStatus(WatermarkStatus.IDLE);
            forwardOutput(idle, 1, valve, valveOutput, global);
            WindowAggOperatorTestBase.ASSERTER.assertOutputEquals(
                    "The idle channel must contribute before the other channel fires the window.",
                    Arrays.asList(insertRecord("key", 131L, 4L, 0L, 10000L), new Watermark(13000)),
                    global.getOutput());
            global.getOutput().clear();

            idle.prepareSnapshotPreBarrier(1);
            forwardOutput(idle, 1, valve, valveOutput, global);
            assertThat(global.getOutput()).isEmpty();
            assertThat(
                            ((WindowAggOperator<?, ?>) global.getOperator())
                                    .getNumLateRecordsDropped()
                                    .getCount())
                    .isZero();
        }
    }

    @Test
    void testWatermarkStillFlushesBeforeIdle() throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness = createHarness()) {
            harness.processElement(insertRecord("key", 101, fromEpochMillis(1000)));
            harness.processWatermark(new Watermark(9999));
            assertRecord(harness, "key", 101L, 1L, 10000L);
            assertThat(harness.getOutput().poll()).isEqualTo(new Watermark(9999));
            harness.processWatermarkStatus(WatermarkStatus.IDLE);
            assertThat(harness.getOutput()).containsExactly(WatermarkStatus.IDLE);
        }
    }    

    @Test
    void testRepeatedIdleContributionsSurviveRestoreWithoutFiringEarly() throws Exception {
        final StatusWatermarkValve valve = new StatusWatermarkValve(2);
        final CollectingDataOutput<RowData> valveOutput = new CollectingDataOutput<>();
        final OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<RowData, RowData> active = createHarness();
                OneInputStreamOperatorTestHarness<RowData, RowData> idle = createHarness();
                OneInputStreamOperatorTestHarness<RowData, RowData> global =
                        createGlobalHarness()) {
            active.processWatermark(new Watermark(0));
            forwardOutput(active, 0, valve, valveOutput, global);
            idle.processWatermark(new Watermark(0));
            forwardOutput(idle, 1, valve, valveOutput, global);
            assertThat(global.getOutput()).containsExactly(new Watermark(0));
            global.getOutput().clear();

            idle.processElement(insertRecord("key", 10, fromEpochMillis(1000)));
            idle.processElement(insertRecord("future", 100, fromEpochMillis(11000)));
            idle.processWatermarkStatus(WatermarkStatus.IDLE);
            forwardOutput(idle, 1, valve, valveOutput, global);
            assertThat(global.getOutput()).isEmpty();

            idle.processWatermarkStatus(WatermarkStatus.ACTIVE);
            idle.processElement(insertRecord("key", 20, fromEpochMillis(2000)));
            idle.processWatermarkStatus(WatermarkStatus.IDLE);
            forwardOutput(idle, 1, valve, valveOutput, global);
            idle.prepareSnapshotPreBarrier(1);
            assertThat(idle.getOutput()).isEmpty();
            global.prepareSnapshotPreBarrier(1);
            snapshot = global.snapshot(1, 0);
            assertThat(global.getOutput()).isEmpty();
        }
        try (OneInputStreamOperatorTestHarness<RowData, RowData> restored =
                createGlobalHarness(snapshot)) {
            restored.processWatermark(new Watermark(9999));
            WindowAggOperatorTestBase.ASSERTER.assertOutputEquals(
                    "Idle contributions must survive restore and wait for the window watermark.",
                    Arrays.asList(insertRecord("key", 30L, 2L, 0L, 10000L), new Watermark(9999)),
                    restored.getOutput());
            restored.getOutput().clear();
            restored.processWatermark(new Watermark(19999));
            WindowAggOperatorTestBase.ASSERTER.assertOutputEquals(
                    "The future window must fire once at its own boundary.",
                    Arrays.asList(
                            insertRecord("future", 100L, 1L, 10000L, 20000L), new Watermark(19999)),
                    restored.getOutput());
        }
    }

    @Test
    void testPartialFlushFailureDoesNotForwardIdle() throws Exception {
        final SlicingSumAndCountAggsFunction aggregator =
                new SlicingSumAndCountAggsFunction(ASSIGNER) {
                    private int accumulated;

                    @Override
                    public void accumulate(RowData inputRow) throws Exception {
                        if (++accumulated == 2) {
                            throw new IOException("Failed after emitting one accumulator");
                        }
                        super.accumulate(inputRow);
                    }
                };
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(aggregator)) {
            harness.processElement(insertRecord("first", 10, fromEpochMillis(1000)));
            harness.processElement(insertRecord("second", 101, fromEpochMillis(1000)));
            assertThatThrownBy(() -> harness.processWatermarkStatus(WatermarkStatus.IDLE))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Failed after emitting one accumulator");
            assertThat(harness.getOutput()).hasSize(1);
            assertThat(harness.getOutput().poll()).isInstanceOf(StreamRecord.class);
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createGlobalHarness()
            throws Exception {
        return createGlobalHarness(null);
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createGlobalHarness(
            @Nullable OperatorSubtaskState snapshot) throws Exception {
        final RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(new int[] {0}, OUTPUT_TYPES);
        final SlicingSumAndCountAggsFunction localMerger =
                new SlicingSumAndCountAggsFunction(ASSIGNER) {
                    @Override
                    public void merge(Long window, RowData otherAcc) throws Exception {
                        super.merge(
                                window,
                                GenericRowData.of(otherAcc.getLong(1), otherAcc.getLong(2)));
                    }
                };
        final OneInputStreamOperator<RowData, RowData> operator =
                WindowAggOperatorBuilder.builder()
                        .inputSerializer(new RowDataSerializer(OUTPUT_TYPES))
                        .keySerializer(
                                (PagedTypeSerializer<RowData>)
                                        keySelector.getProducedType().toSerializer())
                        .assigner(SliceAssigners.sliced(3, ASSIGNER))
                        .shiftTimeZone(ZoneOffset.UTC)
                        .globalAggregate(
                                createGeneratedAggsHandle(localMerger),
                                createGeneratedAggsHandle(
                                        new SlicingSumAndCountAggsFunction(ASSIGNER)),
                                createGeneratedAggsHandle(
                                        new SlicingSumAndCountAggsFunction(ASSIGNER)),
                                new RowDataSerializer(new BigIntType(), new BigIntType()))
                        .build();
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector.getProducedType());
        harness.setup(WindowAggOperatorTestBase.OUT_SERIALIZER);
        if (snapshot != null) {
            harness.initializeState(snapshot);
        }
        harness.open();
        return harness;
    }

    private static void forwardOutput(
            OneInputStreamOperatorTestHarness<RowData, RowData> local,
            int channel,
            StatusWatermarkValve valve,
            CollectingDataOutput<RowData> valveOutput,
            OneInputStreamOperatorTestHarness<RowData, RowData> global)
            throws Exception {
        final RowDataSerializer serializer = new RowDataSerializer(OUTPUT_TYPES);
        Object event;
        while ((event = local.getOutput().poll()) != null) {
            if (event instanceof StreamRecord) {
                // Match the binary rows delivered by the exchange between local and global.
                global.processElement(
                        new StreamRecord<>(
                                serializer
                                        .toBinaryRow((RowData) ((StreamRecord<?>) event).getValue())
                                        .copy()));
            } else if (event instanceof Watermark) {
                valve.inputWatermark((Watermark) event, channel, valveOutput);
            } else {
                assertThat(event).isInstanceOf(WatermarkStatus.class);
                valve.inputWatermarkStatus((WatermarkStatus) event, channel, valveOutput);
            }
            for (Object output : valveOutput.getEvents()) {
                if (output instanceof Watermark) {
                    global.processWatermark((Watermark) output);
                } else {
                    global.processWatermarkStatus((WatermarkStatus) output);
                }
            }
            valveOutput.getEvents().clear();
        }
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness()
            throws Exception {
        return createHarness(new SlicingSumAndCountAggsFunction(ASSIGNER));
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createHarness(
            SlicingSumAndCountAggsFunction aggregator) throws Exception {
        final RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(new int[] {0}, INPUT_TYPES);
        final LocalSlicingWindowAggOperator operator =
                new LocalSlicingWindowAggOperator(
                        keySelector,
                        ASSIGNER,
                        new RecordsWindowBuffer.LocalFactory(
                                (PagedTypeSerializer<RowData>)
                                        keySelector.getProducedType().toSerializer(),
                                new RowDataSerializer(INPUT_TYPES),
                                new LocalAggCombiner.Factory(
                                        createGeneratedAggsHandle(aggregator))),
                        ZoneOffset.UTC);
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                new OneInputStreamOperatorTestHarness<>(operator);
        harness.setup(new RowDataSerializer(OUTPUT_TYPES));
        harness.open();
        return harness;
    }

    private static void assertRecord(
            OneInputStreamOperatorTestHarness<RowData, RowData> harness, Object... fields) {
        assertThat(pollRecord(harness)).isEqualTo(insertRecord(fields).getValue());
    }

    private static RowData pollRecord(OneInputStreamOperatorTestHarness<RowData, RowData> harness) {
        final Object output = harness.getOutput().poll();
        assertThat(output).isInstanceOf(StreamRecord.class);
        return RowDataTestUtil.toGenericRowDeeply(
                (RowData) ((StreamRecord<?>) output).getValue(), OUTPUT_TYPES);
    }
}
