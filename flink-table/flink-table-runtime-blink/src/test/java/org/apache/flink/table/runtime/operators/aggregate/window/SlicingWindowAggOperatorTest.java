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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.dataview.StateDataViewStore;
import org.apache.flink.table.runtime.generated.GeneratedNamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.generated.NamespaceAggsHandleFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigner;
import org.apache.flink.table.runtime.operators.window.slicing.SliceAssigners;
import org.apache.flink.table.runtime.operators.window.slicing.SlicingWindowOperator;
import org.apache.flink.table.runtime.typeutils.PagedTypeSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for window aggregate operators created by {@link SlicingWindowAggOperatorBuilder}. */
@RunWith(Parameterized.class)
public class SlicingWindowAggOperatorTest {

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    private static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");
    private final ZoneId shiftTimeZone;

    public SlicingWindowAggOperatorTest(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    private static final RowType INPUT_ROW_TYPE =
            new RowType(
                    Arrays.asList(
                            new RowType.RowField("f0", new VarCharType(Integer.MAX_VALUE)),
                            new RowType.RowField("f1", new IntType()),
                            new RowType.RowField("f2", new BigIntType())));

    private static final RowDataSerializer INPUT_ROW_SER = new RowDataSerializer(INPUT_ROW_TYPE);

    private static final RowDataSerializer ACC_SER =
            new RowDataSerializer(new BigIntType(), new BigIntType());

    private static final LogicalType[] OUTPUT_TYPES =
            new LogicalType[] {
                new VarCharType(Integer.MAX_VALUE),
                new BigIntType(),
                new BigIntType(),
                new BigIntType(),
                new BigIntType()
            };

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0}, INPUT_ROW_TYPE.getChildren().toArray(new LogicalType[0]));

    private static final PagedTypeSerializer<RowData> KEY_SER =
            (PagedTypeSerializer<RowData>) KEY_SELECTOR.getProducedType().toSerializer();

    private static final TypeSerializer<RowData> OUT_SERIALIZER =
            new RowDataSerializer(OUTPUT_TYPES);

    private static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(
                    OUTPUT_TYPES,
                    new GenericRowRecordSortComparator(0, new VarCharType(VarCharType.MAX_LENGTH)));

    @Test
    public void testEventTimeHoppingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .countStarIndex(1)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processElement(insertRecord("key2", 1, 3000L));

        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key1", 1, 0L));
        testHarness.processElement(insertRecord("key1", 1, 999L));

        testHarness.processElement(insertRecord("key2", 1, 1998L));
        testHarness.processElement(insertRecord("key2", 1, 1999L));
        testHarness.processElement(insertRecord("key2", 1, 1000L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-2000L), localMills(1000L)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(-1000L), localMills(2000L)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertTrue("Close was not called.", aggsFunction.closeCalled.get() > 0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup(OUT_SERIALIZER);
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 5L, 5L, localMills(1000L), localMills(4000L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element for [1K, 4K), but should be accumulated into [2K, 5K), [3K, 6K)
        testHarness.processElement(insertRecord("key2", 1, 3500L));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(2000L), localMills(5000L)));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, 2999L));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(1, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }

    @Test
    public void testProcessingTimeHoppingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.hopping(-1, shiftTimeZone, Duration.ofHours(3), Duration.ofHours(1));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .countStarIndex(1)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T01:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1969-12-31T22:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T02:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1969-12-31T23:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T02:00:00")));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T03:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T07:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T04:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        5L,
                        5L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T01:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T04:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        5L,
                        5L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T02:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T03:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T06:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
        assertTrue("Close was not called.", aggsFunction.closeCalled.get() > 0);
    }

    @Test
    public void testEventTimeCumulativeWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.cumulative(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 2999L));
        testHarness.processElement(insertRecord("key2", 1, 3000L));

        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key1", 1, 0L));
        testHarness.processElement(insertRecord("key1", 1, 999L));

        testHarness.processElement(insertRecord("key2", 1, 1998L));
        testHarness.processElement(insertRecord("key2", 1, 1999L));
        testHarness.processElement(insertRecord("key2", 1, 1000L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(1000L)));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(2000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(2000L)));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 4L, 4L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertTrue("Close was not called.", aggsFunction.closeCalled.get() > 0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(4000L)));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element for [3K, 4K), but should be accumulated into [3K, 5K) [3K, 6K)
        testHarness.processElement(insertRecord("key1", 2, 3500L));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(5000L)));
        expectedOutput.add(insertRecord("key1", 2L, 1L, localMills(3000L), localMills(5000L)));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late for all assigned windows, should be dropped
        testHarness.processElement(insertRecord("key1", 1, 2999L));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 1L, 1L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(insertRecord("key1", 2L, 1L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(1, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }

    @Test
    public void testProcessingTimeCumulativeWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.cumulative(
                        -1, shiftTimeZone, Duration.ofDays(1), Duration.ofHours(8));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T08:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T08:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T16:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T16:00:00")));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-02T00:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key1", 1, Long.MAX_VALUE));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-03T08:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T08:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T08:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T16:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-02T16:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-03T00:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key2",
                        1L,
                        1L,
                        epochMills(UTC_ZONE_ID, "1970-01-02T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-03T00:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
        assertTrue("Close was not called.", aggsFunction.closeCalled.get() > 0);
    }

    @Test
    public void testEventTimeTumblingWindows() throws Exception {
        final SliceAssigner assigner =
                SliceAssigners.tumbling(2, shiftTimeZone, Duration.ofSeconds(3));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(insertRecord("key2", 1, 3999L));
        testHarness.processElement(insertRecord("key2", 1, 3000L));

        testHarness.processElement(insertRecord("key1", 1, 20L));
        testHarness.processElement(insertRecord("key1", 1, 0L));
        testHarness.processElement(insertRecord("key1", 1, 999L));

        testHarness.processElement(insertRecord("key2", 1, 1998L));
        testHarness.processElement(insertRecord("key2", 1, 1999L));
        testHarness.processElement(insertRecord("key2", 1, 1000L));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // do a snapshot, close and restore again
        testHarness.prepareSnapshotPreBarrier(0L);
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0);
        testHarness.close();

        assertTrue("Close was not called.", aggsFunction.closeCalled.get() > 0);

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(insertRecord("key1", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(insertRecord("key2", 3L, 3L, localMills(0L), localMills(3000L)));
        expectedOutput.add(new Watermark(2999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new Watermark(3999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(insertRecord("key1", 1, 2500L));

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // late element, should be dropped
        testHarness.processElement(insertRecord("key2", 1, 2999L));

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(insertRecord("key2", 2L, 2L, localMills(3000L), localMills(6000L)));
        expectedOutput.add(new Watermark(5999));
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        assertEquals(2, operator.getNumLateRecordsDropped().getCount());

        testHarness.close();
    }

    @Test
    public void testProcessingTimeTumblingWindows() throws Exception {

        final SliceAssigner assigner =
                SliceAssigners.tumbling(-1, shiftTimeZone, Duration.ofHours(5));
        // the assigned windows should like as following, e.g. the given timeZone is GMT+08:00:
        //  local windows(timestamp in GMT+08:00)   <=>  epoch windows(timestamp in UTC)
        // [1970-01-01 00:00, 1970-01-01 05:00] <=> [1969-12-31 16:00, 1969-12-31 21:00]
        // [1970-01-01 05:00, 1970-01-01 10:00] <=> [1969-12-31 21:00, 1970-01-01 02:00]

        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);
        SlicingWindowOperator<RowData, ?> operator =
                SlicingWindowAggOperatorBuilder.builder()
                        .inputSerializer(INPUT_ROW_SER)
                        .shiftTimeZone(shiftTimeZone)
                        .keySerializer(KEY_SER)
                        .assigner(assigner)
                        .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                        .build();

        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(operator);

        testHarness.setup(OUT_SERIALIZER);
        testHarness.open();

        // process elements
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T00:00:00.003"));

        // timestamp is ignored in processing time
        testHarness.processElement(insertRecord("key2", 1, Long.MAX_VALUE));
        testHarness.processElement(insertRecord("key2", 1, 7000L));
        testHarness.processElement(insertRecord("key2", 1, 7000L));

        testHarness.processElement(insertRecord("key1", 1, 7000L));
        testHarness.processElement(insertRecord("key1", 1, 7000L));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T05:00:00"));

        expectedOutput.add(
                insertRecord(
                        "key2",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));
        expectedOutput.add(
                insertRecord(
                        "key1",
                        2L,
                        2L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T00:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00")));

        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(insertRecord("key1", 1, 7000L));
        testHarness.processElement(insertRecord("key1", 1, 7000L));
        testHarness.processElement(insertRecord("key1", 1, 7000L));

        testHarness.setProcessingTime(epochMills(shiftTimeZone, "1970-01-01T10:00:01"));

        expectedOutput.add(
                insertRecord(
                        "key1",
                        3L,
                        3L,
                        epochMills(UTC_ZONE_ID, "1970-01-01T05:00:00"),
                        epochMills(UTC_ZONE_ID, "1970-01-01T10:00:00")));

        assertEquals(Long.valueOf(0L), operator.getWatermarkLatency().getValue());
        ASSERTER.assertOutputEqualsSorted(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testInvalidWindows() {
        final SliceAssigner assigner =
                SliceAssigners.hopping(
                        2, shiftTimeZone, Duration.ofSeconds(3), Duration.ofSeconds(1));
        final SumAndCountAggsFunction aggsFunction = new SumAndCountAggsFunction(assigner);

        try {
            // hopping window without specifying count star index
            SlicingWindowAggOperatorBuilder.builder()
                    .inputSerializer(INPUT_ROW_SER)
                    .shiftTimeZone(shiftTimeZone)
                    .keySerializer(KEY_SER)
                    .assigner(assigner)
                    .aggregate(wrapGenerated(aggsFunction), ACC_SER)
                    .build();
            fail("should fail");
        } catch (Exception e) {
            assertThat(
                    e,
                    containsMessage(
                            "Hopping window requires a COUNT(*) in the aggregate functions."));
        }
    }

    /** Get the timestamp in mills by given epoch mills and timezone. */
    private long localMills(long epochMills) {
        return toUtcTimestampMills(epochMills, shiftTimeZone);
    }

    private static OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            SlicingWindowOperator<RowData, ?> operator) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, KEY_SELECTOR, KEY_SELECTOR.getProducedType());
    }

    private static GeneratedNamespaceAggsHandleFunction<Long> wrapGenerated(
            NamespaceAggsHandleFunction<Long> aggsFunction) {
        return new GeneratedNamespaceAggsHandleFunction<Long>("N/A", "N/A", new Object[0]) {
            private static final long serialVersionUID = 1L;

            @Override
            public NamespaceAggsHandleFunction<Long> newInstance(ClassLoader classLoader) {
                return aggsFunction;
            }
        };
    }

    /**
     * This performs a {@code SUM(f1), COUNT(f1)}, where f1 is BIGINT type. The return value
     * contains {@code sum, count, window_start, window_end}.
     */
    private static class SumAndCountAggsFunction implements NamespaceAggsHandleFunction<Long> {

        private static final long serialVersionUID = 1L;

        private final SliceAssigner assigner;

        boolean openCalled;
        final AtomicInteger closeCalled = new AtomicInteger(0);

        long sum;
        boolean sumIsNull;
        long count;
        boolean countIsNull;

        protected transient JoinedRowData result;

        private SumAndCountAggsFunction(SliceAssigner assigner) {
            this.assigner = assigner;
        }

        public void open(StateDataViewStore store) throws Exception {
            openCalled = true;
            result = new JoinedRowData();
        }

        public void setAccumulators(Long window, RowData acc) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            sumIsNull = acc.isNullAt(0);
            if (!sumIsNull) {
                sum = acc.getLong(0);
            } else {
                sum = 0L;
            }

            countIsNull = acc.isNullAt(1);
            if (!countIsNull) {
                count = acc.getLong(1);
            } else {
                count = 0L;
            }
        }

        public void accumulate(RowData inputRow) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean inputIsNull = inputRow.isNullAt(1);
            if (!inputIsNull) {
                sum += inputRow.getInt(1);
                count += 1;
                sumIsNull = false;
                countIsNull = false;
            }
        }

        public void retract(RowData inputRow) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean inputIsNull = inputRow.isNullAt(1);
            if (!inputIsNull) {
                sum -= inputRow.getInt(1);
                count -= 1;
            }
        }

        public void merge(Long window, RowData otherAcc) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            boolean sumIsNull2 = otherAcc.isNullAt(0);
            if (!sumIsNull2) {
                sum += otherAcc.getLong(0);
                sumIsNull = false;
            }
            boolean countIsNull2 = otherAcc.isNullAt(1);
            if (!countIsNull2) {
                count += otherAcc.getLong(1);
                countIsNull = false;
            }
        }

        public RowData createAccumulators() {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData rowData = new GenericRowData(2);
            rowData.setField(1, 0L); // count has default 0 value
            return rowData;
        }

        public RowData getAccumulators() throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData row = new GenericRowData(2);
            if (!sumIsNull) {
                row.setField(0, sum);
            }
            if (!countIsNull) {
                row.setField(1, count);
            }
            return row;
        }

        public void cleanup(Long window) {}

        public void close() {
            closeCalled.incrementAndGet();
        }

        @Override
        public RowData getValue(Long window) throws Exception {
            if (!openCalled) {
                fail("Open was not called");
            }
            GenericRowData row = new GenericRowData(4);
            if (!sumIsNull) {
                row.setField(0, sum);
            }
            if (!countIsNull) {
                row.setField(1, count);
            }
            row.setField(1, count);
            row.setField(2, assigner.getWindowStart(window));
            row.setField(3, window);
            return row;
        }
    }

    /** Get epoch mills from a timestamp string and the time zone the timestamp belongs. */
    private static long epochMills(ZoneId shiftTimeZone, String timestampStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(timestampStr);
        ZoneOffset zoneOffset = shiftTimeZone.getRules().getOffset(localDateTime);
        return localDateTime.toInstant(zoneOffset).toEpochMilli();
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }
}
