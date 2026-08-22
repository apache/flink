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

package org.apache.flink.table.runtime.operators.join.temporal;

import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for {@link TemporalRowTimeJoinOperatorV2}. */
class TemporalRowTimeJoinOperatorV2Test extends TemporalTimeJoinOperatorTestBase {

    private static Stream<Named<StateBackend>> stateBackends() {
        return Stream.of(
                Named.of("heap", new HashMapStateBackend()),
                Named.of("rocksdb", new EmbeddedRocksDBStateBackend()));
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeInnerTemporalJoin(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(insertRecord(3L, "k1", "1a3", 2L, "k1", "1a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(6L, "k2", "2a3", 4L, "k2", "2a4"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(new Watermark(9));
        expectedOutput.add(insertRecord(11L, "k2", "5a12", 10L, "k2", "2a6"));
        expectedOutput.add(new Watermark(13));

        testRowTimeTemporalJoin(backend, false, expectedOutput);
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeLeftTemporalJoin(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        expectedOutput.add(insertRecord(1L, "k1", "1a1", null, null, null));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(insertRecord(3L, "k1", "1a3", 2L, "k1", "1a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(6L, "k2", "2a3", 4L, "k2", "2a4"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(insertRecord(9L, "k2", "5a11", null, null, null));
        expectedOutput.add(new Watermark(9));
        expectedOutput.add(insertRecord(11L, "k2", "5a12", 10L, "k2", "2a6"));
        expectedOutput.add(new Watermark(13));

        testRowTimeTemporalJoin(backend, true, expectedOutput);
    }

    private void testRowTimeTemporalJoin(
            StateBackend backend, boolean isLeftOuterJoin, List<Object> expectedOutput)
            throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuterJoin);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();
        assertThat(joinOperator.isOrderedStateBackend())
                .isEqualTo(backend instanceof EmbeddedRocksDBStateBackend);

        testHarness.processWatermark1(new Watermark(0));
        testHarness.processWatermark2(new Watermark(0));

        testHarness.processElement1(insertRecord(1L, "k1", "1a1"));
        testHarness.processElement2(insertRecord(2L, "k1", "1a2"));

        testHarness.processWatermark1(new Watermark(2));
        testHarness.processWatermark2(new Watermark(2));

        testHarness.processElement1(insertRecord(3L, "k1", "1a3"));
        testHarness.processElement2(insertRecord(4L, "k2", "2a4"));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        testHarness.processElement1(insertRecord(6L, "k2", "2a3"));
        testHarness.processElement2(updateBeforeRecord(7L, "k2", "2a4"));
        testHarness.processElement2(updateAfterRecord(7L, "k2", "2a5"));

        testHarness.processWatermark1(new Watermark(8));
        testHarness.processWatermark2(new Watermark(9));

        testHarness.processElement1(insertRecord(9L, "k2", "5a11"));
        testHarness.processElement1(insertRecord(11L, "k2", "5a12"));
        testHarness.processElement2(deleteRecord(9L, "k2", "2a5"));
        testHarness.processElement2(insertRecord(10L, "k2", "2a6"));

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeTemporalJoinWithStateRetention(StateBackend backend) throws Exception {
        final int minRetentionTime = 4;
        final int maxRetentionTime = minRetentionTime * 3 / 2;
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType,
                        rowType,
                        joinCondition,
                        0,
                        0,
                        minRetentionTime,
                        maxRetentionTime,
                        true);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);
        testHarness.open();

        testHarness.setProcessingTime(3);
        testHarness.processElement2(insertRecord(3L, "k1", "0a3"));
        testHarness.setProcessingTime(6);
        testHarness.processElement1(insertRecord(6L, "k1", "0a6"));

        testHarness.processWatermark1(new Watermark(7));
        testHarness.processWatermark2(new Watermark(7));
        testHarness.processElement2(updateBeforeRecord(3L, "k1", "0a3"));
        testHarness.processElement2(updateAfterRecord(3L, "k1", "0a5"));

        testHarness.setProcessingTime(9);
        testHarness.processElement1(insertRecord(9L, "k1", "7a9"));

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        testHarness.setProcessingTime(9 + maxRetentionTime);
        testHarness.processElement1(insertRecord(15L, "k1", "13a15"));

        testHarness.processWatermark1(new Watermark(15));
        testHarness.processWatermark2(new Watermark(16));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(6L, "k1", "0a6", 3L, "k1", "0a3"));
        expectedOutput.add(new Watermark(7));
        expectedOutput.add(insertRecord(9L, "k1", "7a9", 3L, "k1", "0a5"));
        expectedOutput.add(new Watermark(13));
        expectedOutput.add(insertRecord(15L, "k1", "13a15", null, null, null));
        expectedOutput.add(new Watermark(15));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        assertThat(
                        joinOperator
                                .getKeyedStateStore()
                                .getState(
                                        new ValueStateDescriptor<>(
                                                TemporalRowTimeJoinOperatorV2
                                                        .getNextLeftIndexStateName(),
                                                Types.LONG))
                                .value())
                .isNull();
        assertThat(
                        joinOperator
                                .getKeyedStateStore()
                                .getState(
                                        new ValueStateDescriptor<>(
                                                TemporalRowTimeJoinOperatorV2
                                                        .getRegisteredTimerStateName(),
                                                Types.LONG))
                                .value())
                .isNull();

        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeInnerTemporalJoinOnUpsertSource(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(updateAfterRecord(3L, "k1", "1a3", 2L, "k1", "1a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(6L, "k2", "2a3", 4L, "k2", "2a4"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(new Watermark(9));
        expectedOutput.add(insertRecord(11L, "k2", "5a12", 10L, "k2", "2a6"));
        expectedOutput.add(new Watermark(13));

        testRowTimeTemporalJoinOnUpsertSource(backend, false, expectedOutput);
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeLeftTemporalJoinOnUpsertSource(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        expectedOutput.add(insertRecord(1L, "k1", "1a1", null, null, null));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(updateAfterRecord(3L, "k1", "1a3", 2L, "k1", "1a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(6L, "k2", "2a3", 4L, "k2", "2a4"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(insertRecord(9L, "k2", "5a11", null, null, null));
        expectedOutput.add(new Watermark(9));
        expectedOutput.add(insertRecord(11L, "k2", "5a12", 10L, "k2", "2a6"));
        expectedOutput.add(new Watermark(13));

        testRowTimeTemporalJoinOnUpsertSource(backend, true, expectedOutput);
    }

    private void testRowTimeTemporalJoinOnUpsertSource(
            StateBackend backend, boolean isLeftOuterJoin, List<Object> expectedOutput)
            throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuterJoin);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();

        testHarness.processWatermark1(new Watermark(0));
        testHarness.processWatermark2(new Watermark(0));

        testHarness.processElement1(insertRecord(1L, "k1", "1a1"));
        testHarness.processElement2(insertRecord(2L, "k1", "1a2"));

        testHarness.processWatermark1(new Watermark(2));
        testHarness.processWatermark2(new Watermark(2));

        testHarness.processElement1(updateAfterRecord(3L, "k1", "1a3"));
        testHarness.processElement2(insertRecord(4L, "k2", "2a4"));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        testHarness.processElement1(insertRecord(6L, "k2", "2a3"));
        testHarness.processElement2(updateAfterRecord(7L, "k2", "2a5"));

        testHarness.processWatermark1(new Watermark(8));
        testHarness.processWatermark2(new Watermark(9));

        testHarness.processElement1(insertRecord(9L, "k2", "5a11"));
        testHarness.processElement1(insertRecord(11L, "k2", "5a12"));
        testHarness.processElement2(deleteRecord(9L, "k2", "2a5"));
        testHarness.processElement2(insertRecord(10L, "k2", "2a6"));

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeInnerTemporalJoinLateRecords(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", "1a3", 2L, "k1", "2a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(7L, "k1", "1a7", 2L, "k1", "2a2"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(new Watermark(11));
        expectedOutput.add(insertRecord(13L, "k2", "1a13", 9L, "k2", "2a9"));
        expectedOutput.add(new Watermark(13));
        expectedOutput.add(new Watermark(15));

        testRowTimeTemporalJoinLateRecords(backend, false, expectedOutput);
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRowTimeLeftTemporalJoinLateRecords(StateBackend backend) throws Exception {
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(3L, "k1", "1a3", 2L, "k1", "2a2"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(7L, "k1", "1a7", 2L, "k1", "2a2"));
        expectedOutput.add(new Watermark(8));
        expectedOutput.add(insertRecord(10L, "k2", "1a10", null, null, null));
        expectedOutput.add(new Watermark(11));
        expectedOutput.add(insertRecord(13L, "k2", "1a13", 9L, "k2", "2a9"));
        expectedOutput.add(new Watermark(13));
        expectedOutput.add(new Watermark(15));

        testRowTimeTemporalJoinLateRecords(backend, true, expectedOutput);
    }

    private void testRowTimeTemporalJoinLateRecords(
            StateBackend backend, boolean isLeftOuter, List<Object> expectedOutput)
            throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuter);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();

        // initialize watermark to 1
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Establish a build-side version at time 2 and a non-late probe record at time 3.
        testHarness.processElement2(insertRecord(2L, "k1", "2a2"));
        testHarness.processElement1(insertRecord(3L, "k1", "1a3"));
        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        // After Watermark(5), any probe record with leftTime <= 5 is late and must be dropped.
        testHarness.processElement1(insertRecord(5L, "k1", "1a5")); // leftTime == watermark
        testHarness.processElement1(insertRecord(4L, "k1", "1a4")); // leftTime < watermark
        testHarness.processElement1(insertRecord(1L, "k1", "1a1")); // leftTime << watermark
        // A non-late probe record should still be processed.
        testHarness.processElement1(insertRecord(7L, "k1", "1a7"));
        testHarness.processWatermark1(new Watermark(8));
        testHarness.processWatermark2(new Watermark(8));

        // A record for late retraction
        testHarness.processElement1(insertRecord(10L, "k2", "1a10"));
        testHarness.processWatermark1(new Watermark(11));
        testHarness.processWatermark2(new Watermark(11));

        // Add a late retraction and a late build-side record
        testHarness.processElement1(insertRecord(13L, "k2", "1a13"));
        testHarness.processElement2(insertRecord(9L, "k2", "2a9"));
        testHarness.processElement1(deleteRecord(10L, "k2", "1a10")); // late -> dropped
        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        // Another late retraction
        testHarness.processElement1(deleteRecord(13L, "k2", "1a13"));
        testHarness.processWatermark1(new Watermark(15));
        testHarness.processWatermark2(new Watermark(15));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        assertThat(joinOperator.getNumLateRecordsDropped().getCount()).isEqualTo(5L);

        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testEmissionInArrivalOrder(StateBackend backend) throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, false);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();

        testHarness.processWatermark1(new Watermark(0));
        testHarness.processWatermark2(new Watermark(0));

        testHarness.processElement2(insertRecord(1L, "k1", "r1"));
        // Probe records arrive out of row-time order; 5 first, then 3 and 4, plus one beyond the
        // upcoming watermark. The record with time 5 is exactly at the watermark and must be due.
        testHarness.processElement1(insertRecord(5L, "k1", "1a5"));
        testHarness.processElement1(insertRecord(3L, "k1", "1a3"));
        testHarness.processElement1(insertRecord(4L, "k1", "1a4"));
        testHarness.processElement1(insertRecord(8L, "k1", "1a8"));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        testHarness.processWatermark1(new Watermark(9));
        testHarness.processWatermark2(new Watermark(9));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        // arrival order 5, 3, 4 - not row-time order 3, 4, 5
        expectedOutput.add(insertRecord(5L, "k1", "1a5", 1L, "k1", "r1"));
        expectedOutput.add(insertRecord(3L, "k1", "1a3", 1L, "k1", "r1"));
        expectedOutput.add(insertRecord(4L, "k1", "1a4", 1L, "k1", "r1"));
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(8L, "k1", "1a8", 1L, "k1", "r1"));
        expectedOutput.add(new Watermark(9));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testRightRowAtLeftTimeBoundary(StateBackend backend) throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, false);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();

        testHarness.processWatermark1(new Watermark(0));
        testHarness.processWatermark2(new Watermark(0));

        // Build-side version and probe record at the same row time 2 -> must join.
        testHarness.processElement2(insertRecord(2L, "k1", "2a2"));
        testHarness.processElement1(insertRecord(2L, "k1", "1a2"));

        testHarness.processWatermark1(new Watermark(2));
        testHarness.processWatermark2(new Watermark(2));

        // DELETE build-side version and probe record at the same row time 4 -> no join.
        testHarness.processElement2(deleteRecord(4L, "k1", "2a2"));
        testHarness.processElement1(insertRecord(4L, "k1", "1a4"));

        testHarness.processWatermark1(new Watermark(4));
        testHarness.processWatermark2(new Watermark(4));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(0));
        expectedOutput.add(insertRecord(2L, "k1", "1a2", 2L, "k1", "2a2"));
        expectedOutput.add(new Watermark(2));
        expectedOutput.add(new Watermark(4));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void testKeepsLatestRightVersionAfterCleanup(StateBackend backend) throws Exception {
        TemporalRowTimeJoinOperatorV2 joinOperator =
                new TemporalRowTimeJoinOperatorV2(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, false);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator, backend);

        testHarness.open();

        // Two build-side versions, no probe records; the watermark triggers cleanup which must
        // remove version 2 but keep version 4 (the latest one <= watermark).
        testHarness.processElement2(insertRecord(2L, "k1", "2a2"));
        testHarness.processElement2(insertRecord(4L, "k1", "2a4"));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        // This probe record joins the surviving version 4.
        testHarness.processElement1(insertRecord(6L, "k1", "1a6"));

        testHarness.processWatermark1(new Watermark(7));
        testHarness.processWatermark2(new Watermark(7));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(5));
        expectedOutput.add(insertRecord(6L, "k1", "1a6", 4L, "k1", "2a4"));
        expectedOutput.add(new Watermark(7));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(
                    TemporalRowTimeJoinOperatorV2 temporalJoinOperator, StateBackend backend)
                    throws Exception {

        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        temporalJoinOperator, keySelector, keySelector, keyType);
        harness.setStateBackend(backend);
        return harness;
    }
}
