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
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Harness tests for {@link TemporalRowTimeJoinOperator} and {@link TemporalRowTimeJoinOperatorV2}.
 */
@ExtendWith(ParameterizedTestExtension.class)
class TemporalRowTimeJoinOperatorTest extends TemporalTimeJoinOperatorTestBase {

    /**
     * Number of versions and probe records per key in {@link #testManyEntriesPerKey()}. It exceeds
     * the batch size (128) in which the RocksDB map state iterator loads entries several times.
     */
    private static final int MANY_ENTRIES = 500;

    private enum OperatorVersion {
        V1,
        V2
    }

    private enum Backend {
        HEAP,
        ROCKSDB;

        StateBackend create() {
            return this == HEAP ? new HashMapStateBackend() : new EmbeddedRocksDBStateBackend();
        }
    }

    @Parameter(0)
    private OperatorVersion version;

    @Parameter(1)
    private Backend backend;

    /** V1 does not depend on the state backend, so it only runs on heap. */
    @Parameters(name = "operator={0}, backend={1}")
    private static List<Object[]> parameters() {
        return Arrays.asList(
                new Object[] {OperatorVersion.V1, Backend.HEAP},
                new Object[] {OperatorVersion.V2, Backend.HEAP},
                new Object[] {OperatorVersion.V2, Backend.ROCKSDB});
    }

    @TestTemplate
    void testOrderedStateBackendDetection() throws Exception {
        assumeThat(version).isEqualTo(OperatorVersion.V2);

        TemporalRowTimeJoinOperatorV2 joinOperator =
                (TemporalRowTimeJoinOperatorV2) createJoinOperator(false);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator);
        testHarness.open();

        assertThat(joinOperator.isOrderedStateBackend()).isEqualTo(backend == Backend.ROCKSDB);

        testHarness.close();
    }

    /** Test rowtime temporal join. */
    @TestTemplate
    void testRowTimeInnerTemporalJoin() throws Exception {
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

        testRowTimeTemporalJoin(false, expectedOutput);
    }

    @TestTemplate
    void testRowTimeLeftTemporalJoin() throws Exception {
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

        testRowTimeTemporalJoin(true, expectedOutput);
    }

    private void testRowTimeTemporalJoin(boolean isLeftOuterJoin, List<Object> expectedOutput)
            throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(isLeftOuterJoin));

        testHarness.open();

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

    /** Test rowtime temporal join when set idle state retention. */
    @TestTemplate
    void testRowTimeTemporalJoinWithStateRetention() throws Exception {
        final int minRetentionTime = 4;
        final int maxRetentionTime = minRetentionTime * 3 / 2;
        BaseTwoInputStreamOperatorWithStateRetention joinOperator =
                createJoinOperator(true, minRetentionTime, maxRetentionTime);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator);
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
                                                getNextLeftIndexStateName(), Types.LONG))
                                .value())
                .isNull();
        assertThat(
                        joinOperator
                                .getKeyedStateStore()
                                .getState(
                                        new ValueStateDescriptor<>(
                                                getRegisteredTimerStateName(), Types.LONG))
                                .value())
                .isNull();

        testHarness.close();
    }

    @TestTemplate
    void testRowTimeInnerTemporalJoinOnUpsertSource() throws Exception {
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

        testRowTimeTemporalJoinOnUpsertSource(false, expectedOutput);
    }

    @TestTemplate
    void testRowTimeLeftTemporalJoinOnUpsertSource() throws Exception {
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

        testRowTimeTemporalJoinOnUpsertSource(true, expectedOutput);
    }

    private void testRowTimeTemporalJoinOnUpsertSource(
            boolean isLeftOuterJoin, List<Object> expectedOutput) throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(isLeftOuterJoin));

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

    @TestTemplate
    void testRowTimeInnerTemporalJoinLateRecords() throws Exception {
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

        testRowTimeTemporalJoinLateRecords(false, expectedOutput);
    }

    @TestTemplate
    void testRowTimeLeftTemporalJoinLateRecords() throws Exception {
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

        testRowTimeTemporalJoinLateRecords(true, expectedOutput);
    }

    /**
     * Verifies that probe-side records whose event time is less than or equal to the current
     * watermark are dropped on arrival: they are not joined, not emitted (even with a left outer
     * join), and are counted in the {@code numLateRecordsDropped} metric.
     */
    private void testRowTimeTemporalJoinLateRecords(
            boolean isLeftOuter, List<Object> expectedOutput) throws Exception {
        BaseTwoInputStreamOperatorWithStateRetention joinOperator = createJoinOperator(isLeftOuter);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator);

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
        assertThat(getNumLateRecordsDropped(joinOperator)).isEqualTo(5L);

        testHarness.close();
    }

    @TestTemplate
    void testEmissionInArrivalOrder() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(false));

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

    @TestTemplate
    void testRightRowAtLeftTimeBoundary() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(false));

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

    @TestTemplate
    void testKeepsLatestRightVersionAfterCleanup() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(false));

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

    /**
     * Mixes negative (pre-1970) and positive row times on both sides. On ordered state backends a
     * key serialization that does not order negative before positive values would make the scans
     * stop at the first positive entry and miss the due negative ones.
     */
    @TestTemplate
    void testNegativeRowTimes() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(true));

        testHarness.open();

        testHarness.processElement2(insertRecord(-10L, "k1", "r-10"));
        testHarness.processElement2(insertRecord(-4L, "k1", "r-4"));
        testHarness.processElement2(insertRecord(2L, "k1", "r2"));

        testHarness.processElement1(insertRecord(3L, "k1", "l3"));
        testHarness.processElement1(insertRecord(-7L, "k1", "l-7"));
        testHarness.processElement1(insertRecord(-12L, "k1", "l-12"));
        testHarness.processElement1(insertRecord(-3L, "k1", "l-3"));
        testHarness.processElement1(insertRecord(1L, "k1", "l1"));

        testHarness.processWatermark1(new Watermark(-5));
        testHarness.processWatermark2(new Watermark(-5));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(-7L, "k1", "l-7", -10L, "k1", "r-10"));
        expectedOutput.add(insertRecord(-12L, "k1", "l-12", null, null, null));
        expectedOutput.add(new Watermark(-5));
        expectedOutput.add(insertRecord(3L, "k1", "l3", 2L, "k1", "r2"));
        expectedOutput.add(insertRecord(-3L, "k1", "l-3", -4L, "k1", "r-4"));
        expectedOutput.add(insertRecord(1L, "k1", "l1", -4L, "k1", "r-4"));
        expectedOutput.add(new Watermark(5));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /**
     * Keeps more entries per key than the RocksDB map state iterator loads in one batch, so the
     * scans that remove entries and stop early on ordered backends cross batch boundaries. The
     * first watermark makes exactly one batch of probe records due.
     */
    @TestTemplate
    void testManyEntriesPerKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(createJoinOperator(false));

        testHarness.open();

        // build-side versions at even row times, probe records at odd row times in random order
        List<Long> probeTimes = new ArrayList<>();
        for (long i = 0; i < MANY_ENTRIES; i++) {
            testHarness.processElement2(insertRecord(2 * i, "k1", "r" + 2 * i));
            probeTimes.add(2 * i + 1);
        }
        Collections.shuffle(probeTimes, new Random(42));
        for (long probeTime : probeTimes) {
            testHarness.processElement1(insertRecord(probeTime, "k1", "l" + probeTime));
        }

        List<Object> expectedOutput = new ArrayList<>();
        long previousWatermark = Long.MIN_VALUE;
        for (long watermark : new long[] {256, 700, 2 * MANY_ENTRIES}) {
            testHarness.processWatermark1(new Watermark(watermark));
            testHarness.processWatermark2(new Watermark(watermark));

            for (long probeTime : probeTimes) {
                if (probeTime > previousWatermark && probeTime <= watermark) {
                    expectedOutput.add(
                            insertRecord(
                                    probeTime,
                                    "k1",
                                    "l" + probeTime,
                                    probeTime - 1,
                                    "k1",
                                    "r" + (probeTime - 1)));
                }
            }
            expectedOutput.add(new Watermark(watermark));
            previousWatermark = watermark;
        }

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private BaseTwoInputStreamOperatorWithStateRetention createJoinOperator(
            boolean isLeftOuterJoin) {
        return createJoinOperator(isLeftOuterJoin, 0, 0);
    }

    private BaseTwoInputStreamOperatorWithStateRetention createJoinOperator(
            boolean isLeftOuterJoin, long minRetentionTime, long maxRetentionTime) {
        if (version == OperatorVersion.V1) {
            return new TemporalRowTimeJoinOperator(
                    rowType,
                    rowType,
                    joinCondition,
                    0,
                    0,
                    minRetentionTime,
                    maxRetentionTime,
                    isLeftOuterJoin);
        }
        return new TemporalRowTimeJoinOperatorV2(
                rowType,
                rowType,
                joinCondition,
                0,
                0,
                minRetentionTime,
                maxRetentionTime,
                isLeftOuterJoin);
    }

    private String getNextLeftIndexStateName() {
        return version == OperatorVersion.V1
                ? TemporalRowTimeJoinOperator.getNextLeftIndexStateName()
                : TemporalRowTimeJoinOperatorV2.getNextLeftIndexStateName();
    }

    private String getRegisteredTimerStateName() {
        return version == OperatorVersion.V1
                ? TemporalRowTimeJoinOperator.getRegisteredTimerStateName()
                : TemporalRowTimeJoinOperatorV2.getRegisteredTimerStateName();
    }

    private long getNumLateRecordsDropped(
            BaseTwoInputStreamOperatorWithStateRetention joinOperator) {
        return version == OperatorVersion.V1
                ? ((TemporalRowTimeJoinOperator) joinOperator).getNumLateRecordsDropped().getCount()
                : ((TemporalRowTimeJoinOperatorV2) joinOperator)
                        .getNumLateRecordsDropped()
                        .getCount();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(BaseTwoInputStreamOperatorWithStateRetention temporalJoinOperator)
                    throws Exception {

        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        temporalJoinOperator, keySelector, keySelector, keyType);
        harness.setStateBackend(backend.create());
        return harness;
    }
}
