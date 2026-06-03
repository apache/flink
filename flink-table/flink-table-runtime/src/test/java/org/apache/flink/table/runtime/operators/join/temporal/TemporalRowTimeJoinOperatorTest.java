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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Harness tests for {@link TemporalRowTimeJoinOperatorTest}. */
class TemporalRowTimeJoinOperatorTest extends TemporalTimeJoinOperatorTestBase {
    /** Test rowtime temporal join. */
    @Test
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

    @Test
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
        TemporalRowTimeJoinOperator joinOperator =
                new TemporalRowTimeJoinOperator(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuterJoin);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator);

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
    @Test
    void testRowTimeTemporalJoinWithStateRetention() throws Exception {
        final int minRetentionTime = 4;
        final int maxRetentionTime = minRetentionTime * 3 / 2;
        TemporalRowTimeJoinOperator joinOperator =
                new TemporalRowTimeJoinOperator(
                        rowType,
                        rowType,
                        joinCondition,
                        0,
                        0,
                        minRetentionTime,
                        maxRetentionTime,
                        true);
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
                                                TemporalRowTimeJoinOperator
                                                        .getNextLeftIndexStateName(),
                                                Types.LONG))
                                .value())
                .isNull();
        assertThat(
                        joinOperator
                                .getKeyedStateStore()
                                .getState(
                                        new ValueStateDescriptor<>(
                                                TemporalRowTimeJoinOperator
                                                        .getRegisteredTimerStateName(),
                                                Types.LONG))
                                .value())
                .isNull();

        testHarness.close();
    }

    @Test
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

    @Test
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
        TemporalRowTimeJoinOperator joinOperator =
                new TemporalRowTimeJoinOperator(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuterJoin);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinOperator);

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

    @Test
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

    @Test
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
        TemporalRowTimeJoinOperator joinOperator =
                new TemporalRowTimeJoinOperator(
                        rowType, rowType, joinCondition, 0, 0, 0, 0, isLeftOuter);
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
        assertThat(joinOperator.getNumLateRecordsDropped().getCount()).isEqualTo(5L);

        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(TemporalRowTimeJoinOperator temporalJoinOperator) throws Exception {

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                temporalJoinOperator, keySelector, keySelector, keyType);
    }
}
