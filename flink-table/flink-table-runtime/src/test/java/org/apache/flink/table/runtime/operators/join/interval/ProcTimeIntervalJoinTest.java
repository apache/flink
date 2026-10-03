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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ProcTimeIntervalJoin}. */
class ProcTimeIntervalJoinTest extends TimeIntervalStreamJoinTestBase {

    private int keyIdx = 0;
    private RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {keyIdx}, rowType.toRowFieldTypes());
    private TypeInformation<RowData> keyType = InternalTypeInfo.ofFields();

    /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime + 20. * */
    @Test
    void testProcTimeInnerJoinWithCommonBounds() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.INNER, -10, 20, 15, rowType, rowType, joinFunction, -1L);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);

        testHarness.setProcessingTime(2);
        testHarness.processElement1(insertRecord(2L, "2a2"));
        // timers for key = 1 and key = 2
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(1L, "1a3"));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        // The number of timers won't increase.
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        testHarness.processElement2(insertRecord(1L, "1b3"));

        testHarness.setProcessingTime(4);
        testHarness.processElement2(insertRecord(2L, "2b4"));
        // The number of states should be doubled.
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(8);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(4);

        // Test for -10 boundary (13 - 10 = 3).
        // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
        testHarness.setProcessingTime(13);
        testHarness.processElement2(insertRecord(1L, "1b13"));

        // Test for +20 boundary (13 + 20 = 33).
        testHarness.setProcessingTime(33);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        testHarness.processElement1(insertRecord(1L, "1a33"));
        testHarness.processElement1(insertRecord(2L, "2a33"));
        // The left row (key = 2) with timestamp = 2 will be eagerly removed here.
        testHarness.processElement2(insertRecord(2L, "2b33"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "1a1", 1L, "1b3"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b3"));
        expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b4"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b13"));
        expectedOutput.add(insertRecord(1L, "1a33", 1L, "1b13"));
        expectedOutput.add(insertRecord(2L, "2a33", 2L, "2b33"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** a.proctime >= b.proctime - 10 and a.proctime <= b.proctime - 5. * */
    @Test
    void testProcTimeInnerJoinWithNegativeBounds() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.INNER, -10, -5, 2, rowType, rowType, joinFunction, -1L);

        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();

        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement1(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(1L, "1a3"));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        // All the right rows will not be cached.
        testHarness.processElement2(insertRecord(1L, "1b3"));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        testHarness.setProcessingTime(7);

        // Meets a.proctime <= b.proctime - 5.
        // This row will only be joined without being cached (7 >= 7 - 5).
        testHarness.processElement2(insertRecord(2L, "2b7"));
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        testHarness.setProcessingTime(12);
        // The left row (key = 1) with timestamp = 1 will be eagerly removed here.
        testHarness.processElement2(insertRecord(1L, "1b12"));

        // We add a delay (relativeWindowSize / 2) for cleaning up state.
        // No timers will be triggered here.
        testHarness.setProcessingTime(13);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(4);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        // Trigger the timer registered by the left row (key = 1) with timestamp = 1
        // (1 + 10 + 2 + 0 + 1 = 14).
        // The left row (key = 1) with timestamp = 3 will removed here.
        testHarness.setProcessingTime(14);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);

        // Clean up the left row (key = 2) with timestamp = 2.
        testHarness.setProcessingTime(16);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(0);
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(0);

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(2L, "2a2", 2L, "2b7"));
        expectedOutput.add(insertRecord(1L, "1a3", 1L, "1b12"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** Early fire on processing time, then a match retracts the speculative pad. */
    @Test
    void testProcTimeLeftOuterEarlyFireThenMatch() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.LEFT, -5, 9, 0, rowType, rowType, joinFunction, 3L);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();

        testHarness.setProcessingTime(10);
        testHarness.processElement1(insertRecord(1L, "a"));
        // One cleanup timer plus one early-fire timer at 10 + 3 = 13.
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(2);

        // Fire the early-fire timer: the unmatched left row is speculatively padded.
        testHarness.setProcessingTime(13);

        // A right row matches the early-fired left row.
        testHarness.setProcessingTime(14);
        testHarness.processElement2(insertRecord(1L, "b"));

        // Advance past cleanup: no further pad.
        testHarness.setProcessingTime(40);

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "a", null, null));
        expectedOutput.add(updateBeforeRecord(1L, "a", null, null));
        expectedOutput.add(updateAfterRecord(1L, "a", 1L, "b"));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** With early fire disabled the processing-time inner join behaves exactly as before. */
    @Test
    void testProcTimeInnerJoinIgnoresEarlyFire() throws Exception {
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.INNER, -5, 9, 0, rowType, rowType, joinFunction, 3L);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();

        testHarness.setProcessingTime(10);
        testHarness.processElement1(insertRecord(1L, "a"));
        // No early-fire timer for an inner join.
        assertThat(testHarness.numProcessingTimeTimers()).isEqualTo(1);

        testHarness.setProcessingTime(13);
        testHarness.setProcessingTime(40);

        List<Object> expectedOutput = new ArrayList<>();
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** Delay larger than the window span still pads an unmatched row exactly once. */
    @Test
    void testProcTimeLeftOuterEarlyFireDelayExceedsSpan() throws Exception {
        // Window span is 5 + 9 = 14; the delay exceeds it so cleanup may reach the row first.
        ProcTimeIntervalJoin joinProcessFunc =
                new ProcTimeIntervalJoin(
                        FlinkJoinType.LEFT, -5, 9, 0, rowType, rowType, joinFunction, 20L);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(joinProcessFunc);
        testHarness.open();

        testHarness.setProcessingTime(10);
        testHarness.processElement1(insertRecord(1L, "a"));
        // Cleanup at 16, early fire at 30: advancing past both must still emit a single pad.
        testHarness.setProcessingTime(35);

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "a", null, null));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(ProcTimeIntervalJoin intervalJoinFunc) throws Exception {
        KeyedCoProcessOperator<RowData, RowData, RowData, RowData> operator =
                new KeyedCoProcessOperator<>(intervalJoinFunc);
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector, keyType);
        return testHarness;
    }
}
