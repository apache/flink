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

package org.apache.flink.table.runtime.operators.join.window;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.TimeWindowUtil.toUtcTimestampMills;
import static org.junit.Assert.assertEquals;

/** Tests for window join operators created by {@link WindowJoinOperatorBuilder}. */
@RunWith(Parameterized.class)
public class WindowJoinOperatorTest {

    private static final InternalTypeInfo<RowData> INPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

    private static final InternalTypeInfo<RowData> OUTPUT_ROW_TYPE =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH),
                    new BigIntType(),
                    new VarCharType(VarCharType.MAX_LENGTH));

    private static final RowDataHarnessAssertor ASSERTER =
            new RowDataHarnessAssertor(OUTPUT_ROW_TYPE.toRowFieldTypes());

    private static final RowDataHarnessAssertor SEMI_ANTI_JOIN_ASSERTER =
            new RowDataHarnessAssertor(INPUT_ROW_TYPE.toRowFieldTypes());

    private static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

    private static final ZoneId SHANGHAI_ZONE_ID = ZoneId.of("Asia/Shanghai");

    private final ZoneId shiftTimeZone;

    public WindowJoinOperatorTest(ZoneId shiftTimeZone) {
        this.shiftTimeZone = shiftTimeZone;
    }

    @Parameterized.Parameters(name = "TimeZone = {0}")
    public static Collection<Object[]> runMode() {
        return Arrays.asList(new Object[] {UTC_ZONE_ID}, new Object[] {SHANGHAI_ZONE_ID});
    }

    @Test
    public void testSemiJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.SEMI);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        expectedOutput.add(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(10));
        SEMI_ANTI_JOIN_ASSERTER.assertOutputEqualsSorted(
                "output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testAntiJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.ANTI);
        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(10));
        SEMI_ANTI_JOIN_ASSERTER.assertOutputEqualsSorted(
                "output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testInnerJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.INNER);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(new Watermark(10));
        ASSERTER.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testLeftOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.LEFT);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1", null, null));
        expectedOutput.add(new Watermark(10));
        ASSERTER.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1", null, null));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testRightOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.RIGHT);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(insertRecord(null, null, toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(10));
        ASSERTER.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testOuterJoin() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(FlinkJoinType.FULL);

        testHarness.open();
        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));

        // Test late data would be dropped
        testHarness.processElement1(insertRecord(toUtcTimestampMills(1L, shiftTimeZone), "k1"));
        assertEquals(0, testHarness.numEventTimeTimers());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(3L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(4, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(10));
        testHarness.processWatermark2(new Watermark(10));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(new Watermark(1));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(3L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(insertRecord(toUtcTimestampMills(6L, shiftTimeZone), "k1", null, null));
        expectedOutput.add(insertRecord(null, null, toUtcTimestampMills(9L, shiftTimeZone), "k1"));
        expectedOutput.add(new Watermark(10));
        ASSERTER.assertOutputEqualsSorted("output wrong.", expectedOutput, testHarness.getOutput());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement1(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1"));
        testHarness.processElement1(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        testHarness.processElement2(insertRecord(toUtcTimestampMills(15L, shiftTimeZone), "k1"));
        assertEquals(3, testHarness.numKeyedStateEntries());

        testHarness.processWatermark1(new Watermark(13));
        testHarness.processWatermark2(new Watermark(13));

        expectedOutput.add(insertRecord(toUtcTimestampMills(12L, shiftTimeZone), "k1", null, null));
        expectedOutput.add(new Watermark(13));
        assertEquals(2, testHarness.numKeyedStateEntries());
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());

        testHarness.processWatermark1(new Watermark(18));
        testHarness.processWatermark2(new Watermark(18));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(
                insertRecord(
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1",
                        toUtcTimestampMills(15L, shiftTimeZone),
                        "k1"));
        expectedOutput.add(new Watermark(18));
        ASSERTER.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(FlinkJoinType joinType) throws Exception {
        String funcCode =
                "public class TestWindowJoinCondition extends org.apache.flink.api.common.functions.AbstractRichFunction "
                        + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                        + "\n"
                        + "    public TestWindowJoinCondition(Object[] reference) {\n"
                        + "    }\n"
                        + "\n"
                        + "    @Override\n"
                        + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                        + "        return true;\n"
                        + "    }\n"
                        + "}\n";
        GeneratedJoinCondition joinFunction =
                new GeneratedJoinCondition("TestWindowJoinCondition", funcCode, new Object[0]);
        int keyIdx = 1;
        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {keyIdx}, INPUT_ROW_TYPE.toRowFieldTypes());
        TypeInformation<RowData> keyType = InternalTypeInfo.ofFields();
        WindowJoinOperator operator =
                WindowJoinOperatorBuilder.builder()
                        .leftSerializer(INPUT_ROW_TYPE.toRowSerializer())
                        .rightSerializer(INPUT_ROW_TYPE.toRowSerializer())
                        .generatedJoinCondition(joinFunction)
                        .leftWindowEndIndex(0)
                        .rightWindowEndIndex(0)
                        .filterNullKeys(new boolean[] {true})
                        .joinType(joinType)
                        .withShiftTimezone(shiftTimeZone)
                        .build();
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator, keySelector, keySelector, keyType);
        return testHarness;
    }
}
