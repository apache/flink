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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.KeyEqualityGeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Tests for {@link StreamingJoinOperator}. */
public class StreamingJoinOperatorTest {

    private final DataType inputRowDataType =
            ROW(FIELD("f0", INT()), FIELD("f1", STRING())).bridgedTo(RowData.class);

    private final DataType keyRowDataType = ROW(FIELD("f0", INT())).bridgedTo(RowData.class);

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        INT().getLogicalType(),
                        STRING().getLogicalType(),
                        INT().getLogicalType(),
                        STRING().getLogicalType()
                    });

    @Test
    void testInnerJoinWithUniqueKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(false, false, true);

        testHarness.open();

        testHarness.processElement1(insertRecord(1, "s1"));
        testHarness.processElement2(insertRecord(1, "s11"));
        testHarness.processElement1(updateBeforeRecord(1, "s1"));
        testHarness.processElement1(updateAfterRecord(1, "s2"));
        testHarness.processElement2(insertRecord(1, "s12"));
        testHarness.processElement1(deleteRecord(1, "s2"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "s1", 1, "s11"));
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s11")); // row kind forwarded
        expectedOutput.add(updateAfterRecord(1, "s2", 1, "s11")); // row kind forwarded
        expectedOutput.add(insertRecord(1, "s2", 1, "s12"));
        expectedOutput.add(deleteRecord(1, "s2", 1, "s12")); // 1 row in state cuz  has key

        assertor.assertOutputEquals("wrong output", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testInnerJoinWithoutUniqueKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(false, false, false);

        testHarness.open();

        testHarness.processElement1(insertRecord(1, "s1"));
        testHarness.processElement2(insertRecord(1, "s11"));
        testHarness.processElement1(updateBeforeRecord(1, "s1"));
        testHarness.processElement1(updateAfterRecord(1, "s2"));
        testHarness.processElement2(insertRecord(1, "s12"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "s1", 1, "s11"));
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s11")); // row kind forwarded
        expectedOutput.add(updateAfterRecord(1, "s2", 1, "s11")); // row kind forwarded
        expectedOutput.add(insertRecord(1, "s2", 1, "s12"));

        assertor.assertOutputEquals("wrong output", expectedOutput, testHarness.getOutput());

        testHarness.processElement1(updateBeforeRecord(1, "s2")); // updated join key
        testHarness.processElement1(updateAfterRecord(2, "s2")); // no pair for key '2'

        // next 2 rows can be in any order
        expectedOutput.clear();
        // in case of update on join key we can have UPDATE_BEFORE without subsequent UPDATE_AFTER
        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s11")); // 2 rows in state cuz no key
        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s12")); // 2 rows in state cuz no key

        List<Object> result = new ArrayList<>(testHarness.getOutput()).subList(4, 6);
        assertor.assertOutputEqualsSorted("wrong output", expectedOutput, result);
        testHarness.close();
    }

    @Test
    void testLeftOuterJoinWithUniqueKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(true, false, true);

        testHarness.open();

        testHarness.processElement1(insertRecord(1, "s1"));
        testHarness.processElement2(insertRecord(1, "s11"));
        testHarness.processElement2(updateBeforeRecord(1, "s11"));
        testHarness.processElement2(updateAfterRecord(1, "s22"));
        testHarness.processElement1(insertRecord(1, "s2"));
        testHarness.processElement2(deleteRecord(1, "s22"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s1", null, null));
        expectedOutput.add(updateAfterRecord(1, "s1", 1, "s11"));
        // update on non-outer side leads to 4 output records, probably can be optimized
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s11"));
        expectedOutput.add(updateAfterRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s1", null, null));
        expectedOutput.add(updateAfterRecord(1, "s1", 1, "s22"));

        expectedOutput.add(insertRecord(1, "s2", 1, "s22"));

        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s22"));
        expectedOutput.add(updateAfterRecord(1, "s2", null, null));

        assertor.assertOutputEquals("wrong output", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    void testLeftOuterJoinWithoutUniqueKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(true, false, false);

        testHarness.open();

        testHarness.processElement1(insertRecord(1, "s1"));
        testHarness.processElement2(insertRecord(1, "s11"));
        testHarness.processElement2(deleteRecord(1, "s11"));
        testHarness.processElement2(insertRecord(1, "s22"));

        testHarness.processElement1(insertRecord(1, "s2"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s1", null, null));
        expectedOutput.add(updateAfterRecord(1, "s1", 1, "s11"));
        // update on non-outer side leads to 4 output records, probably can be optimized
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s11")); // -U instead of -D
        expectedOutput.add(updateAfterRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s1", null, null));
        expectedOutput.add(updateAfterRecord(1, "s1", 1, "s22"));

        expectedOutput.add(insertRecord(1, "s2", 1, "s22"));

        assertor.assertOutputEquals("wrong output", expectedOutput, testHarness.getOutput());

        testHarness.processElement2(deleteRecord(1, "s22"));

        // next -U/+U pairs can be in any order
        expectedOutput.clear();
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s22"));
        expectedOutput.add(updateAfterRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s22"));
        expectedOutput.add(updateAfterRecord(1, "s2", null, null));

        List<Object> result = new ArrayList<>(testHarness.getOutput()).subList(8, 12);
        assertor.assertOutputEqualsSorted("wrong output", expectedOutput, result);
        testHarness.close();
    }

    @Test
    void testFullOuterJoinWithUniqueKey() throws Exception {
        KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness =
                createTestHarness(true, true, true);

        testHarness.open();

        testHarness.processElement1(insertRecord(1, "s1"));
        testHarness.processElement2(insertRecord(1, "s11"));
        testHarness.processElement1(updateBeforeRecord(1, "s1"));
        testHarness.processElement1(updateAfterRecord(1, "s2"));
        testHarness.processElement2(updateBeforeRecord(1, "s11"));
        testHarness.processElement2(updateAfterRecord(1, "s22"));
        testHarness.processElement1(deleteRecord(1, "s2"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "s1", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s1", null, null));
        expectedOutput.add(updateAfterRecord(1, "s1", 1, "s11"));
        // update on non-outer side leads to 4 output records, probably can be optimized
        expectedOutput.add(updateBeforeRecord(1, "s1", 1, "s11"));
        expectedOutput.add(updateAfterRecord(null, null, 1, "s11"));
        expectedOutput.add(updateBeforeRecord(null, null, 1, "s11"));
        expectedOutput.add(updateAfterRecord(1, "s2", 1, "s11"));

        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s11"));
        expectedOutput.add(updateAfterRecord(1, "s2", null, null));
        expectedOutput.add(updateBeforeRecord(1, "s2", null, null));
        expectedOutput.add(updateAfterRecord(1, "s2", 1, "s22"));

        expectedOutput.add(updateBeforeRecord(1, "s2", 1, "s22"));
        expectedOutput.add(updateAfterRecord(null, null, 1, "s22"));

        assertor.assertOutputEquals("wrong output", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(boolean leftIsOuter, boolean rightIsOuter, boolean withUniqueKey)
                    throws Exception {
        InternalTypeInfo<RowData> inputTypeInfo =
                InternalTypeInfo.of(inputRowDataType.getLogicalType());
        InternalTypeInfo<RowData> keyTypeInfo =
                InternalTypeInfo.of(keyRowDataType.getLogicalType());
        KeySelector<RowData, RowData> keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0},
                        keyRowDataType.getLogicalType().getChildren().toArray(new LogicalType[0]));
        JoinInputSideSpec inputSideSpec =
                withUniqueKey
                        ? JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                keyTypeInfo, keySelector)
                        : JoinInputSideSpec.withoutUniqueKey();

        StreamingJoinOperator streamingJoinOperator =
                new StreamingJoinOperator(
                        inputTypeInfo,
                        inputTypeInfo,
                        new KeyEqualityGeneratedJoinCondition(keySelector, keySelector),
                        inputSideSpec,
                        inputSideSpec,
                        leftIsOuter,
                        rightIsOuter,
                        new boolean[] {true},
                        0);
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                streamingJoinOperator, keySelector, keySelector, keyTypeInfo);
    }
}
