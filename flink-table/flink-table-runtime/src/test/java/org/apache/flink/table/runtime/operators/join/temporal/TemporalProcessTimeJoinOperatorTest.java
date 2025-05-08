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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.temporal.asyncprocessing.AsyncStateTemporalProcessTimeJoinOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Harness tests for {@link TemporalProcessTimeJoinOperator}. */
@ExtendWith(ParameterizedTestExtension.class)
class TemporalProcessTimeJoinOperatorTest extends TemporalTimeJoinOperatorTestBase {

    private int keyIdx = 0;
    private InternalTypeInfo<RowData> rowType =
            InternalTypeInfo.ofFields(new BigIntType(), VarCharType.STRING_TYPE);
    private RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {keyIdx}, rowType.toRowFieldTypes());
    private TypeInformation<RowData> keyType = keySelector.getProducedType();
    private InternalTypeInfo<RowData> outputRowType =
            InternalTypeInfo.ofFields(
                    new BigIntType(),
                    VarCharType.STRING_TYPE,
                    new BigIntType(),
                    VarCharType.STRING_TYPE);
    private RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(outputRowType.toRowFieldTypes());

    @Parameters(name = "enableAsyncState = {0}")
    public static List<Boolean> enableAsyncState() {
        return Arrays.asList(false, true);
    }

    @Parameter private boolean enableAsyncState;

    /** Test proctime temporal join. */
    @TestTemplate
    void testProcTimeTemporalJoin() throws Exception {
        TwoInputStreamOperator<RowData, RowData, RowData> joinOperator =
                createTemporalProcessTimeJoinOperator(rowType, joinCondition, 0, 0, false);
        testHarness = createTestHarness(joinOperator);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement2(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(2L, "2a3"));

        testHarness.setProcessingTime(4);
        testHarness.processElement2(insertRecord(1L, "1a4"));

        testHarness.setProcessingTime(5);
        testHarness.processElement1(insertRecord(1L, "1a5"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));
        expectedOutput.add(insertRecord(1L, "1a5", 1L, "1a4"));
        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** Test proctime temporal join when set idle state retention. */
    @TestTemplate
    void testProcTimeTemporalJoinWithStateRetention() throws Exception {
        final int minRetentionTime = 10;
        final int maxRetentionTime = minRetentionTime * 3 / 2;
        TwoInputStreamOperator<RowData, RowData, RowData> joinOperator =
                createTemporalProcessTimeJoinOperator(
                        rowType, joinCondition, minRetentionTime, maxRetentionTime, false);
        testHarness = createTestHarness(joinOperator);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement2(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(2L, "2a3"));

        testHarness.setProcessingTime(3 + maxRetentionTime);
        testHarness.processElement1(insertRecord(2L, "1a5"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** Test proctime left temporal join when set idle state retention. */
    @TestTemplate
    void testLeftProcTimeTemporalJoinWithStateRetention() throws Exception {
        final int minRetentionTime = 10;
        final int maxRetentionTime = minRetentionTime * 3 / 2;
        TwoInputStreamOperator<RowData, RowData, RowData> joinOperator =
                createTemporalProcessTimeJoinOperator(
                        rowType, joinCondition, minRetentionTime, maxRetentionTime, true);
        testHarness = createTestHarness(joinOperator);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement2(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(2L, "2a3"));

        testHarness.setProcessingTime(3 + maxRetentionTime);
        testHarness.processElement1(insertRecord(2L, "1a5"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1L, "1a1", null, null));
        expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));
        expectedOutput.add(insertRecord(2L, "1a5", null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    /** Test proctime temporal join changelog stream. */
    @TestTemplate
    void testProcTimeTemporalJoinOnChangelog() throws Exception {
        TwoInputStreamOperator<RowData, RowData, RowData> joinOperator =
                createTemporalProcessTimeJoinOperator(rowType, joinCondition, 0, 0, false);
        testHarness = createTestHarness(joinOperator);
        testHarness.open();
        testHarness.setProcessingTime(1);
        testHarness.processElement1(insertRecord(1L, "1a1"));

        testHarness.setProcessingTime(2);
        testHarness.processElement2(insertRecord(2L, "2a2"));

        testHarness.setProcessingTime(3);
        testHarness.processElement1(insertRecord(2L, "2a3"));

        testHarness.setProcessingTime(4);
        testHarness.processElement2(insertRecord(1L, "1a4"));
        testHarness.processElement2(updateBeforeRecord(1L, "1a4"));
        testHarness.processElement2(updateAfterRecord(1L, "1a7"));

        testHarness.setProcessingTime(5);
        testHarness.processElement1(insertRecord(1L, "1a5"));
        testHarness.processElement2(deleteRecord(1L, "1a7"));

        testHarness.setProcessingTime(6);
        testHarness.processElement1(insertRecord(1L, "1a6"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(2L, "2a3", 2L, "2a2"));
        expectedOutput.add(insertRecord(1L, "1a5", 1L, "1a7"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Override
    protected KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createTestHarness(
                    TwoInputStreamOperator<RowData, RowData, RowData> temporalJoinOperator)
                    throws Exception {

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                temporalJoinOperator, keySelector, keySelector, keyType);
    }

    private TwoInputStreamOperator<RowData, RowData, RowData> createTemporalProcessTimeJoinOperator(
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            long minRetentionTime,
            long maxRetentionTime,
            boolean isLeftOuterJoin) {
        if (!enableAsyncState) {
            return new TemporalProcessTimeJoinOperator(
                    rightType,
                    generatedJoinCondition,
                    minRetentionTime,
                    maxRetentionTime,
                    isLeftOuterJoin);
        } else {
            return new AsyncStateTemporalProcessTimeJoinOperator(
                    rightType,
                    generatedJoinCondition,
                    minRetentionTime,
                    maxRetentionTime,
                    isLeftOuterJoin);
        }
    }
}
