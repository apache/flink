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

package org.apache.flink.table.runtime.operators.join.stream.asyncprocessing;

import org.apache.flink.state.forst.ForStStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests {@link AsyncStateStreamingJoinOperator} on the ForSt state backend, where state requests
 * complete asynchronously and in batches, so the callbacks of many keys are in flight at the same
 * time.
 */
class AsyncStateStreamingJoinOperatorForStTest {

    private static final int NUM_KEYS = 1000;

    private static final String CONDITION_FUNC_CODE =
            "public class ConditionFunction extends org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "\n"
                    + "    public ConditionFunction(Object[] reference) {\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public boolean apply(org.apache.flink.table.data.RowData in1, org.apache.flink.table.data.RowData in2) {\n"
                    + "        return true;\n"
                    + "    }\n"
                    + "\n"
                    + "    @Override\n"
                    + "    public void close() throws Exception {\n"
                    + "        super.close();\n"
                    + "    }"
                    + "}\n";

    private final InternalTypeInfo<RowData> leftTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {VarCharType.STRING_TYPE, VarCharType.STRING_TYPE},
                            new String[] {"id", "left_value"}));

    private final InternalTypeInfo<RowData> rightTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {VarCharType.STRING_TYPE, VarCharType.STRING_TYPE},
                            new String[] {"id0", "right_value"}));

    private final RowType outputType =
            RowType.of(
                    new LogicalType[] {
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE
                    },
                    new String[] {"id", "left_value", "id0", "right_value"});

    private final RowDataKeySelector leftKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0},
                    leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    private final RowDataKeySelector rightKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    new int[] {0},
                    rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    @Test
    void testLeftOuterJoinOnlyMatchesRowsOfTheSameKey() throws Exception {
        AsyncStateStreamingJoinOperator operator =
                new AsyncStateStreamingJoinOperator(
                        leftTypeInfo,
                        rightTypeInfo,
                        new GeneratedJoinCondition(
                                "ConditionFunction", CONDITION_FUNC_CODE, new Object[0]),
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                leftTypeInfo, leftKeySelector),
                        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(
                                rightTypeInfo, rightKeySelector),
                        true,
                        false,
                        new boolean[] {true},
                        0L,
                        0L);

        AsyncKeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> harness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        leftKeySelector,
                        rightKeySelector,
                        leftKeySelector.getProducedType());
        harness.setStateBackend(new ForStStateBackend());
        harness.setup(new RowDataSerializer(outputType));
        harness.open();
        try {
            for (int i = 0; i < NUM_KEYS; i++) {
                harness.processElement1(insertRecord("key-" + i, "left-" + i));
            }
            harness.drainStateRequests();

            for (int i = 0; i < NUM_KEYS; i++) {
                harness.processElement2(insertRecord("key-" + i, "right-" + i));
            }
            harness.drainStateRequests();

            List<String> crossKeyRows = new ArrayList<>();
            int joinedRows = 0;
            for (Object element : harness.getOutput()) {
                if (!(element instanceof StreamRecord)) {
                    continue;
                }
                RowData row = (RowData) ((StreamRecord<?>) element).getValue();
                if (row.isNullAt(2)) {
                    continue;
                }
                joinedRows++;
                String leftKey = row.getString(0).toString();
                String rightKey = row.getString(2).toString();
                if (!leftKey.equals(rightKey)) {
                    crossKeyRows.add(
                            row.getRowKind().shortString()
                                    + "("
                                    + leftKey
                                    + " joined to "
                                    + rightKey
                                    + ")");
                }
            }

            assertThat(crossKeyRows).as("rows that join two different keys").isEmpty();
            assertThat(joinedRows).as("rows with both sides joined").isEqualTo(NUM_KEYS);
        } finally {
            harness.close();
        }
    }
}
