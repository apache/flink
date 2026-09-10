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

package org.apache.flink.table.runtime.operators.aggregate;

import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedAggsHandleFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.bundle.KeyedMapBundleOperator;
import org.apache.flink.table.runtime.operators.bundle.trigger.CountBundleTrigger;
import org.apache.flink.table.runtime.operators.over.SumAggsHandleFunction;
import org.apache.flink.table.runtime.util.GenericRowRecordSortComparator;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;

/**
 * Tests for {@link MiniBatchGroupAggFunction}.
 *
 * <p>This test covers the scenario where MiniBatchGroupAggFunction.finishBundle() encounters a key
 * with only retraction messages and no state.
 */
class MiniBatchGroupAggFunctionTest {

    // Input row: (key: String, value: Long)
    private final LogicalType[] inputFieldTypes =
            new LogicalType[] {VarCharType.STRING_TYPE, new BigIntType()};

    private final RowType inputRowType = RowType.of(inputFieldTypes, new String[] {"key", "value"});

    // Accumulator: (sum: Long)
    private final LogicalType[] accTypes = new LogicalType[] {new BigIntType()};

    // Output row: (key: String, sum: Long)
    private final LogicalType[] outputTypes =
            new LogicalType[] {VarCharType.STRING_TYPE, new BigIntType()};

    private final int keyIdx = 0;
    private final RowDataKeySelector keySelector =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {keyIdx}, inputFieldTypes);

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    outputTypes, new GenericRowRecordSortComparator(keyIdx, outputTypes[keyIdx]));

    private final GeneratedAggsHandleFunction genAggsHandler =
            new GeneratedAggsHandleFunction("SumAgg", "", new Object[0]) {
                @Override
                public SumAggsHandleFunction newInstance(ClassLoader classLoader) {
                    return new SumAggsHandleFunction(1); // inputIndex = 1 (the value field)
                }
            };

    private final GeneratedRecordEqualiser genRecordEqualiser =
            new GeneratedRecordEqualiser("Equaliser", "", new Object[0]) {
                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return (row1, row2) -> {
                        if (row1 instanceof GenericRowData && row2 instanceof GenericRowData) {
                            return row1.equals(row2);
                        }
                        return false;
                    };
                }
            };

    private MiniBatchGroupAggFunction createFunction(boolean generateUpdateBefore) {
        return new MiniBatchGroupAggFunction(
                genAggsHandler,
                genRecordEqualiser,
                accTypes,
                inputRowType,
                -1, // no COUNT(*) for this test
                generateUpdateBefore,
                0); // no state retention
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private OneInputStreamOperatorTestHarness<RowData, RowData> createTestHarness(
            MiniBatchGroupAggFunction function) throws Exception {
        CountBundleTrigger trigger = new CountBundleTrigger<>(10);
        KeyedMapBundleOperator operator = new KeyedMapBundleOperator(function, trigger);
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, keySelector, keySelector.getProducedType());
    }

    /**
     * Verifies that when finishBundle processes a key with only retraction messages (which gets
     * filtered out because there's no accumulator state), the method continues to process
     * subsequent keys in the bundle instead of returning early.
     */
    @Test
    void testFinishBundleContinuesAfterEmptyInputRows() throws Exception {
        MiniBatchGroupAggFunction function = createFunction(false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createTestHarness(function);
        testHarness.open();

        // Process a DELETE for key "aaa" (no existing state, will be filtered out)
        testHarness.processElement(deleteRecord("aaa", 1L));

        // Process INSERTs for keys "bbb" and "ccc"
        testHarness.processElement(insertRecord("bbb", 2L));
        testHarness.processElement(insertRecord("ccc", 3L));

        // Close to trigger finishBundle
        testHarness.close();

        // Verify that keys "bbb" and "ccc" were processed correctly
        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("bbb", 2L));
        expectedOutput.add(insertRecord("ccc", 3L));

        assertor.assertOutputEqualsSorted(
                "Keys after retraction-only key should still be processed",
                expectedOutput,
                testHarness.getOutput());
    }
}
