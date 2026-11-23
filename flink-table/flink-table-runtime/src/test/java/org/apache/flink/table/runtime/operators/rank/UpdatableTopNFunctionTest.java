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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UpdatableTopNFunction}. */
class UpdatableTopNFunctionTest extends TopNFunctionTestBase {

    @Override
    protected AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            boolean enableAsyncState) {
        return new UpdatableTopNFunction(
                ttlConfig,
                inputRowType,
                rowKeySelector,
                generatedSortKeyComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber,
                cacheSize);
    }

    @Override
    boolean supportedAsyncState() {
        return false;
    }

    @TestTemplate
    void testOffsetUpdatesDoNotChangeOtherStateRows() throws Exception {
        assertOtherStateRowsKeepInsertKind(3L, 11);
        assertOtherStateRowsKeepInsertKind(1L, 21);
    }

    private void assertOtherStateRowsKeepInsertKind(long updatedId, int newScore) throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(2, 3), true, false);
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createTestHarness(func)) {
            harness.setStateBackend(new EmbeddedRocksDBStateBackend());
            harness.open();
            harness.processElement(insertRecord("book", 1L, 12));
            harness.processElement(insertRecord("book", 2L, 19));
            harness.processElement(insertRecord("book", 3L, 20));
            harness.processElement(updateAfterRecord("book", updatedId, newScore));
            harness.snapshot(0L, 0L);

            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> descriptor =
                    new MapStateDescriptor<>(
                            "data-state-with-update",
                            rowKeySelector.getProducedType(),
                            new TupleTypeInfo<>(inputRowType, Types.INT));
            descriptor.enableTimeToLive(ttlConfig);
            MapState<RowData, Tuple2<RowData, Integer>> state =
                    harness.getOperator()
                            .getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    descriptor);
            assertThat(state.values())
                    .filteredOn(value -> value.f0.getLong(1) != updatedId)
                    .hasSize(2)
                    .extracting(value -> value.f0.getRowKind())
                    .containsOnly(RowKind.INSERT);
        }
    }

    @TestTemplate
    void testVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(updateAfterRecord("book", 2L, 18));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 33));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 2L, 18));
        expectedOutput.add(insertRecord("fruit", 1L, 44));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 44));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 33));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @Override
    @TestTemplate
    void testOutputRankNumberWithVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(updateAfterRecord("book", 2L, 12));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 33));
        testHarness.processElement(updateAfterRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 12, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(insertRecord("fruit", 1L, 44, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 44, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateBeforeRecord("fruit", 1L, 33, 1L));
        expectedOutput.add(updateAfterRecord("fruit", 1L, 22, 1L));
        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        // ("book", 3L, 16)
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 1L));
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // U ("book", 2L, 11)
        expectedOutput.add(updateBeforeRecord("book", 3L, 16, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 2L));
        // U ("book", 3L, 15)
        expectedOutput.add(updateBeforeRecord("book", 3L, 16, 2L));
        expectedOutput.add(updateAfterRecord("book", 3L, 15, 2L));
        // ("book", 4L, 2)
        expectedOutput.add(updateBeforeRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 1L));
        expectedOutput.add(updateBeforeRecord("book", 3L, 15, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 2L));
        // U ("book", 2L, 1)
        expectedOutput.add(updateBeforeRecord("book", 4L, 2, 1L));
        expectedOutput.add(updateBeforeRecord("book", 2L, 11, 2L));
        expectedOutput.add(updateAfterRecord("book", 2L, 1, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 2L));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenOutputRankNumberAndNotGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, true);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        // ("book", 2L, 19)
        expectedOutput.add(insertRecord("book", 2L, 19, 1L));
        // ("book", 3L, 16)
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 1L));
        expectedOutput.add(insertRecord("book", 2L, 19, 2L));
        // U ("book", 2L, 11)
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 1L));
        expectedOutput.add(updateAfterRecord("book", 3L, 16, 2L));
        // U ("book", 3L, 15)
        expectedOutput.add(updateAfterRecord("book", 3L, 15, 2L));
        // ("book", 4L, 2)
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 1L));
        expectedOutput.add(updateAfterRecord("book", 2L, 11, 2L));
        // U ("book", 2L, 1)
        expectedOutput.add(updateAfterRecord("book", 2L, 1, 1L));
        expectedOutput.add(updateAfterRecord("book", 4L, 2, 2L));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenNotOutputRankNumber() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 3L, 16));
        expectedOutput.add(updateBeforeRecord("book", 2L, 19));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateBeforeRecord("book", 3L, 16));
        expectedOutput.add(updateAfterRecord("book", 3L, 15));
        expectedOutput.add(deleteRecord("book", 3L, 15));
        expectedOutput.add(insertRecord("book", 4L, 2));
        expectedOutput.add(updateBeforeRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 2L, 1));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testSortKeyChangesWhenNotOutputRankNumberAndNotGenerateUpdateBefore() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new ConstantRankRange(1, 2), false, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 3L, 16));
        testHarness.processElement(updateAfterRecord("book", 2L, 11));
        testHarness.processElement(updateAfterRecord("book", 3L, 15));
        testHarness.processElement(insertRecord("book", 4L, 2));
        testHarness.processElement(updateAfterRecord("book", 2L, 1));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 3L, 16));
        expectedOutput.add(updateAfterRecord("book", 2L, 11));
        expectedOutput.add(updateAfterRecord("book", 3L, 15));
        expectedOutput.add(deleteRecord("book", 3L, 15));
        expectedOutput.add(insertRecord("book", 4L, 2));
        expectedOutput.add(updateAfterRecord("book", 2L, 1));

        assertorWithRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }
}
