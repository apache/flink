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
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.async.AsyncStateAppendOnlyTopNFunction;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AppendOnlyTopNFunction} and {@link AsyncStateAppendOnlyTopNFunction}. */
class AppendOnlyTopNFunctionTest extends TopNFunctionTestBase {

    @Override
    protected AbstractTopNFunction createFunction(
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            boolean enableAsyncState) {
        if (enableAsyncState) {
            return new AsyncStateAppendOnlyTopNFunction(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        } else {
            return new AppendOnlyTopNFunction(
                    ttlConfig,
                    inputRowType,
                    generatedSortKeyComparator,
                    sortKeySelector,
                    rankType,
                    rankRange,
                    generateUpdateBefore,
                    outputRankNumber,
                    cacheSize);
        }
    }

    @Override
    boolean supportedAsyncState() {
        return true;
    }

    @Test
    void testOffsetUpdatesDoNotChangeRowsInSynchronousState() throws Exception {
        AbstractTopNFunction func =
                createFunction(
                        RankType.ROW_NUMBER, new ConstantRankRange(2, 3), true, false, false);
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createTestHarness(func)) {
            harness.open();
            harness.processElement(insertRecord("book", 1L, 12));
            harness.processElement(insertRecord("book", 2L, 19));
            harness.processElement(insertRecord("book", 3L, 20));
            harness.processElement(insertRecord("book", 4L, 11));
            harness.snapshot(0L, 0L);

            MapStateDescriptor<RowData, List<RowData>> descriptor =
                    new MapStateDescriptor<>(
                            "data-state-with-append",
                            sortKeySelector.getProducedType(),
                            new ListTypeInfo<>(inputRowType));
            descriptor.enableTimeToLive(ttlConfig);
            MapState<RowData, List<RowData>> state =
                    harness.getOperator()
                            .getPartitionedState(
                                    VoidNamespace.INSTANCE,
                                    VoidNamespaceSerializer.INSTANCE,
                                    descriptor);
            List<RowData> rows = new ArrayList<>();
            state.values().forEach(rows::addAll);
            assertThat(rows)
                    .filteredOn(row -> row.getLong(1) == 1L || row.getLong(1) == 2L)
                    .hasSize(2)
                    .extracting(RowData::getRowKind)
                    .containsOnly(RowKind.INSERT);
        }
    }

    @TestTemplate
    void testVariableRankRange() throws Exception {
        AbstractTopNFunction func =
                createFunction(RankType.ROW_NUMBER, new VariableRankRange(1), true, false);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = createTestHarness(func);
        testHarness.open();
        testHarness.processElement(insertRecord("book", 2L, 12));
        testHarness.processElement(insertRecord("book", 2L, 19));
        testHarness.processElement(insertRecord("book", 2L, 11));
        testHarness.processElement(insertRecord("fruit", 1L, 33));
        testHarness.processElement(insertRecord("fruit", 1L, 44));
        testHarness.processElement(insertRecord("fruit", 1L, 22));
        testHarness.close();

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord("book", 2L, 12));
        expectedOutput.add(insertRecord("book", 2L, 19));
        expectedOutput.add(deleteRecord("book", 2L, 19));
        expectedOutput.add(insertRecord("book", 2L, 11));
        expectedOutput.add(insertRecord("fruit", 1L, 33));
        expectedOutput.add(deleteRecord("fruit", 1L, 33));
        expectedOutput.add(insertRecord("fruit", 1L, 22));
        assertorWithoutRowNumber.assertOutputEquals(
                "output wrong.", expectedOutput, testHarness.getOutput());
    }
}
