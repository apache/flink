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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedCollectorWrapper;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.KeyedLookupJoinWrapper;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinRunner;
import org.apache.flink.table.runtime.operators.join.lookup.LookupJoinWithCalcRunner;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.data.StringData.fromString;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;

/** Harness tests for {@link KeyedLookupJoinWrapper}. */
public class KeyedLookupJoinHarnessTest {

    private final InternalTypeInfo<RowData> inputRowType =
            InternalTypeInfo.ofFields(new IntType(), VarCharType.STRING_TYPE);

    private final RowDataHarnessAssertor assertor =
            new RowDataHarnessAssertor(
                    new LogicalType[] {
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType(),
                        DataTypes.INT().getLogicalType(),
                        DataTypes.STRING().getLogicalType()
                    });

    StateTtlConfig ttlConfig = StateConfigUtil.createTtlConfig(10_000_000);

    @Test
    public void testTemporalInnerJoin() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.INNER_JOIN, FilterOnTable.WITHOUT_FILTER, false);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateBeforeRecord(1, "a"));
        testHarness.processElement(updateAfterRecord(1, "a2"));
        testHarness.processElement(deleteRecord(1, "a2"));
        testHarness.processElement(insertRecord(1, "a3"));
        testHarness.processElement(deleteRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c2"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(deleteRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(1, "a2", 2, "Julian-2"));
        expectedOutput.add(deleteRecord(1, "a2", 2, "Julian-2"));
        expectedOutput.add(insertRecord(1, "a3", 3, "Julian-3"));
        expectedOutput.add(deleteRecord(3, "c", 3, "Jark"));
        expectedOutput.add(deleteRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jackson-2"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalInnerJoinLookupKeyContainsPk() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.INNER_JOIN, FilterOnTable.WITHOUT_FILTER, true);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateBeforeRecord(1, "a"));
        testHarness.processElement(updateAfterRecord(1, "a2"));
        testHarness.processElement(deleteRecord(1, "a2"));
        testHarness.processElement(insertRecord(1, "a3"));
        testHarness.processElement(deleteRecord(3, "c"));
        testHarness.processElement(insertRecord(3, "c2"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(deleteRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(1, "a2", 2, "Julian-2"));
        expectedOutput.add(deleteRecord(1, "a2", 2, "Julian-2"));
        expectedOutput.add(insertRecord(1, "a3", 3, "Julian-3"));
        expectedOutput.add(deleteRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalInnerJoinWithFilter() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.INNER_JOIN, FilterOnTable.WITH_FILTER, false);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateBeforeRecord(3, "c"));
        testHarness.processElement(updateAfterRecord(3, "c2"));
        testHarness.processElement(deleteRecord(3, "c2"));
        testHarness.processElement(insertRecord(3, "c3"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(deleteRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jackson-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jackson-2"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jark-3"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jackson-3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalInnerJoinWithFilterLookupKeyContainsPk() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.INNER_JOIN, FilterOnTable.WITH_FILTER, true);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateBeforeRecord(3, "c"));
        testHarness.processElement(updateAfterRecord(3, "c2"));
        testHarness.processElement(deleteRecord(3, "c2"));
        testHarness.processElement(insertRecord(3, "c3"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(deleteRecord(3, "c", null, null));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jark-3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalLeftJoin() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITHOUT_FILTER, false);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateAfterRecord(2, "b2"));
        testHarness.processElement(deleteRecord(2, "b2"));
        testHarness.processElement(insertRecord(2, "b3"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));
        expectedOutput.add(insertRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(deleteRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(insertRecord(2, "b3", 3, "default-3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalLeftJoinLookupKeyContainsPk() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITHOUT_FILTER, true);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(updateAfterRecord(2, "b2"));
        testHarness.processElement(deleteRecord(2, "b2"));
        testHarness.processElement(insertRecord(2, "b3"));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jark"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));
        expectedOutput.add(insertRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(deleteRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(insertRecord(2, "b3", 3, "default-3"));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalLeftJoinWithFilter() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITH_FILTER, false);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(deleteRecord(2, "b"));
        testHarness.processElement(insertRecord(2, "b2"));
        testHarness.processElement(updateBeforeRecord(3, "c"));
        testHarness.processElement(updateAfterRecord(3, "c2"));
        testHarness.processElement(deleteRecord(3, "c2"));
        testHarness.processElement(insertRecord(3, "c3"));
        testHarness.processElement(insertRecord(4, null));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));
        expectedOutput.add(deleteRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(deleteRecord(3, "c", 3, "Jackson"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jackson-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jackson-2"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jark-3"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jackson-3"));
        expectedOutput.add(insertRecord(4, null, null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    @Test
    public void testTemporalLeftJoinWithFilterLookupKeyContainsPk() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                createHarness(JoinType.LEFT_JOIN, FilterOnTable.WITH_FILTER, true);

        testHarness.open();

        testHarness.processElement(insertRecord(1, "a"));
        testHarness.processElement(insertRecord(2, "b"));
        testHarness.processElement(insertRecord(3, "c"));
        testHarness.processElement(insertRecord(4, "d"));
        testHarness.processElement(insertRecord(5, "e"));
        testHarness.processElement(deleteRecord(2, "b"));
        testHarness.processElement(insertRecord(2, "b2"));
        testHarness.processElement(updateBeforeRecord(3, "c"));
        testHarness.processElement(updateAfterRecord(3, "c2"));
        testHarness.processElement(deleteRecord(3, "c2"));
        testHarness.processElement(insertRecord(3, "c3"));
        testHarness.processElement(insertRecord(4, null));

        List<Object> expectedOutput = new ArrayList<>();
        expectedOutput.add(insertRecord(1, "a", 1, "Julian"));
        expectedOutput.add(insertRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(3, "c", null, null));
        expectedOutput.add(insertRecord(4, "d", 4, "Fabian"));
        expectedOutput.add(insertRecord(5, "e", null, null));
        expectedOutput.add(deleteRecord(2, "b", null, null));
        expectedOutput.add(insertRecord(2, "b2", 2, "default-2"));
        expectedOutput.add(deleteRecord(3, "c", null, null));
        expectedOutput.add(insertRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(deleteRecord(3, "c2", 6, "Jark-2"));
        expectedOutput.add(insertRecord(3, "c3", 9, "Jark-3"));
        expectedOutput.add(insertRecord(4, null, null, null));

        assertor.assertOutputEquals("output wrong.", expectedOutput, testHarness.getOutput());
        testHarness.close();
    }

    // ---------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            JoinType joinType, FilterOnTable filterOnTable, boolean lookupKeyContainsPrimaryKey)
            throws Exception {
        boolean isLeftJoin = joinType == JoinType.LEFT_JOIN;
        LookupJoinRunner joinRunner;
        TestingEvolvingOutputFetcherFunction fetcher;
        if (lookupKeyContainsPrimaryKey) {
            fetcher = new TestingEvolvingOutputFetcherFunctionWithPk();
        } else {
            fetcher = new TestingEvolvingOutputFetcherFunction();
        }
        if (filterOnTable == FilterOnTable.WITHOUT_FILTER) {
            joinRunner =
                    new LookupJoinRunner(
                            new GeneratedFunctionWrapper<>(fetcher),
                            new GeneratedCollectorWrapper<>(
                                    new LookupJoinHarnessTest.TestingFetcherCollector()),
                            new GeneratedFunctionWrapper(
                                    new LookupJoinHarnessTest.TestingPreFilterCondition()),
                            isLeftJoin,
                            2);
        } else {
            joinRunner =
                    new LookupJoinWithCalcRunner(
                            new GeneratedFunctionWrapper<>(fetcher),
                            new GeneratedFunctionWrapper<>(
                                    new LookupJoinHarnessTest.CalculateOnTemporalTable()),
                            new GeneratedCollectorWrapper<>(
                                    new LookupJoinHarnessTest.TestingFetcherCollector()),
                            new GeneratedFunctionWrapper(
                                    new LookupJoinHarnessTest.TestingPreFilterCondition()),
                            isLeftJoin,
                            2);
        }
        TypeSerializer<RowData> temporalSerializer =
                new RowDataSerializer(
                        DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());

        KeyedLookupJoinWrapper keyedLookupJoinWrapper =
                new KeyedLookupJoinWrapper(
                        joinRunner, ttlConfig, temporalSerializer, lookupKeyContainsPrimaryKey);

        KeyedProcessOperator<RowData, RowData, RowData> operator =
                new KeyedProcessOperator<>(keyedLookupJoinWrapper);

        RowDataKeySelector keySelector =
                HandwrittenSelectorUtil.getRowDataSelector(
                        new int[] {0}, inputRowType.toRowFieldTypes());

        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, keySelector, keySelector.getProducedType());
    }

    /** Whether this is a inner join or left join. */
    private enum JoinType {
        INNER_JOIN,
        LEFT_JOIN
    }

    /** Whether there is a filter on temporal table. */
    private enum FilterOnTable {
        WITH_FILTER,
        WITHOUT_FILTER
    }

    // ---------------------------------------------------------------------------------

    /**
     * The {@link TestingEvolvingOutputFetcherFunctionWithPk} extends the {@link
     * TestingEvolvingOutputFetcherFunction} which only returns zero or one RowData for a single
     * integer key.
     */
    public static class TestingEvolvingOutputFetcherFunctionWithPk
            extends TestingEvolvingOutputFetcherFunction {
        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            int id = value.getInt(0);
            int currentCnt = counter(id);
            List<GenericRowData> rows = lookup(id);
            if (rows != null) {
                // collect first row
                collectUpdatedRow(rows.get(0), currentCnt, out);
            } else if (currentCnt > 1) {
                // return a default value for which lookup miss at 1st time
                out.collect(GenericRowData.of(currentCnt, fromString("default-" + currentCnt)));
            }
        }
    }

    /**
     * The {@link TestingEvolvingOutputFetcherFunction} only accepts a single integer lookup key and
     * returns zero or one or more RowData which will updates after first access.
     */
    public static class TestingEvolvingOutputFetcherFunction
            extends RichFlatMapFunction<RowData, RowData> {

        private static final long serialVersionUID = 1L;
        private static final Map<Integer, List<GenericRowData>> baseData = new HashMap<>();

        private transient Map<Integer, Integer> accessCounter;

        @Override
        public void open(OpenContext openContext) throws Exception {
            baseData.clear();
            baseData.put(1, Collections.singletonList(GenericRowData.of(1, fromString("Julian"))));
            baseData.put(
                    3,
                    Arrays.asList(
                            GenericRowData.of(3, fromString("Jark")),
                            GenericRowData.of(3, fromString("Jackson"))));
            baseData.put(4, Collections.singletonList(GenericRowData.of(4, fromString("Fabian"))));
            accessCounter = new HashMap<>();
        }

        protected int counter(int id) {
            int currentCnt = accessCounter.computeIfAbsent(id, key -> 0) + 1;
            accessCounter.put(id, currentCnt);
            return currentCnt;
        }

        protected void collectUpdatedRow(
                RowData originalRow, int currentCnt, Collector<RowData> out) {
            if (currentCnt > 1) {
                out.collect(
                        GenericRowData.of(
                                originalRow.getInt(0) * currentCnt,
                                fromString(originalRow.getString(1) + "-" + currentCnt)));
            } else {
                out.collect(originalRow);
            }
        }

        protected List<GenericRowData> lookup(int id) {
            return baseData.get(id);
        }

        @Override
        public void flatMap(RowData value, Collector<RowData> out) throws Exception {
            int id = value.getInt(0);
            int currentCnt = counter(id);
            List<GenericRowData> rows = lookup(id);
            if (rows != null) {
                for (int i = 0; i < rows.size(); i++) {
                    collectUpdatedRow(rows.get(i), currentCnt, out);
                }
            } else if (currentCnt > 1) {
                // return a default value for which lookup miss at 1st time
                out.collect(GenericRowData.of(currentCnt, fromString("default-" + currentCnt)));
            }
        }
    }
}
