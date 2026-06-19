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

package org.apache.flink.table.runtime.operators.join.deltajoin;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.deltajoin.DeltaJoinRuntimeTree.JoinNode;
import org.apache.flink.table.runtime.operators.join.deltajoin.LookupHandlerBase.Object2RowDataConverterResultFuture;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.row;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for cascaded {@link StreamingDeltaJoinOperator}.
 *
 * <p>Compared to tests on {@link StreamingBinaryDeltaJoinOperatorTest} that tests the binary delta
 * join logic and aec logic, this class focuses on cascaded delta join.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class StreamingCascadedDeltaJoinOperatorTest extends StreamingDeltaJoinOperatorTestBase {

    /**
     * Mock ddl like the following.
     *
     * <pre>
     *      CREATE TABLE A(
     *          a0 DOUBLE,
     *          a1 INT PRIMARY KEY NOT ENFORCED,
     *          a2 STRING
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE B(
     *          b1 INT,
     *          b0 DOUBLE,
     *          b2 STRING,
     *          INDEX(b1)
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE C(
     *          c1 INT,
     *          c2 STRING,
     *          c0 DOUBLE PRIMARY KEY NOT ENFORCED,
     *          INDEX(c1)
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE D(
     *          d2 STRING,
     *          d0 DOUBLE,
     *          d1 INT,
     *          INDEX(d1),
     *          INDEX(d0)
     *      )
     * </pre>
     */
    private static final Map<Integer, RowType> tableRowTypeMap = new HashMap<>();

    private static final Map<Integer, RowDataKeySelector> tableUpsertKeySelector = new HashMap<>();

    static {
        tableRowTypeMap.put(
                0,
                RowType.of(
                        new LogicalType[] {
                            new DoubleType(), new IntType(), VarCharType.STRING_TYPE
                        },
                        new String[] {"a0", "a1", "a2"}));
        tableUpsertKeySelector.put(0, getKeySelector(new int[] {1}, tableRowTypeMap.get(0)));

        tableRowTypeMap.put(
                1,
                RowType.of(
                        new LogicalType[] {
                            new IntType(), new DoubleType(), VarCharType.STRING_TYPE
                        },
                        new String[] {"b1", "b0", "b2"}));
        tableUpsertKeySelector.put(1, getKeySelector(new int[] {0, 1, 2}, tableRowTypeMap.get(1)));

        tableRowTypeMap.put(
                2,
                RowType.of(
                        new LogicalType[] {
                            new IntType(), VarCharType.STRING_TYPE, new DoubleType()
                        },
                        new String[] {"c1", "c2", "c0"}));
        tableUpsertKeySelector.put(2, getKeySelector(new int[] {2}, tableRowTypeMap.get(2)));

        tableRowTypeMap.put(
                3,
                RowType.of(
                        new LogicalType[] {
                            VarCharType.STRING_TYPE, new DoubleType(), new IntType()
                        },
                        new String[] {"d2", "d0", "d1"}));
        tableUpsertKeySelector.put(3, getKeySelector(new int[] {0, 1, 2}, tableRowTypeMap.get(3)));
    }

    // the data snapshot of the tables when joining
    // <table idx, <uk, value>>
    private final Map<Integer, LinkedHashMap<RowData, RowData>> tableCurrentDataMap =
            new HashMap<>();

    private StreamingDeltaJoinOperator operator;
    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness;
    private RowDataHarnessAssertor assertor;

    @Parameter public boolean enableCache;

    @Parameters(name = "EnableCache = {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (testHarness != null) {
            testHarness.close();
        }

        tableCurrentDataMap.clear();

        MyAsyncFunction.getLookupInvokeCount().clear();
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testLHSWithTwoInputsProcessData() throws Exception {
        LHSTestSpec spec = new LHSTestSpec(false, false, false);
        prepareEnv(spec);

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(1.0, 1, "a-1", 1, 1.0, "b-1");
        // this record exists in table B but is filtered out in DT1
        insertTableData(1, row(1, 1.0, "b-2"));
        StreamRecord<RowData> leftRecordK1V2 = insertRecord(1.0, 1, "a-2", 1, 1.0, "b-3");
        StreamRecord<RowData> leftRecordK1V3 = updateAfterRecord(1.0, 1, "a-3", 1, 1.0, "b-3");

        testHarness.processElement1(leftRecordK1V1);
        testHarness.processElement1(leftRecordK1V2);
        testHarness.processElement1(leftRecordK1V3);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1, "c-1", 1.0);
        testHarness.processElement2(rightRecordK1V1);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(1.0, 1, "a-4"));

        StreamRecord<RowData> rightRecordK1V2 = insertRecord(1, "c-2", 1.0);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1", 1, "c-2", 1.0));
            expectedOutput.add(insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3", 1, "c-2", 1.0));
        } else {
            expectedOutput.add(insertRecord(1.0, 1, "a-4", 1, 1.0, "b-1", 1, "c-2", 1.0));
            expectedOutput.add(insertRecord(1.0, 1, "a-4", 1, 1.0, "b-3", 1, "c-2", 1.0));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(2, row(1, "c-3", 1.0));

        StreamRecord<RowData> leftRecordK1V4 = updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3");
        testHarness.processElement1(leftRecordK1V4);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3", 1, "c-2", 1.0));
        } else {
            expectedOutput.add(updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3", 1, "c-3", 1.0));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V3 = insertRecord(1, "c-3", 1.0);
        testHarness.processElement2(rightRecordK1V3);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1", 1, "c-3", 1.0));
            expectedOutput.add(insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3", 1, "c-3", 1.0));
            expectedOutput.add(insertRecord(1.0, 1, "a-4", 1, 1.0, "b-3", 1, "c-3", 1.0));
        } else {
            expectedOutput.add(insertRecord(1.0, 1, "a-4", 1, 1.0, "b-1", 1, "c-3", 1.0));
            expectedOutput.add(insertRecord(1.0, 1, "a-4", 1, 1.0, "b-3", 1, "c-3", 1.0));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        // validate aec
        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        // validate cache
        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowDataKeySelector leftJoinKeySelector = spec.getLeftJoinKeySelector();
            RowDataKeySelector leftUpsertKeySelector = spec.getLeftUpsertKeySelector();
            RowDataKeySelector rightJoinKeySelector = spec.getRightJoinKeySelector();
            RowDataKeySelector rightUpsertKeySelector = spec.getRightUpsertKeySelector();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            rightJoinKeySelector.getKey(rightRecordK1V3.getValue()),
                            Map.of(
                                    leftUpsertKeySelector.getKey(
                                            insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1").getValue()),
                                    insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1").getValue(),
                                    leftUpsertKeySelector.getKey(
                                            insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3").getValue()),
                                    insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3").getValue(),
                                    leftUpsertKeySelector.getKey(
                                            updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3")
                                                    .getValue()),
                                    updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3").getValue()));

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            leftJoinKeySelector.getKey(leftRecordK1V4.getValue()),
                            Map.of(
                                    rightUpsertKeySelector.getKey(rightRecordK1V3.getValue()),
                                    rightRecordK1V3.getValue()));

            verifyCacheData(spec, cache, expectedLeftCacheData, expectedRightCacheData, 3, 2, 4, 3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2).get()).isEqualTo(1);
        } else {
            verifyCacheData(
                    spec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2).get()).isEqualTo(4);
        }

        AsyncDeltaJoinRunner openedLeft2RightAsyncRunner = operator.getLeftTriggeredUserFunction();
        AsyncDeltaJoinRunner openedRight2LeftAsyncRunner = operator.getRightTriggeredUserFunction();

        assertThat(openedLeft2RightAsyncRunner.getAllProcessors().size())
                .isEqualTo(AEC_CAPACITY + 1);
        openedLeft2RightAsyncRunner
                .getAllProcessors()
                .forEach(
                        processor -> {
                            DeltaJoinHandlerBase handler =
                                    processor.getDeltaJoinHandlerChain().getHead();
                            assertThat(handler).isInstanceOf(BinaryLookupHandler.class);
                        });

        assertThat(openedRight2LeftAsyncRunner.getAllProcessors().size())
                .isEqualTo(AEC_CAPACITY + 1);
        openedRight2LeftAsyncRunner
                .getAllProcessors()
                .forEach(
                        processor -> {
                            DeltaJoinHandlerBase handler =
                                    processor.getDeltaJoinHandlerChain().getHead();
                            assertThat(handler).isInstanceOf(CascadedLookupHandler.class);
                            assertThat(handler.getNext()).isNotNull();

                            handler = handler.getNext();
                            assertThat(handler).isInstanceOf(CascadedLookupHandler.class);
                            assertThat(handler.getNext()).isNotNull();

                            handler = handler.getNext();
                            assertThat(handler).isInstanceOf(TailOutputDataHandler.class);
                        });
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testLHSWithTwoInputsProcessDataWithFilterBetweenJoinAndSource() throws Exception {
        prepareEnv(new LHSTestSpec(true, false, false));

        // this record exists in table A, but is filtered out in filter on A
        insertTableData(0, row(1.0, 1, "a-1"));
        // this record exists in table B, but is filtered out in filter on B
        insertTableData(1, row(1, 1.0, "b-1"));
        // this record exists in table B, but is filtered out in dt1
        insertTableData(1, row(1, 1000.0, "b-2"));

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(1000.0, 1, "a-2", 1, 1000.0, "b-3");
        StreamRecord<RowData> leftRecordK1V2 = insertRecord(1000.0, 1, "a-2", 1, 1000.0, "b-4");

        testHarness.processElement1(leftRecordK1V1);
        testHarness.processElement1(leftRecordK1V2);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1, "c-1", 1000.0);
        testHarness.processElement2(rightRecordK1V1);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V2 = updateAfterRecord(1, "c-2", 1200.0);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-2", 1, 1000.0, "b-3", 1, "c-2", 1200.0));
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-2", 1, 1000.0, "b-4", 1, "c-2", 1200.0));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(1000.0, 1, "a-3"));

        StreamRecord<RowData> rightRecordK1V3 = updateAfterRecord(1, "c-3", 1300.0);
        testHarness.processElement2(rightRecordK1V3);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(
                    updateAfterRecord(1000.0, 1, "a-2", 1, 1000.0, "b-3", 1, "c-3", 1300.0));
            expectedOutput.add(
                    updateAfterRecord(1000.0, 1, "a-2", 1, 1000.0, "b-4", 1, "c-3", 1300.0));
        } else {
            expectedOutput.add(
                    updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-3", 1, "c-3", 1300.0));
            expectedOutput.add(
                    updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-4", 1, "c-3", 1300.0));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V3 =
                updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-3");
        StreamRecord<RowData> leftRecordK1V4 =
                updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-4");
        testHarness.processElement1(leftRecordK1V3);
        testHarness.processElement1(leftRecordK1V4);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-3", 1, "c-2", 1200.0));
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-3", 1, "c-3", 1300.0));
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-4", 1, "c-2", 1200.0));
        expectedOutput.add(updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-4", 1, "c-3", 1300.0));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testLHSWithTwoInputsProcessDataWithFilterBothBetweenJoinAndSourceAndCascadedJoins()
            throws Exception {
        prepareEnv(new LHSTestSpec(true, true, false));

        // this record exists in table A, but is filtered out in filter on A
        insertTableData(0, row(99.0, 1, "a-1"));
        // this record exists in table B, but is filtered out in filter on B
        insertTableData(1, row(1, 199.0, "b-1"));
        // this record exists in table B, but is filtered out in dt1
        insertTableData(1, row(1, 1000.0, "b-2"));
        // this record exists in table B, but is filtered out in filter after dt1
        insertTableData(1, row(1, 599.0, "b-3"));
        // this record exists in table B, but is filtered out in filter after dt1
        insertTableData(1, row(1, 800.0, "b-4"));

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1, "c-1", 1000.0);
        testHarness.processElement2(rightRecordK1V1);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V2 = updateAfterRecord(1, "c-2", 1200.0);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(300.0, 1, "a-2"));

        StreamRecord<RowData> rightRecordK1V3 = updateAfterRecord(1, "c-3", 1300.0);
        testHarness.processElement2(rightRecordK1V3);
        testHarness.endAllInputs();
        if (!enableCache) {
            expectedOutput.add(
                    updateAfterRecord(300.0, 1, "a-2", 1, 800.0, "b-4", 1, "c-3", 1300.0));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(2, row(1, "c-4", 200.0));

        StreamRecord<RowData> leftRecordK1V1 = updateAfterRecord(300.0, 1, "a-2", 1, 800.0, "b-4");
        testHarness.processElement1(leftRecordK1V1);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(300.0, 1, "a-2", 1, 800.0, "b-4", 1, "c-2", 1200.0));
        expectedOutput.add(updateAfterRecord(300.0, 1, "a-2", 1, 800.0, "b-4", 1, "c-3", 1300.0));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *       DT2
     *     /    \
     *    C     DT1
     *        /    \
     *       A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testRHSWithTwoInputsProcessData() throws Exception {
        RHSTestSpec spec = new RHSTestSpec(false, false);
        prepareEnv(spec);

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1.0, 1, "a-1", 1, 1.0, "b-1");
        // this record exists in table B but is filtered out in DT1
        insertTableData(1, row(1, 1.0, "b-2"));
        StreamRecord<RowData> rightRecordK1V2 = insertRecord(1.0, 1, "a-2", 1, 1.0, "b-3");
        StreamRecord<RowData> rightRecordK1V3 = updateAfterRecord(1.0, 1, "a-3", 1, 1.0, "b-3");

        testHarness.processElement2(rightRecordK1V1);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.processElement2(rightRecordK1V3);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(1, "c-1", 1.0);
        testHarness.processElement1(leftRecordK1V1);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(1.0, 1, "a-4"));

        StreamRecord<RowData> leftRecordK1V2 = insertRecord(1, "c-2", 1.0);
        testHarness.processElement1(leftRecordK1V2);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(insertRecord(1, "c-2", 1.0, 1.0, 1, "a-3", 1, 1.0, "b-1"));
            expectedOutput.add(insertRecord(1, "c-2", 1.0, 1.0, 1, "a-3", 1, 1.0, "b-3"));
        } else {
            expectedOutput.add(insertRecord(1, "c-2", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-1"));
            expectedOutput.add(insertRecord(1, "c-2", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-3"));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(2, row(1, "c-3", 1.0));

        StreamRecord<RowData> rightRecordK1V4 = updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3");
        testHarness.processElement2(rightRecordK1V4);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(updateAfterRecord(1, "c-2", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-3"));
        } else {
            expectedOutput.add(updateAfterRecord(1, "c-3", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-3"));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V3 = insertRecord(1, "c-3", 1.0);
        testHarness.processElement1(leftRecordK1V3);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(insertRecord(1, "c-3", 1.0, 1.0, 1, "a-3", 1, 1.0, "b-1"));
            expectedOutput.add(insertRecord(1, "c-3", 1.0, 1.0, 1, "a-3", 1, 1.0, "b-3"));
            expectedOutput.add(insertRecord(1, "c-3", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-3"));
        } else {
            expectedOutput.add(insertRecord(1, "c-3", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-1"));
            expectedOutput.add(insertRecord(1, "c-3", 1.0, 1.0, 1, "a-4", 1, 1.0, "b-3"));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        // validate aec
        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        // validate cache
        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowDataKeySelector leftJoinKeySelector = spec.getLeftJoinKeySelector();
            RowDataKeySelector leftUpsertKeySelector = spec.getLeftUpsertKeySelector();
            RowDataKeySelector rightJoinKeySelector = spec.getRightJoinKeySelector();
            RowDataKeySelector rightUpsertKeySelector = spec.getRightUpsertKeySelector();

            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            rightJoinKeySelector.getKey(rightRecordK1V4.getValue()),
                            Map.of(
                                    leftUpsertKeySelector.getKey(leftRecordK1V3.getValue()),
                                    leftRecordK1V3.getValue()));

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            leftJoinKeySelector.getKey(leftRecordK1V3.getValue()),
                            Map.of(
                                    rightUpsertKeySelector.getKey(
                                            insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1").getValue()),
                                    insertRecord(1.0, 1, "a-3", 1, 1.0, "b-1").getValue(),
                                    rightUpsertKeySelector.getKey(
                                            insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3").getValue()),
                                    insertRecord(1.0, 1, "a-3", 1, 1.0, "b-3").getValue(),
                                    rightUpsertKeySelector.getKey(
                                            updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3")
                                                    .getValue()),
                                    updateAfterRecord(1.0, 1, "a-4", 1, 1.0, "b-3").getValue()));

            verifyCacheData(spec, cache, expectedLeftCacheData, expectedRightCacheData, 4, 3, 3, 2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2).get()).isEqualTo(1);
        } else {
            verifyCacheData(
                    spec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2).get()).isEqualTo(4);
        }

        AsyncDeltaJoinRunner openedLeft2RightAsyncRunner = operator.getLeftTriggeredUserFunction();
        AsyncDeltaJoinRunner openedRight2LeftAsyncRunner = operator.getRightTriggeredUserFunction();

        assertThat(openedLeft2RightAsyncRunner.getAllProcessors().size())
                .isEqualTo(AEC_CAPACITY + 1);
        openedLeft2RightAsyncRunner
                .getAllProcessors()
                .forEach(
                        processor -> {
                            DeltaJoinHandlerBase handler =
                                    processor.getDeltaJoinHandlerChain().getHead();
                            assertThat(handler).isInstanceOf(CascadedLookupHandler.class);
                            assertThat(handler.getNext()).isNotNull();

                            handler = handler.getNext();
                            assertThat(handler).isInstanceOf(CascadedLookupHandler.class);
                            assertThat(handler.getNext()).isNotNull();

                            handler = handler.getNext();
                            assertThat(handler).isInstanceOf(TailOutputDataHandler.class);
                        });

        assertThat(openedRight2LeftAsyncRunner.getAllProcessors().size())
                .isEqualTo(AEC_CAPACITY + 1);
        openedRight2LeftAsyncRunner
                .getAllProcessors()
                .forEach(
                        processor -> {
                            DeltaJoinHandlerBase handler =
                                    processor.getDeltaJoinHandlerChain().getHead();
                            assertThat(handler).isInstanceOf(BinaryLookupHandler.class);
                        });
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *       DT2
     *     /    \
     *    C     DT1
     *        /    \
     *       A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testRHSWithTwoInputsProcessDataWithFilterBetweenJoinAndSource() throws Exception {
        prepareEnv(new RHSTestSpec(true, false));

        // this record exists in table A, but is filtered out in filter on A
        insertTableData(0, row(1.0, 1, "a-1"));
        // this record exists in table B, but is filtered out in filter on B
        insertTableData(1, row(1, 1.0, "b-1"));
        // this record exists in table B, but is filtered out in dt1
        insertTableData(1, row(1, 1000.0, "b-2"));

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1000.0, 1, "a-2", 1, 1000.0, "b-3");
        StreamRecord<RowData> rightRecordK1V2 = insertRecord(1000.0, 1, "a-2", 1, 1000.0, "b-4");

        testHarness.processElement2(rightRecordK1V1);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(1, "c-1", 1000.0);
        testHarness.processElement1(leftRecordK1V1);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V2 = updateAfterRecord(1, "c-2", 1200.0);
        testHarness.processElement1(leftRecordK1V2);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(1, "c-2", 1200.0, 1000.0, 1, "a-2", 1, 1000.0, "b-3"));
        expectedOutput.add(updateAfterRecord(1, "c-2", 1200.0, 1000.0, 1, "a-2", 1, 1000.0, "b-4"));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(1000.0, 1, "a-3"));

        StreamRecord<RowData> leftRecordK1V3 = updateAfterRecord(1, "c-3", 1300.0);
        testHarness.processElement1(leftRecordK1V3);
        testHarness.endAllInputs();
        if (enableCache) {
            expectedOutput.add(
                    updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-2", 1, 1000.0, "b-3"));
            expectedOutput.add(
                    updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-2", 1, 1000.0, "b-4"));
        } else {
            expectedOutput.add(
                    updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-3", 1, 1000.0, "b-3"));
            expectedOutput.add(
                    updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-3", 1, 1000.0, "b-4"));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V3 =
                updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-3");
        StreamRecord<RowData> rightRecordK1V4 =
                updateAfterRecord(1000.0, 1, "a-3", 1, 1000.0, "b-4");
        testHarness.processElement2(rightRecordK1V3);
        testHarness.processElement2(rightRecordK1V4);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(1, "c-2", 1200.0, 1000.0, 1, "a-3", 1, 1000.0, "b-3"));
        expectedOutput.add(updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-3", 1, 1000.0, "b-3"));
        expectedOutput.add(updateAfterRecord(1, "c-2", 1200.0, 1000.0, 1, "a-3", 1, 1000.0, "b-4"));
        expectedOutput.add(updateAfterRecord(1, "c-3", 1300.0, 1000.0, 1, "a-3", 1, 1000.0, "b-4"));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *       DT2
     *     /    \
     *    C     DT1
     *        /    \
     *       A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testRHSWithTwoInputsProcessDataWithFilterBothBetweenJoinAndSourceAndCascadedJoins()
            throws Exception {
        prepareEnv(new RHSTestSpec(true, true));

        // this record exists in table A, but is filtered out in filter on A
        insertTableData(0, row(99.0, 1, "a-1"));
        // this record exists in table B, but is filtered out in filter on B
        insertTableData(1, row(1, 199.0, "b-1"));
        // this record exists in table B, but is filtered out in dt1
        insertTableData(1, row(1, 1000.0, "b-2"));
        // this record exists in table B, but is filtered out in filter after dt1
        insertTableData(1, row(1, 599.0, "b-3"));
        // this record exists in table B, but is filtered out in filter after dt1
        insertTableData(1, row(1, 800.0, "b-4"));

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(1, "c-1", 1000.0);
        testHarness.processElement1(leftRecordK1V1);
        testHarness.endAllInputs();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V2 = updateAfterRecord(1, "c-2", 1200.0);
        testHarness.processElement1(leftRecordK1V2);
        testHarness.endAllInputs();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(0, row(300.0, 1, "a-2"));

        StreamRecord<RowData> leftRecordK1V3 = updateAfterRecord(1, "c-3", 1300.0);
        testHarness.processElement1(leftRecordK1V3);
        testHarness.endAllInputs();
        if (!enableCache) {
            expectedOutput.add(
                    updateAfterRecord(1, "c-3", 1300.0, 300.0, 1, "a-2", 1, 800.0, "b-4"));
        }
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        insertTableData(2, row(1, "c-4", 200.0));

        StreamRecord<RowData> rightRecordK1V1 = updateAfterRecord(300.0, 1, "a-2", 1, 800.0, "b-4");
        testHarness.processElement2(rightRecordK1V1);
        testHarness.endAllInputs();
        expectedOutput.add(updateAfterRecord(1, "c-2", 1200.0, 300.0, 1, "a-2", 1, 800.0, "b-4"));
        expectedOutput.add(updateAfterRecord(1, "c-3", 1300.0, 300.0, 1, "a-2", 1, 800.0, "b-4"));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());
    }

    /**
     * Test lookup chain with the order "C -> D -> B -> A" and the following join tree.
     *
     * <pre>
     *
     *   inner/left/right/full DT3
     *     /               \
     *    C           inner/left/right/full DT2
     *                  /               \
     *                 D      inner/left/right/full DT1
     *                                /    \
     *                               A      B
     * </pre>
     */
    @TestTemplate
    void testMultiCascadedHandlers() throws Exception {
        prepareEnv(new ThirdCascadedWithOrderCDBATestSpec());

        insertTableData(3, row("d-1", 1.0, 1));
        insertTableData(3, row("d-2", 1.0, 2));
        insertTableData(3, row("d-3", 1.0, 3));
        insertTableData(3, row("d-4", 2.0, 4));
        insertTableData(3, row("d-5", 1.0, 89));
        insertTableData(3, row("d-99", 99.0, 99));

        insertTableData(1, row(1, 1.0, "b-1"));
        insertTableData(1, row(1, 2.0, "b-2"));
        insertTableData(1, row(1, 3.0, "b-3"));
        insertTableData(1, row(2, 4.0, "b-1"));
        insertTableData(1, row(2, 5.0, "b-2"));
        insertTableData(1, row(2, 6.0, "b-3"));
        insertTableData(1, row(3, 7.0, "b-1"));
        insertTableData(1, row(3, 8.0, "b-2"));
        insertTableData(1, row(3, 9.0, "b-3"));
        insertTableData(1, row(4, 10.0, "b-4"));
        insertTableData(1, row(199, 199.0, "b-199"));

        insertTableData(0, row(1.0, 1, "a-1"));
        insertTableData(0, row(2.0, 2, "a-2"));
        insertTableData(0, row(3.0, 3, "a-3"));
        insertTableData(0, row(4.0, 4, "a-4"));
        insertTableData(0, row(299.0, 299, "a-299"));

        StreamRecord<RowData> leftInput1 = insertRecord(1, "c-1", 1.0);
        testHarness.processElement1(leftInput1);
        testHarness.endAllInputs();

        //               /- b-1 -- a-1 x filtered
        //         - d-1 -- b-2 -- a-1 x filtered
        //        /      \- b-3 -- a-1 x filtered
        //      /        /- b-1 -- a-2
        // c-1  ---- d-2 -- b-2 -- a-2 x filtered
        //      \        \- b-3 -- a-2
        //      | \      /- b-1 -- a-3
        //      \  - d-3 -- b-2 -- a-3 x filtered
        //      |        \- b-3 -- a-3
        //       \
        //         - d-5 -- N/A -- N/A x filtered
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(
                insertRecord(1, "c-1", 1.0, "d-2", 1.0, 2, 2.0, 2, "a-2", 2, 4.0, "b-1"));
        expectedOutput.add(
                insertRecord(1, "c-1", 1.0, "d-2", 1.0, 2, 2.0, 2, "a-2", 2, 6.0, "b-3"));
        expectedOutput.add(
                insertRecord(1, "c-1", 1.0, "d-3", 1.0, 3, 3.0, 3, "a-3", 3, 7.0, "b-1"));
        expectedOutput.add(
                insertRecord(1, "c-1", 1.0, "d-3", 1.0, 3, 3.0, 3, "a-3", 3, 9.0, "b-3"));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        // all are filtered by "c2 <> 'c-2'"
        StreamRecord<RowData> leftInput2 = updateAfterRecord(1, "c-2", 1.0);
        testHarness.processElement1(leftInput2);
        testHarness.endAllInputs();

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftInput3 = updateAfterRecord(1, "c-3", 1.0);
        testHarness.processElement1(leftInput3);
        testHarness.endAllInputs();

        expectedOutput.add(
                updateAfterRecord(1, "c-3", 1.0, "d-2", 1.0, 2, 2.0, 2, "a-2", 2, 4.0, "b-1"));
        expectedOutput.add(
                updateAfterRecord(1, "c-3", 1.0, "d-2", 1.0, 2, 2.0, 2, "a-2", 2, 6.0, "b-3"));
        expectedOutput.add(
                updateAfterRecord(1, "c-3", 1.0, "d-3", 1.0, 3, 3.0, 3, "a-3", 3, 7.0, "b-1"));
        expectedOutput.add(
                updateAfterRecord(1, "c-3", 1.0, "d-3", 1.0, 3, 3.0, 3, "a-3", 3, 9.0, "b-3"));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        if (enableCache) {
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(4);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2)).isNull();
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(3).get()).isEqualTo(1);
        } else {
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(9);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(12);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(2)).isNull();
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(3).get()).isEqualTo(3);
        }
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testLookupFunctionThrowsException() throws Exception {
        prepareEnv(new LHSTestSpec(false, false, true));

        AtomicReference<Throwable> latestException = new AtomicReference<>(null);
        // set external failure cause consumer to prevent hang
        // DO NOT throw exception up again to avoid hang
        testHarness
                .getEnvironment()
                // DO NOT throw exception up again to avoid hang
                .setExternalFailureCauseConsumer(latestException::set);

        insertTableData(1, row(1, 1.0, "b-2"));

        StreamRecord<RowData> rightRecordK1V1 = insertRecord(1, "c-1", 1000.0);
        testHarness.processElement2(rightRecordK1V1);

        testHarness.endAllInputs();

        // Exception: Could not complete the stream element: ...
        // +- RuntimeException: Failed to look up table
        //   +- ExpectedTestException
        assertThat(latestException.get()).isNotNull();
        assertThat(latestException.get().getCause().getCause())
                .isInstanceOf(ExpectedTestException.class);
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while C comes data.
     */
    @TestTemplate
    void testOmitCalcCollectorWhenLookupIfNecessary() throws Exception {
        insertTableData(0, row(1000.0, 1, "a"));
        insertTableData(1, row(1, 1000.0, "b"));

        StreamRecord<RowData> rightRecord = insertRecord(1, "c", 1000.0);

        // if there are no calc between source and delta join, the calc collector can be omitted
        prepareEnv(new LHSTestSpec(false, false, false));
        testHarness.processElement2(rightRecord);
        testHarness.endAllInputs();

        MyAsyncFunction rightAsyncFunc1 = unwrapAsyncFunctions(operator, false, 0).get(0);
        validateCalcFunctionAndCollectorWhenLookup(rightAsyncFunc1.getLastResultFuture(), true);
        MyAsyncFunction rightAsyncFunc2 = unwrapAsyncFunctions(operator, false, 1).get(0);
        validateCalcFunctionAndCollectorWhenLookup(rightAsyncFunc2.getLastResultFuture(), true);
        testHarness.close();

        // if there is a calc between source and delta join, the calc collector cannot be omitted
        prepareEnv(new LHSTestSpec(true, true, false));
        testHarness.processElement2(rightRecord);
        testHarness.endAllInputs();

        rightAsyncFunc1 = unwrapAsyncFunctions(operator, false, 0).get(0);
        validateCalcFunctionAndCollectorWhenLookup(rightAsyncFunc1.getLastResultFuture(), false);
        rightAsyncFunc2 = unwrapAsyncFunctions(operator, false, 1).get(0);
        validateCalcFunctionAndCollectorWhenLookup(rightAsyncFunc2.getLastResultFuture(), false);
        testHarness.close();
    }

    /**
     * The Join tree used to test is as following.
     *
     * <pre>
     *       DT2
     *     /    \
     *    C     DT1
     *        /    \
     *       A      B
     *
     *    when records from C come, lookup chain is as following:
     *    C -> B -> A
     * </pre>
     *
     * <p>Here we mainly test DT2 while two inputs come data.
     */
    @TestTemplate
    void testRowDataSerializerAreAlwaysSameInCalcCollector() throws Exception {
        prepareEnv(new RHSTestSpec(true, true));

        insertTableData(0, row(1000.0, 1, "a1"));
        insertTableData(0, row(2000.0, 2, "a2"));
        insertTableData(1, row(1, 1000.0, "b1"));
        insertTableData(1, row(2, 2000.0, "b2"));

        StreamRecord<RowData> leftRecord1 = insertRecord(1, "c1", 1001.0);
        testHarness.processElement1(leftRecord1);
        testHarness.endAllInputs();

        MyAsyncFunction firstHandlerAsyncFunc1 = unwrapAsyncFunctions(operator, true, 0).get(0);
        assertThat(firstHandlerAsyncFunc1.getLastResultFuture()).isNotNull();
        Object2RowDataConverterResultFuture firstHandlerResultFuture1 =
                (Object2RowDataConverterResultFuture) firstHandlerAsyncFunc1.getLastResultFuture();

        MyAsyncFunction secondHandlerAsyncFunc1 = unwrapAsyncFunctions(operator, true, 1).get(0);
        assertThat(secondHandlerAsyncFunc1.getLastResultFuture()).isNotNull();
        Object2RowDataConverterResultFuture secondHandlerResultFuture1 =
                (Object2RowDataConverterResultFuture) secondHandlerAsyncFunc1.getLastResultFuture();

        DeltaJoinRuntimeTree joinTree1 =
                unwrapProcessor(operator, true).get(0).getMultiInputRowDataBuffer().getJoinTree();

        StreamRecord<RowData> leftRecord2 = insertRecord(2, "c2", 2002.0);
        testHarness.processElement1(leftRecord2);
        testHarness.endAllInputs();

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(insertRecord(1, "c1", 1001.0, 1000.0, 1, "a1", 1, 1000.0, "b1"));
        expectedOutput.add(insertRecord(2, "c2", 2002.0, 2000.0, 2, "a2", 2, 2000.0, "b2"));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        MyAsyncFunction firstHandlerAsyncFunc2 = unwrapAsyncFunctions(operator, true, 0).get(1);
        assertThat(firstHandlerAsyncFunc2.getLastResultFuture()).isNotNull();
        Object2RowDataConverterResultFuture firstHandlerResultFuture2 =
                (Object2RowDataConverterResultFuture) firstHandlerAsyncFunc2.getLastResultFuture();

        MyAsyncFunction secondHandlerAsyncFunc2 = unwrapAsyncFunctions(operator, true, 1).get(1);
        assertThat(secondHandlerAsyncFunc2.getLastResultFuture()).isNotNull();
        Object2RowDataConverterResultFuture secondHandlerResultFuture2 =
                (Object2RowDataConverterResultFuture) secondHandlerAsyncFunc2.getLastResultFuture();

        DeltaJoinRuntimeTree joinTree2 =
                unwrapProcessor(operator, true).get(1).getMultiInputRowDataBuffer().getJoinTree();

        // validate first lookup handler
        assertThat(firstHandlerResultFuture1).isNotSameAs(firstHandlerResultFuture2);
        assertThat(firstHandlerResultFuture1.getCalcCollector()).isNotNull();
        assertThat(firstHandlerResultFuture2.getCalcCollector()).isNotNull();
        assertThat(firstHandlerResultFuture1.getCalcCollector().getLookupResultRowSerializer())
                .isSameAs(
                        firstHandlerResultFuture2
                                .getCalcCollector()
                                .getLookupResultRowSerializer());

        // validate second lookup handler
        assertThat(secondHandlerResultFuture1).isNotSameAs(secondHandlerResultFuture2);
        assertThat(secondHandlerResultFuture1.getCalcCollector()).isNotNull();
        assertThat(secondHandlerResultFuture2.getCalcCollector()).isNotNull();
        assertThat(secondHandlerResultFuture1.getCalcCollector().getLookupResultRowSerializer())
                .isSameAs(
                        secondHandlerResultFuture2
                                .getCalcCollector()
                                .getLookupResultRowSerializer());

        // validate join tree
        //
        //           Join without calc
        //          /                  \
        //  Binary with calc    Join without calc
        //                       /              \
        //              Binary with calc     Binary with calc
        assertThat(joinTree1).isNotSameAs(joinTree2);

        JoinNode topJoin1 = (JoinNode) joinTree1.root;
        JoinNode topJoin2 = (JoinNode) joinTree2.root;
        assertThat(topJoin1.rowDataSerializerPassThroughCalc)
                .isSameAs(topJoin2.rowDataSerializerPassThroughCalc);
        assertThat(topJoin1.left.rowDataSerializerPassThroughCalc)
                .isSameAs(topJoin2.left.rowDataSerializerPassThroughCalc);

        JoinNode bottomJoin1 = (JoinNode) topJoin1.right;
        JoinNode bottomJoin2 = (JoinNode) topJoin2.right;
        assertThat(bottomJoin1.rowDataSerializerPassThroughCalc)
                .isSameAs(bottomJoin2.rowDataSerializerPassThroughCalc);
        assertThat(bottomJoin1.left.rowDataSerializerPassThroughCalc)
                .isSameAs(bottomJoin2.left.rowDataSerializerPassThroughCalc);
        assertThat(bottomJoin1.right.rowDataSerializerPassThroughCalc)
                .isSameAs(bottomJoin2.right.rowDataSerializerPassThroughCalc);
    }

    /** Abstract test specification for cascaded delta join operator tests. */
    private abstract static class CascadedTestSpec extends AbstractTestSpec {

        abstract int[] getEachBinaryInputFieldSize();

        abstract DeltaJoinHandlerChain getLeft2RightHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector);

        abstract DeltaJoinHandlerChain getRight2LeftHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector);

        abstract @Nullable GeneratedFilterCondition getRemainingJoinCondition();

        abstract DeltaJoinRuntimeTree getJoinRuntimeTree();

        abstract Set<Set<Integer>> getLeft2RightDrivenSideInfo();

        abstract Set<Set<Integer>> getRight2LeftDrivenSideInfo();

        void insertTableDataOnEmit(int inputIdx, RowData rowData) {}
    }

    private void prepareEnv(CascadedTestSpec spec) throws Exception {
        testHarness = createCascadedDeltaJoinOperatorTestHarness(spec);

        testHarness.setup();
        testHarness.open();
        operator = unwrapOperator(testHarness);
        operator.setAsyncExecutionController(
                new MyAsyncExecutionControllerDelegate(
                        operator.getAsyncExecutionController(), true, spec::insertTableDataOnEmit));

        assertor = createAssertor(spec.getOutputRowType());
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createCascadedDeltaJoinOperatorTestHarness(CascadedTestSpec spec) throws Exception {
        Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector =
                new HashMap<>();
        DeltaJoinHandlerChain left2RightHandlerChain =
                spec.getLeft2RightHandlerChain(fetcherCollector);
        DeltaJoinHandlerChain right2LeftHandlerChain =
                spec.getRight2LeftHandlerChain(fetcherCollector);

        return createDeltaJoinOperatorTestHarness(
                spec.getEachBinaryInputFieldSize(),
                left2RightHandlerChain,
                right2LeftHandlerChain,
                spec.getRemainingJoinCondition(),
                spec.getJoinRuntimeTree(),
                spec.getLeft2RightDrivenSideInfo(),
                spec.getRight2LeftDrivenSideInfo(),
                spec.getLeftJoinKeySelector(),
                spec.getLeftUpsertKeySelector(),
                spec.getRightJoinKeySelector(),
                spec.getRightUpsertKeySelector(),
                fetcherCollector,
                spec.getLeftInputTypeInfo(),
                spec.getRightInputTypeInfo(),
                enableCache);
    }

    /**
     * Test spec for LHS cascaded join.
     *
     * <p>If there is not a filter on binary input, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select *
     *          from A
     *      join B
     *          on a1 = b1 and b2 <> 'b-2'
     *      join C
     *          on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>If there is a filter on binary input and no calc on cascaded joins, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select * from (
     *          select * from A where a0 >= 100.0
     *      )
     *      join (
     *          select * from B where b0 >= 200.0
     *      )
     *      on a1 = b1 and b2 <> 'b-2'
     *      join (
     *          select * from C where c0 >= 300.0
     *      )
     *      on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>If there is a filter on binary input and a calc on cascaded joins, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select * from (
     *          select * from (
     *              select * from A where a0 >= 100.0
     *          ) join (
     *              select * from B where b0 >= 200.0
     *          )
     *          on a1 = b1 and b2 <> 'b-2'
     *          where a0 + b0 >= 900.0
     *      )
     *      join (
     *          select * from C where c0 >= 300.0
     *      )
     *      on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>The join tree is like:
     *
     * <pre>
     *             DT2
     *           /    \
     *         DT1     C
     *       /    \
     *      A      B
     * </pre>
     */
    private class LHSTestSpec extends CascadedTestSpec {

        private final boolean containsFilterOnTable;
        private final boolean containsFilterBetweenCascadedJoins;
        private final boolean throwExceptionWhenLookingUpFromB2A;

        LHSTestSpec(
                boolean containsFilterOnTable,
                boolean containsFilterBetweenCascadedJoins,
                boolean throwExceptionWhenLookingUpFromB2A) {
            Preconditions.checkArgument(
                    containsFilterOnTable || !containsFilterBetweenCascadedJoins,
                    "Unsupported pattern in LHSTestSpec");
            this.containsFilterOnTable = containsFilterOnTable;
            this.containsFilterBetweenCascadedJoins = containsFilterBetweenCascadedJoins;
            this.throwExceptionWhenLookingUpFromB2A = throwExceptionWhenLookingUpFromB2A;
        }

        @Override
        int[] getEachBinaryInputFieldSize() {
            return new int[] {3, 3, 3};
        }

        @Override
        RowType getLeftInputRowType() {
            return combineSourceRowTypes(0, 1);
        }

        @Override
        RowType getRightInputRowType() {
            return tableRowTypeMap.get(2);
        }

        @Override
        int[] getLeftJoinKeyIndices() {
            // left jk: b1
            return new int[] {3};
        }

        @Override
        Optional<int[]> getLeftUpsertKey() {
            // left uk: none
            return Optional.empty();
        }

        @Override
        int[] getRightJoinKeyIndices() {
            // right jk: c1
            return new int[] {0};
        }

        @Override
        Optional<int[]> getRightUpsertKey() {
            // right uk: c0
            return Optional.of(new int[] {2});
        }

        @Override
        DeltaJoinHandlerChain getLeft2RightHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType dt1OutType = combineSourceRowTypes(0, 1);
            RowType cInOutType = tableRowTypeMap.get(2);
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnC =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(300.0, 2) : null;
            return buildBinaryChain(
                    LookupTestSpec.builder()
                            .withSourceInputs(0, 1)
                            .withTargetInput(2)
                            .withTargetTableIdx(2)
                            .withSourceLookupKeyIdx(3)
                            .withSourceRowType(dt1OutType)
                            .withTargetLookupKeyIdx(0)
                            .withTargetRowType(cInOutType)
                            .withTargetGeneratedCalc(generatedCalcOnC)
                            .build(),
                    fetcherCollector);
        }

        @Override
        DeltaJoinHandlerChain getRight2LeftHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType cInOutType = tableRowTypeMap.get(2);
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnA =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(100.0, 0) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnB =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(200.0, 1) : null;
            return buildCascadedChain(
                    Arrays.asList(
                            LookupTestSpec.builder()
                                    .withSourceInputs(2)
                                    .withTargetInput(1)
                                    .withTargetTableIdx(1)
                                    .withSourceLookupKeyIdx(0)
                                    .withSourceRowType(cInOutType)
                                    .withTargetLookupKeyIdx(0)
                                    .withTargetRowType(bInOutType)
                                    .withTargetGeneratedCalc(generatedCalcOnB)
                                    .build(),
                            LookupTestSpec.builder()
                                    .withSourceInputs(1)
                                    .withTargetInput(0)
                                    .withTargetTableIdx(0)
                                    .withSourceLookupKeyIdx(0)
                                    .withSourceRowType(bInOutType)
                                    .withTargetLookupKeyIdx(1)
                                    .withTargetRowType(aInOutType)
                                    .withTargetGeneratedCalc(generatedCalcOnA)
                                    // b2 <> 'b-2'
                                    .withGeneratedRemainingCondition(
                                            getFilterCondition(
                                                    // A + B
                                                    combineSourceRowTypes(0, 1),
                                                    new int[] {5},
                                                    lookupResult ->
                                                            !lookupResult
                                                                    .getString(0)
                                                                    .toString()
                                                                    .equals("b-2")))
                                    .expectedThrownException(
                                            throwExceptionWhenLookingUpFromB2A
                                                    ? new ExpectedTestException()
                                                    : null)
                                    .build()),
                    new int[] {0, 1},
                    new int[] {2},
                    fetcherCollector);
        }

        @Override
        @Nullable
        GeneratedFilterCondition getRemainingJoinCondition() {
            // c1 = b1 and c2 <> 'c-1'
            return getFilterCondition(
                    // A + B + C
                    getOutputRowType(),
                    new int[] {6, 3, 7},
                    row ->
                            row.getInt(0) == row.getInt(1)
                                    && !row.getString(2).toString().equals("c-1"));
        }

        @Override
        DeltaJoinRuntimeTree getJoinRuntimeTree() {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType cInOutType = tableRowTypeMap.get(2);

            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnA =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(100.0, 0) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnB =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(200.0, 1) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnC =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(300.0, 2) : null;

            DeltaJoinRuntimeTree.BinaryInputNode nodeA =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            0, generatedCalcOnA, InternalSerializers.create(aInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeB =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            1, generatedCalcOnB, InternalSerializers.create(bInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeC =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            2, generatedCalcOnC, InternalSerializers.create(cInOutType));

            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnDT1 =
                    containsFilterBetweenCascadedJoins
                            ? createFlatMap(
                                    rowData ->
                                            rowData.getDouble(0) + rowData.getDouble(4) > 900.0
                                                    ? Optional.of(rowData)
                                                    : Optional.empty())
                            : null;

            DeltaJoinRuntimeTree.JoinNode nodeDT1 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    getLeftInputRowType(),
                                    new int[] {1, 3, 5},
                                    row ->
                                            row.getInt(0) == row.getInt(1)
                                                    && !row.getString(2).toString().equals("b-2")),
                            generatedCalcOnDT1,
                            nodeA,
                            nodeB,
                            InternalSerializers.create(getLeftInputRowType()));
            DeltaJoinRuntimeTree.JoinNode nodeDT2 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    getOutputRowType(),
                                    new int[] {6, 3, 7},
                                    row ->
                                            row.getInt(0) == row.getInt(1)
                                                    && !row.getString(2).toString().equals("c-1")),
                            null,
                            nodeDT1,
                            nodeC,
                            InternalSerializers.create(getOutputRowType()));
            return new DeltaJoinRuntimeTree(nodeDT2);
        }

        @Override
        Set<Set<Integer>> getLeft2RightDrivenSideInfo() {
            return Set.of(Set.of(0, 1));
        }

        @Override
        Set<Set<Integer>> getRight2LeftDrivenSideInfo() {
            return Set.of(Set.of(2), Set.of(1));
        }

        @Override
        void insertTableDataOnEmit(int inputIdx, RowData rowData) {
            if (inputIdx == 0) {
                // split A and B
                RowType dt1OutType = combineSourceRowTypes(0, 1);
                RowData dataOnA = projectRowData(rowData, dt1OutType, new int[] {0, 1, 2});
                insertTableData(0, dataOnA);
                RowData dataOnB = projectRowData(rowData, dt1OutType, new int[] {3, 4, 5});
                insertTableData(1, dataOnB);
            } else {
                insertTableData(2, rowData);
            }
        }
    }

    /**
     * Test spec for RHS cascaded join.
     *
     * <p>If there is not a filter on binary input, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select *
     *          from C
     *      join (
     *          select * from A
     *          join B
     *          on a1 = b1 and b2 <> 'b-2'
     *      )
     *      on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>If there is a filter on binary input, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select *
     *          from (
     *              select * from C where c0 > 300.0
     *          )
     *      join (
     *          select * from (
     *              select * from A where a0 > 100.0
     *          )
     *          join (
     *              select * from B where b0 > 200.0
     *          )
     *          on a1 = b1 and b2 <> 'b-2'
     *      )
     *      on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>If there is a filter on binary input and a calc on cascaded joins, the sql is:
     *
     * <pre>
     *  insert into snk
     *      select *
     *          from (
     *              select * from C where c0 > 300.0
     *          )
     *      join (
     *          select * from (
     *              select * from A where a0 > 100.0
     *          )
     *          join (
     *              select * from B where b0 > 200.0
     *          )
     *          on a1 = b1 and b2 <> 'b-2'
     *          where a0 + b0 >= 900.0
     *      )
     *      on c1 = b1 and c2 <> 'c-1'
     * </pre>
     *
     * <p>The join tree is like:
     *
     * <pre>
     *       DT2
     *     /    \
     *    C     DT1
     *        /    \
     *       A      B
     * </pre>
     */
    private class RHSTestSpec extends CascadedTestSpec {

        private final boolean containsFilterOnTable;
        private final boolean containsFilterBetweenCascadedJoins;

        RHSTestSpec(boolean containsFilterOnTable, boolean containsFilterBetweenCascadedJoins) {
            Preconditions.checkArgument(
                    containsFilterOnTable || !containsFilterBetweenCascadedJoins,
                    "Unsupported pattern in RHSTestSpec");
            this.containsFilterOnTable = containsFilterOnTable;
            this.containsFilterBetweenCascadedJoins = containsFilterBetweenCascadedJoins;
        }

        @Override
        int[] getEachBinaryInputFieldSize() {
            return new int[] {3, 3, 3};
        }

        @Override
        RowType getLeftInputRowType() {
            return tableRowTypeMap.get(2);
        }

        @Override
        RowType getRightInputRowType() {
            return combineSourceRowTypes(0, 1);
        }

        @Override
        int[] getLeftJoinKeyIndices() {
            // left jk: c1
            return new int[] {0};
        }

        @Override
        int[] getRightJoinKeyIndices() {
            // right jk: b1
            return new int[] {3};
        }

        @Override
        Optional<int[]> getLeftUpsertKey() {
            // left uk: c0
            return Optional.of(new int[] {2});
        }

        @Override
        Optional<int[]> getRightUpsertKey() {
            // right uk: none
            return Optional.empty();
        }

        @Override
        DeltaJoinHandlerChain getLeft2RightHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType cInOutType = tableRowTypeMap.get(2);
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnA =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(100.0, 0) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnB =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(200.0, 1) : null;
            return buildCascadedChain(
                    Arrays.asList(
                            LookupTestSpec.builder()
                                    .withSourceInputs(0)
                                    .withTargetInput(2)
                                    .withTargetTableIdx(1)
                                    .withSourceLookupKeyIdx(0)
                                    .withSourceRowType(cInOutType)
                                    .withTargetLookupKeyIdx(0)
                                    .withTargetRowType(bInOutType)
                                    .withTargetGeneratedCalc(generatedCalcOnB)
                                    .build(),
                            LookupTestSpec.builder()
                                    .withSourceInputs(2)
                                    .withTargetInput(1)
                                    .withTargetTableIdx(0)
                                    .withSourceLookupKeyIdx(0)
                                    .withSourceRowType(bInOutType)
                                    .withTargetLookupKeyIdx(1)
                                    .withTargetRowType(aInOutType)
                                    .withTargetGeneratedCalc(generatedCalcOnA)
                                    // b2 <> 'b-2'
                                    .withGeneratedRemainingCondition(
                                            getFilterCondition(
                                                    // A + B
                                                    combineSourceRowTypes(0, 1),
                                                    new int[] {5},
                                                    lookupResult ->
                                                            !lookupResult
                                                                    .getString(0)
                                                                    .toString()
                                                                    .equals("b-2")))
                                    .build()),
                    new int[] {1, 2},
                    new int[] {0},
                    fetcherCollector);
        }

        @Override
        DeltaJoinHandlerChain getRight2LeftHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType cInOutType = tableRowTypeMap.get(2);
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnC =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(300.0, 2) : null;
            return buildBinaryChain(
                    LookupTestSpec.builder()
                            .withSourceInputs(1, 2)
                            .withTargetInput(0)
                            .withTargetTableIdx(2)
                            .withSourceLookupKeyIdx(3)
                            .withSourceRowType(getRightInputRowType())
                            .withTargetLookupKeyIdx(0)
                            .withTargetRowType(cInOutType)
                            .withTargetGeneratedCalc(generatedCalcOnC)
                            .build(),
                    fetcherCollector);
        }

        @Override
        @Nullable
        GeneratedFilterCondition getRemainingJoinCondition() {
            // c1 = b1 and c2 <> 'c-1'
            return getFilterCondition(
                    // C + A & B
                    getOutputRowType(),
                    new int[] {0, 6, 1},
                    row ->
                            row.getInt(0) == row.getInt(1)
                                    && !row.getString(2).toString().equals("c-1"));
        }

        @Override
        DeltaJoinRuntimeTree getJoinRuntimeTree() {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType dt1OutType = combineSourceRowTypes(0, 1);
            RowType cInOutType = tableRowTypeMap.get(2);

            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnA =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(100.0, 0) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnB =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(200.0, 1) : null;
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnC =
                    containsFilterOnTable ? createDoubleGreaterThanFilter(300.0, 2) : null;

            DeltaJoinRuntimeTree.BinaryInputNode nodeA =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            1, generatedCalcOnA, InternalSerializers.create(aInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeB =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            2, generatedCalcOnB, InternalSerializers.create(bInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeC =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            0, generatedCalcOnC, InternalSerializers.create(cInOutType));

            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnDT1 =
                    containsFilterBetweenCascadedJoins
                            ? createFlatMap(
                                    rowData ->
                                            rowData.getDouble(0) + rowData.getDouble(4) > 900.0
                                                    ? Optional.of(rowData)
                                                    : Optional.empty())
                            : null;
            DeltaJoinRuntimeTree.JoinNode nodeDT1 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    dt1OutType,
                                    new int[] {1, 3, 5},
                                    row ->
                                            row.getInt(0) == row.getInt(1)
                                                    && !row.getString(2).toString().equals("b-2")),
                            generatedCalcOnDT1,
                            nodeA,
                            nodeB,
                            InternalSerializers.create(dt1OutType));

            DeltaJoinRuntimeTree.JoinNode nodeDT2 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            // c1 = b1 and c2 <> 'c-1'
                            getFilterCondition(
                                    // C + A & B
                                    getOutputRowType(),
                                    new int[] {0, 6, 1},
                                    row ->
                                            row.getInt(0) == row.getInt(1)
                                                    && !row.getString(2).toString().equals("c-1")),
                            null,
                            nodeC,
                            nodeDT1,
                            InternalSerializers.create(getOutputRowType()));

            return new DeltaJoinRuntimeTree(nodeDT2);
        }

        @Override
        Set<Set<Integer>> getLeft2RightDrivenSideInfo() {
            return Set.of(Set.of(0), Set.of(2));
        }

        @Override
        Set<Set<Integer>> getRight2LeftDrivenSideInfo() {
            return Set.of(Set.of(1, 2));
        }

        @Override
        void insertTableDataOnEmit(int inputIdx, RowData rowData) {
            if (inputIdx == 1) {
                // split A and B
                RowType dt1OutType = combineSourceRowTypes(0, 1);
                RowData dataOnA = projectRowData(rowData, dt1OutType, new int[] {0, 1, 2});
                insertTableData(0, dataOnA);
                RowData dataOnB = projectRowData(rowData, dt1OutType, new int[] {3, 4, 5});
                insertTableData(1, dataOnB);
            } else {
                insertTableData(2, rowData);
            }
        }
    }

    /**
     * Test spec for third-cascaded join with lookup order C -> D -> B -> A.
     *
     * <p>The sql is like:
     *
     * <pre>
     *  insert into snk
     *      select * from C
     *      join (
     *          select * from D
     *          join (
     *              select * from A
     *              join B
     *              on a1 = b1 and b2 <> 'b-2'
     *          )
     *          on d1 = b1
     *      )
     *      on d0 = c0 and d2 <> 'd-1' and c2 <> 'c-2'
     * </pre>
     *
     * <p>The join tree is like:
     *
     * <pre>
     *     DT3
     *   /    \
     *  C     DT2
     *     /    \
     *    D     DT1
     *        /    \
     *       A      B
     * </pre>
     */
    private class ThirdCascadedWithOrderCDBATestSpec extends CascadedTestSpec {

        @Override
        int[] getEachBinaryInputFieldSize() {
            return new int[] {3, 3, 3, 3};
        }

        @Override
        RowType getLeftInputRowType() {
            return tableRowTypeMap.get(2);
        }

        @Override
        RowType getRightInputRowType() {
            return combineSourceRowTypes(3, 0, 1);
        }

        @Override
        int[] getLeftJoinKeyIndices() {
            // left jk: c0
            return new int[] {2};
        }

        @Override
        int[] getRightJoinKeyIndices() {
            // right jk: d0
            return new int[] {1};
        }

        @Override
        Optional<int[]> getLeftUpsertKey() {
            // left uk: c0
            return Optional.of(new int[] {2});
        }

        @Override
        Optional<int[]> getRightUpsertKey() {
            // right uk: none
            return Optional.empty();
        }

        @Override
        DeltaJoinHandlerChain getLeft2RightHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType cInOutType = tableRowTypeMap.get(2);
            RowType dInOutType = tableRowTypeMap.get(3);
            return buildCascadedChain(
                    Arrays.asList(
                            LookupTestSpec.builder()
                                    .withSourceInputs(0)
                                    .withTargetInput(1)
                                    .withTargetTableIdx(3)
                                    .withSourceLookupKeyIdx(2)
                                    .withSourceRowType(cInOutType)
                                    .withTargetLookupKeyIdx(1)
                                    .withTargetRowType(dInOutType)
                                    .build(),
                            LookupTestSpec.builder()
                                    .withSourceInputs(1)
                                    .withTargetInput(3)
                                    .withTargetTableIdx(1)
                                    .withSourceLookupKeyIdx(2)
                                    .withSourceRowType(dInOutType)
                                    .withTargetLookupKeyIdx(0)
                                    .withTargetRowType(bInOutType)
                                    .build(),
                            LookupTestSpec.builder()
                                    .withSourceInputs(3)
                                    .withTargetInput(2)
                                    .withTargetTableIdx(0)
                                    .withSourceLookupKeyIdx(0)
                                    .withSourceRowType(bInOutType)
                                    .withTargetLookupKeyIdx(1)
                                    .withTargetRowType(aInOutType)
                                    // b2 <> 'b-2'
                                    .withGeneratedRemainingCondition(
                                            getFilterCondition(
                                                    // B + A
                                                    combineSourceRowTypes(0, 1),
                                                    new int[] {5},
                                                    row ->
                                                            !row.getString(0)
                                                                    .toString()
                                                                    .equals("b-2")))
                                    .build()),
                    new int[] {1, 2, 3},
                    new int[] {0},
                    fetcherCollector);
        }

        @Override
        DeltaJoinHandlerChain getRight2LeftHandlerChain(
                Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
            RowType dt2OutType = getRightInputRowType();
            RowType cInOutType = getLeftInputRowType();
            return buildBinaryChain(
                    LookupTestSpec.builder()
                            .withSourceInputs(1, 2, 3)
                            .withTargetInput(0)
                            .withTargetTableIdx(2)
                            .withSourceLookupKeyIdx(1)
                            .withSourceRowType(dt2OutType)
                            .withTargetLookupKeyIdx(2)
                            .withTargetRowType(cInOutType)
                            .build(),
                    fetcherCollector);
        }

        @Override
        @Nullable
        GeneratedFilterCondition getRemainingJoinCondition() {
            // d0 = c0 and d2 <> 'd-1' and c2 <> 'c-2'
            return getFilterCondition(
                    // C + D & A & B
                    getOutputRowType(),
                    new int[] {2, 4, 3, 1},
                    row ->
                            row.getDouble(0) == row.getDouble(1)
                                    && !row.getString(2).toString().equals("d-1")
                                    && !row.getString(3).toString().equals("c-2"));
        }

        @Override
        DeltaJoinRuntimeTree getJoinRuntimeTree() {
            RowType aInOutType = tableRowTypeMap.get(0);
            RowType bInOutType = tableRowTypeMap.get(1);
            RowType cInOutType = tableRowTypeMap.get(2);
            RowType dInOutType = tableRowTypeMap.get(3);
            RowType dt1OutType = combineSourceRowTypes(0, 1);
            RowType dt2OutType = getRightInputRowType();
            RowType dt3OutType = getOutputRowType();

            DeltaJoinRuntimeTree.BinaryInputNode nodeA =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            2, null, InternalSerializers.create(aInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeB =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            3, null, InternalSerializers.create(bInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeC =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            0, null, InternalSerializers.create(cInOutType));
            DeltaJoinRuntimeTree.BinaryInputNode nodeD =
                    new DeltaJoinRuntimeTree.BinaryInputNode(
                            1, null, InternalSerializers.create(dInOutType));
            DeltaJoinRuntimeTree.JoinNode nodeDT1 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    dt1OutType,
                                    new int[] {1, 3, 5},
                                    row ->
                                            row.getInt(0) == row.getInt(1)
                                                    && !row.getString(2).toString().equals("b-2")),
                            null,
                            nodeA,
                            nodeB,
                            InternalSerializers.create(dt1OutType));
            DeltaJoinRuntimeTree.JoinNode nodeDT2 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    dt2OutType,
                                    new int[] {2, 6},
                                    row -> row.getInt(0) == row.getInt(1)),
                            null,
                            nodeD,
                            nodeDT1,
                            InternalSerializers.create(dt2OutType));
            DeltaJoinRuntimeTree.JoinNode nodeDT3 =
                    new DeltaJoinRuntimeTree.JoinNode(
                            FlinkJoinType.INNER,
                            getFilterCondition(
                                    dt3OutType,
                                    new int[] {2, 4, 3, 1},
                                    row ->
                                            row.getDouble(0) == row.getDouble(1)
                                                    && !row.getString(2).toString().equals("d-1")
                                                    && !row.getString(3).toString().equals("c-2")),
                            null,
                            nodeC,
                            nodeDT2,
                            InternalSerializers.create(dt3OutType));
            return new DeltaJoinRuntimeTree(nodeDT3);
        }

        @Override
        Set<Set<Integer>> getLeft2RightDrivenSideInfo() {
            return Set.of(Set.of(0), Set.of(1), Set.of(3));
        }

        @Override
        Set<Set<Integer>> getRight2LeftDrivenSideInfo() {
            return Set.of(Set.of(1, 2, 3));
        }
    }

    private void insertTableData(int tableIdx, RowData data) {
        try {
            RowData uk = tableUpsertKeySelector.get(tableIdx).getKey(data);

            tableCurrentDataMap.compute(
                    tableIdx,
                    (k, v) -> {
                        if (v == null) {
                            v = new LinkedHashMap<>();
                        }
                        v.put(uk, data);
                        return v;
                    });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to add data to table", e);
        }
    }

    private List<MyAsyncFunction> unwrapAsyncFunctions(
            StreamingDeltaJoinOperator operator, boolean unwrapLeft, int handlerIdxInChain) {
        List<AsyncDeltaJoinRunner.DeltaJoinProcessor> processors =
                unwrapProcessor(operator, unwrapLeft);
        List<MyAsyncFunction> results = new ArrayList<>();
        for (AsyncDeltaJoinRunner.DeltaJoinProcessor processor : processors) {
            int idx = 0;
            DeltaJoinHandlerBase handler = processor.getDeltaJoinHandlerChain().getHead();
            while (idx < handlerIdxInChain) {
                handler = Objects.requireNonNull(handler.getNext());
                idx++;
            }
            if (handler instanceof LookupHandlerBase) {
                results.add((MyAsyncFunction) ((LookupHandlerBase) handler).getFetcher());
            } else {
                throw new IllegalStateException("The handler is not a lookup handler");
            }
        }
        return results;
    }

    private List<AsyncDeltaJoinRunner.DeltaJoinProcessor> unwrapProcessor(
            StreamingDeltaJoinOperator operator, boolean unwrapLeft) {
        if (unwrapLeft) {
            return operator.getLeftTriggeredUserFunction().getAllProcessors();
        } else {
            return operator.getRightTriggeredUserFunction().getAllProcessors();
        }
    }

    private RowData projectRowData(RowData rowData, RowType rowType, int[] fields) {
        List<LogicalType> inputTypes = rowType.getChildren();
        GenericRowData data = new GenericRowData(rowData.getRowKind(), fields.length);
        for (int i = 0; i < fields.length; i++) {
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(inputTypes.get(fields[i]), fields[i]);
            data.setField(i, fieldGetter.getFieldOrNull(rowData));
        }
        return data;
    }

    private RowType combineSourceRowTypes(int... sourceIdx) {
        return combineRowTypes(
                Arrays.stream(sourceIdx).boxed().map(tableRowTypeMap::get).toArray(RowType[]::new));
    }

    private void verifyCacheData(
            CascadedTestSpec testSpec,
            DeltaJoinCache actualCache,
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData,
            Map<RowData, Map<RowData, RowData>> expectedRightCacheData,
            long expectedLeftCacheRequestCount,
            long expectedLeftCacheHitCount,
            long expectedRightCacheRequestCount,
            long expectedRightCacheHitCount) {
        // assert left cache
        verifyCacheData(
                actualCache,
                expectedLeftCacheData,
                expectedLeftCacheRequestCount,
                expectedLeftCacheHitCount,
                testSpec.getLeftJoinKeySelector().getProducedType().toRowType(),
                testSpec.getLeftUpsertKeySelector().getProducedType().toRowType(),
                testSpec.getLeftInputRowType(),
                true);

        // assert right cache
        verifyCacheData(
                actualCache,
                expectedRightCacheData,
                expectedRightCacheRequestCount,
                expectedRightCacheHitCount,
                testSpec.getRightJoinKeySelector().getProducedType().toRowType(),
                testSpec.getRightUpsertKeySelector().getProducedType().toRowType(),
                testSpec.getRightInputRowType(),
                false);
    }

    private DeltaJoinHandlerChain buildBinaryChain(
            LookupTestSpec lookupTestSpec,
            Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
        fetcherCollector.put(
                lookupTestSpec.targetInput,
                createFetcherFunction(
                        tableCurrentDataMap,
                        getKeySelector(
                                lookupTestSpec.sourceLookupKeyIdx, lookupTestSpec.sourceRowType),
                        getKeySelector(
                                lookupTestSpec.targetLookupKeyIdx, lookupTestSpec.targetRowType),
                        lookupTestSpec.targetTableIdx,
                        lookupTestSpec.expectedThrownException));
        BinaryLookupHandler handler =
                new BinaryLookupHandler(
                        toInternalDataType(lookupTestSpec.sourceRowType),
                        toInternalDataType(lookupTestSpec.targetRowType),
                        toInternalDataType(lookupTestSpec.targetRowType),
                        InternalSerializers.create(lookupTestSpec.targetRowType),
                        lookupTestSpec.targetGeneratedCalc,
                        lookupTestSpec.sourceInputs,
                        lookupTestSpec.targetInput);
        return DeltaJoinHandlerChain.build(
                Collections.singletonList(handler), lookupTestSpec.sourceInputs);
    }

    private DeltaJoinHandlerChain buildCascadedChain(
            List<LookupTestSpec> lookupChain,
            int[] allLookupSideBinaryInputOrdinals,
            int[] streamInputOrdinals,
            Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetcherCollector) {
        Preconditions.checkArgument(lookupChain.size() > 1);
        List<DeltaJoinHandlerBase> handlers = new ArrayList<>();
        for (int i = 0; i < lookupChain.size(); i++) {
            LookupTestSpec lookupTestSpec = lookupChain.get(i);
            fetcherCollector.put(
                    lookupTestSpec.targetInput,
                    createFetcherFunction(
                            tableCurrentDataMap,
                            getKeySelector(
                                    lookupTestSpec.sourceLookupKeyIdx,
                                    lookupTestSpec.sourceRowType),
                            getKeySelector(
                                    lookupTestSpec.targetLookupKeyIdx,
                                    lookupTestSpec.targetRowType),
                            lookupTestSpec.targetTableIdx,
                            lookupTestSpec.expectedThrownException));

            handlers.add(
                    new CascadedLookupHandler(
                            i + 1,
                            toInternalDataType(lookupTestSpec.sourceRowType),
                            toInternalDataType(lookupTestSpec.targetRowType),
                            toInternalDataType(lookupTestSpec.targetRowType),
                            InternalSerializers.create(lookupTestSpec.targetRowType),
                            lookupTestSpec.targetGeneratedCalc,
                            lookupTestSpec.generatedRemainingCondition,
                            getKeySelector(
                                    lookupTestSpec.sourceLookupKeyIdx,
                                    lookupTestSpec.sourceRowType),
                            lookupTestSpec.sourceInputs,
                            lookupTestSpec.targetInput,
                            Arrays.stream(lookupTestSpec.sourceInputs)
                                    .allMatch(src -> src < lookupTestSpec.targetInput)));
        }

        handlers.add(new TailOutputDataHandler(allLookupSideBinaryInputOrdinals));
        return DeltaJoinHandlerChain.build(handlers, streamInputOrdinals);
    }

    private static class LookupTestSpec {
        private final int[] sourceInputs;
        private final int targetInput;
        private final int targetTableIdx;
        private final int[] sourceLookupKeyIdx;
        private final RowType sourceRowType;
        private final int[] targetLookupKeyIdx;
        private final RowType targetRowType;
        private final @Nullable GeneratedFilterCondition generatedRemainingCondition;
        private final @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>>
                targetGeneratedCalc;
        private final @Nullable Throwable expectedThrownException;

        private LookupTestSpec(
                int[] sourceInputs,
                int targetInput,
                int targetTableIdx,
                int[] sourceLookupKeyIdx,
                RowType sourceRowType,
                int[] targetLookupKeyIdx,
                RowType targetRowType,
                @Nullable GeneratedFilterCondition generatedRemainingCondition,
                @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>> targetGeneratedCalc,
                @Nullable Throwable expectedThrownException) {
            this.sourceInputs = sourceInputs;
            this.targetInput = targetInput;
            this.targetTableIdx = targetTableIdx;
            this.sourceLookupKeyIdx = sourceLookupKeyIdx;
            this.sourceRowType = sourceRowType;
            this.targetLookupKeyIdx = targetLookupKeyIdx;
            this.targetRowType = targetRowType;
            this.generatedRemainingCondition = generatedRemainingCondition;
            this.targetGeneratedCalc = targetGeneratedCalc;
            this.expectedThrownException = expectedThrownException;
        }

        public static Builder builder() {
            return new Builder();
        }

        private static class Builder {
            private int[] sourceInputs;
            private int targetInput;
            private int targetTableIdx;
            private int[] sourceLookupKeyIdx;
            private RowType sourceRowType;
            private int[] targetLookupKeyIdx;
            private RowType targetRowType;
            private @Nullable GeneratedFilterCondition generatedRemainingCondition;
            private @Nullable GeneratedFunction<FlatMapFunction<RowData, RowData>>
                    targetGeneratedCalc;
            private @Nullable Throwable expectedThrownException;

            public Builder withSourceInputs(int... sources) {
                this.sourceInputs = sources;
                return this;
            }

            public Builder withTargetInput(int target) {
                this.targetInput = target;
                return this;
            }

            public Builder withTargetTableIdx(int targetTableIdx) {
                this.targetTableIdx = targetTableIdx;
                return this;
            }

            public Builder withSourceLookupKeyIdx(int... sourceLookupKeyIdx) {
                this.sourceLookupKeyIdx = sourceLookupKeyIdx;
                return this;
            }

            public Builder withSourceRowType(RowType sourceRowType) {
                this.sourceRowType = sourceRowType;
                return this;
            }

            public Builder withTargetLookupKeyIdx(int... targetLookupKeyIdx) {
                this.targetLookupKeyIdx = targetLookupKeyIdx;
                return this;
            }

            public Builder withTargetRowType(RowType targetRowType) {
                this.targetRowType = targetRowType;
                return this;
            }

            public Builder withGeneratedRemainingCondition(
                    GeneratedFilterCondition generatedRemainingCondition) {
                this.generatedRemainingCondition = generatedRemainingCondition;
                return this;
            }

            public Builder withTargetGeneratedCalc(
                    @Nullable
                            GeneratedFunction<FlatMapFunction<RowData, RowData>>
                                    lookupSideGeneratedCalc) {
                this.targetGeneratedCalc = lookupSideGeneratedCalc;
                return this;
            }

            public Builder expectedThrownException(@Nullable Throwable expectedThrownException) {
                this.expectedThrownException = expectedThrownException;
                return this;
            }

            public LookupTestSpec build() {
                return new LookupTestSpec(
                        requireNonNull(sourceInputs),
                        targetInput,
                        targetTableIdx,
                        requireNonNull(sourceLookupKeyIdx),
                        requireNonNull(sourceRowType),
                        requireNonNull(targetLookupKeyIdx),
                        requireNonNull(targetRowType),
                        generatedRemainingCondition,
                        targetGeneratedCalc,
                        expectedThrownException);
            }
        }
    }
}
