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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.AecRecord;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.RecordsBuffer;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.binaryrow;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link StreamingDeltaJoinOperator} with two binary inputs. */
@ExtendWith(ParameterizedTestExtension.class)
public class StreamingBinaryDeltaJoinOperatorTest extends StreamingDeltaJoinOperatorTestBase {

    // the data snapshot of the left/right table when joining
    // <table index, <upsert key, data>>
    // left table index is 0, right table index is 1
    private final Map<Integer, LinkedHashMap<RowData, RowData>> tableCurrentDataMap =
            new HashMap<>();

    @Parameters(name = "EnableCache = {0}")
    public static List<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    @Parameter public boolean enableCache;

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness;

    private RowDataHarnessAssertor assertor;

    private Optional<Throwable> latestException = Optional.empty();

    @BeforeEach
    public void beforeEach() throws Exception {
        MyAsyncFunction.getLookupInvokeCount().clear();
    }

    @AfterEach
    public void afterEach() throws Exception {
        if (assertor != null) {
            testHarness.close();
        }
        tableCurrentDataMap.clear();
        latestException = Optional.empty();
    }

    @TestTemplate
    void testJoinBothLogTables() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        StreamRecord<RowData> leftRecord2 = insertRecord(100, false, "jklk2");
        testHarness.processElement1(leftRecord1);
        testHarness.processElement1(leftRecord2);

        StreamRecord<RowData> leftRecord3 = insertRecord(200, true, "jklk1");
        StreamRecord<RowData> leftRecord4 = insertRecord(200, false, "jklk2");
        testHarness.processElement1(leftRecord3);
        testHarness.processElement1(leftRecord4);

        StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        StreamRecord<RowData> rightRecord2 = insertRecord("jklk2", 300, false);
        testHarness.processElement2(rightRecord1);
        testHarness.processElement2(rightRecord2);

        // mismatch
        StreamRecord<RowData> rightRecord3 = insertRecord("unknown", 500, false);
        testHarness.processElement2(rightRecord3);

        StreamRecord<RowData> leftRecord5 = insertRecord(800, true, "jklk1");
        StreamRecord<RowData> leftRecord6 = insertRecord(800, false, "jklk2");
        testHarness.processElement1(leftRecord5);
        testHarness.processElement1(leftRecord6);

        StreamRecord<RowData> rightRecord4 = insertRecord("jklk1", 1000, true);
        StreamRecord<RowData> rightRecord5 = insertRecord("jklk2", 1000, false);
        testHarness.processElement2(rightRecord4);
        testHarness.processElement2(rightRecord5);

        waitAllDataProcessed();

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(100, false, "jklk2", "jklk2", 300, false));
        expectedOutput.add(insertRecord(200, false, "jklk2", "jklk2", 300, false));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(800, false, "jklk2", "jklk2", 300, false));
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(100, false, "jklk2", "jklk2", 1000, false));
        expectedOutput.add(insertRecord(200, false, "jklk2", "jklk2", 1000, false));
        expectedOutput.add(insertRecord(800, false, "jklk2", "jklk2", 1000, false));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowType leftRowType = testSpec.getLeftInputRowType();
            RowType rightRowType = testSpec.getRightInputRowType();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    leftRecord1.getValue(),
                                    toBinary(leftRecord3.getValue(), leftRowType),
                                    leftRecord3.getValue(),
                                    toBinary(leftRecord5.getValue(), leftRowType),
                                    leftRecord5.getValue()),
                            binaryrow(false, "jklk2"),
                            Map.of(
                                    toBinary(leftRecord2.getValue(), leftRowType),
                                    leftRecord2.getValue(),
                                    toBinary(leftRecord4.getValue(), leftRowType),
                                    leftRecord4.getValue(),
                                    toBinary(leftRecord6.getValue(), leftRowType),
                                    leftRecord6.getValue()),
                            binaryrow(false, "unknown"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(rightRecord1.getValue(), rightRowType),
                                    rightRecord1.getValue(),
                                    toBinary(rightRecord4.getValue(), rightRowType),
                                    rightRecord4.getValue()),
                            binaryrow(false, "jklk2"),
                            Map.of(
                                    toBinary(rightRecord2.getValue(), rightRowType),
                                    rightRecord2.getValue(),
                                    toBinary(rightRecord5.getValue(), rightRowType),
                                    rightRecord5.getValue()));

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 5, 2, 6, 4);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(6);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(5);
        }
    }

    @TestTemplate
    void testJoinBothLogTablesWhileFilterExistsOnBothTable() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITH_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        testHarness.processElement1(leftRecord1);

        // will be filtered upstream
        StreamRecord<RowData> leftRecord2 = insertRecord(100, false, "jklk2");
        insertLeftTable(testSpec, leftRecord2);

        StreamRecord<RowData> leftRecord3 = insertRecord(200, true, "jklk1");
        testHarness.processElement1(leftRecord3);

        // will be filtered upstream
        StreamRecord<RowData> leftRecord4 = insertRecord(200, false, "jklk2");
        insertLeftTable(testSpec, leftRecord4);

        StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        testHarness.processElement2(rightRecord1);

        // will be filtered upstream
        StreamRecord<RowData> rightRecord2 = insertRecord("jklk2", 300, false);
        insertRightTable(testSpec, rightRecord2);

        // mismatch
        StreamRecord<RowData> leftRecord5 = insertRecord(200, true, "unknown1");
        testHarness.processElement1(leftRecord5);

        // mismatch and will be filtered upstream
        StreamRecord<RowData> rightRecord3 = insertRecord("unknown2", 300, false);
        insertRightTable(testSpec, rightRecord3);

        StreamRecord<RowData> leftRecord6 = insertRecord(800, true, "jklk1");
        testHarness.processElement1(leftRecord6);

        // will be filtered upstream
        StreamRecord<RowData> leftRecord7 = insertRecord(800, false, "jklk2");
        insertLeftTable(testSpec, leftRecord7);

        StreamRecord<RowData> rightRecord4 = insertRecord("jklk1", 1000, true);
        testHarness.processElement2(rightRecord4);

        // will be filtered upstream
        StreamRecord<RowData> rightRecord5 = insertRecord("jklk2", 1000, false);
        insertRightTable(testSpec, rightRecord5);

        waitAllDataProcessed();

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 1000, true));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowType leftRowType = testSpec.getLeftInputRowType();
            RowType rightRowType = testSpec.getRightInputRowType();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    leftRecord1.getValue(),
                                    toBinary(leftRecord3.getValue(), leftRowType),
                                    leftRecord3.getValue(),
                                    toBinary(leftRecord6.getValue(), leftRowType),
                                    leftRecord6.getValue()));

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(rightRecord1.getValue(), rightRowType),
                                    rightRecord1.getValue(),
                                    toBinary(rightRecord4.getValue(), rightRowType),
                                    rightRecord4.getValue()),
                            binaryrow(true, "unknown1"),
                            Collections.emptyMap());

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 2, 1, 4, 2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(1);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(4);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        }
    }

    @TestTemplate
    void testJoinBothPkTables() throws Exception {
        PkPkTableJoinTestSpec testSpec = PkPkTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(100, true, "Tom");
        StreamRecord<RowData> leftRecordK2V1 = insertRecord(101, false, "Tom");
        // mismatch
        StreamRecord<RowData> leftRecordK3V1 = insertRecord(1999, false, "Jim");
        testHarness.processElement1(leftRecordK1V1);
        testHarness.processElement1(leftRecordK2V1);
        testHarness.processElement1(leftRecordK3V1);

        waitAllDataProcessed();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V1 = insertRecord("Tom", 200, true);
        StreamRecord<RowData> rightRecordK2V1 = insertRecord("Tom", 201, false);
        // mismatch
        StreamRecord<RowData> rightRecordK3V1 = insertRecord("Sam", 2999, false);
        testHarness.processElement2(rightRecordK1V1);
        testHarness.processElement2(rightRecordK2V1);
        testHarness.processElement2(rightRecordK3V1);

        waitAllDataProcessed();
        expectedOutput.add(insertRecord(100, true, "Tom", "Tom", 200, true));
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 200, true));
        expectedOutput.add(insertRecord(100, true, "Tom", "Tom", 201, false));
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 201, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V2 = updateAfterRecord(1000, true, "Tom");
        testHarness.processElement1(leftRecordK1V2);

        waitAllDataProcessed();
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 200, true));
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 201, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> rightRecordK1V2 = updateAfterRecord("Tom", 2000, true);
        StreamRecord<RowData> rightRecordK2V2 = updateAfterRecord("Tom", 2001, false);
        testHarness.processElement2(rightRecordK1V2);
        testHarness.processElement2(rightRecordK2V2);

        waitAllDataProcessed();
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 2000, true));
        expectedOutput.add(updateAfterRecord(101, false, "Tom", "Tom", 2000, true));
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 2001, false));
        expectedOutput.add(updateAfterRecord(101, false, "Tom", "Tom", 2001, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(
                                    binaryrow(true, "Tom"),
                                    leftRecordK1V2.getValue(),
                                    binaryrow(false, "Tom"),
                                    leftRecordK2V1.getValue()),
                            binaryrow("Sam"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(
                                    binaryrow("Tom", true),
                                    rightRecordK1V2.getValue(),
                                    binaryrow("Tom", false),
                                    rightRecordK2V2.getValue()),
                            binaryrow("Jim"),
                            Collections.emptyMap());
            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 5, 3, 4, 2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(4);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(5);
        }
    }

    @TestTemplate
    void testJoinBothPkTablesWhileFilterExistsOnBothTable() throws Exception {
        PkPkTableJoinTestSpec testSpec = PkPkTableJoinTestSpec.WITH_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        StreamRecord<RowData> leftRecordK1V1 = insertRecord(100, true, "Tom");
        testHarness.processElement1(leftRecordK1V1);

        // will be filtered upstream
        StreamRecord<RowData> leftRecordK2V1 = insertRecord(101, false, "Tom");
        insertLeftTable(testSpec, leftRecordK2V1);

        // mismatch and will be filtered upstream
        StreamRecord<RowData> leftRecordK3V1 = insertRecord(1999, false, "Jim");
        insertLeftTable(testSpec, leftRecordK3V1);

        waitAllDataProcessed();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        // will be filtered upstream
        StreamRecord<RowData> rightRecordK1V1 = insertRecord("Tom", 200, true);
        insertRightTable(testSpec, rightRecordK1V1);

        StreamRecord<RowData> rightRecordK2V1 = insertRecord("Tom", 201, false);
        testHarness.processElement2(rightRecordK2V1);

        // mismatch
        StreamRecord<RowData> rightRecordK3V1 = insertRecord("Sam", 2999, true);
        testHarness.processElement2(rightRecordK3V1);

        waitAllDataProcessed();
        expectedOutput.add(insertRecord(100, true, "Tom", "Tom", 201, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        StreamRecord<RowData> leftRecordK1V2 = updateAfterRecord(1000, true, "Tom");
        testHarness.processElement1(leftRecordK1V2);

        waitAllDataProcessed();
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 201, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        // will be filtered upstream
        StreamRecord<RowData> rightRecordK1V2 = updateAfterRecord("Tom", 2000, true);
        insertRightTable(testSpec, rightRecordK1V2);

        StreamRecord<RowData> rightRecordK2V2 = updateAfterRecord("Tom", 2001, false);
        testHarness.processElement2(rightRecordK2V2);

        waitAllDataProcessed();
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 2001, false));
        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(binaryrow(true, "Tom"), leftRecordK1V2.getValue()),
                            binaryrow("Sam"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(binaryrow("Tom", false), rightRecordK2V2.getValue()));
            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 3, 1, 2, 1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
        }
    }

    @TestTemplate
    void testBlockingWithSameJoinKey() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        // block the async function
        MyAsyncFunction.block();

        // in flight
        StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        StreamRecord<RowData> leftRecord2 = insertRecord(100, false, "jklk2");
        testHarness.processElement1(leftRecord1);
        testHarness.processElement1(leftRecord2);

        // blocked
        StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        StreamRecord<RowData> rightRecord2 = insertRecord("jklk2", 300, false);
        testHarness.processElement2(rightRecord1);
        testHarness.processElement2(rightRecord2);

        // blocked
        StreamRecord<RowData> leftRecord3 = insertRecord(200, true, "jklk1");
        StreamRecord<RowData> leftRecord4 = insertRecord(200, false, "jklk2");
        StreamRecord<RowData> leftRecord5 = insertRecord(201, false, "jklk2");
        testHarness.processElement1(leftRecord3);
        testHarness.processElement1(leftRecord4);
        testHarness.processElement1(leftRecord5);

        // in flight
        StreamRecord<RowData> rightRecord3 = insertRecord("unknown", 500, false);
        testHarness.processElement2(rightRecord3);

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(5);
        assertThat(aec.getInFlightSize()).isEqualTo(3);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        RecordsBuffer<AecRecord<RowData, RowData>, RowData> recordsBuffer = aec.getRecordsBuffer();
        assertThat(recordsBuffer.getActiveBuffer().size()).isEqualTo(3);
        assertThat(recordsBuffer.getBlockingBuffer().size()).isEqualTo(2);

        RowDataKeySelector leftJoinKeySelector = testSpec.getLeftJoinKeySelector();
        RowDataKeySelector rightJoinKeySelector = testSpec.getRightJoinKeySelector();

        RowData joinKey1 = leftJoinKeySelector.getKey(insertRecord(100, true, "jklk1").getValue());
        RowData joinKey2 = leftJoinKeySelector.getKey(insertRecord(100, false, "jklk2").getValue());
        RowData joinKey3 =
                rightJoinKeySelector.getKey(insertRecord("unknown", 500, false).getValue());

        assertThat(recordsBuffer.getActiveBuffer().get(joinKey1)).isNotNull();
        assertThat(recordsBuffer.getActiveBuffer().get(joinKey2)).isNotNull();
        assertThat(recordsBuffer.getActiveBuffer().get(joinKey3)).isNotNull();
        assertThat(recordsBuffer.getBlockingBuffer().get(joinKey1)).isNotNull().hasSize(2);
        assertThat(recordsBuffer.getBlockingBuffer().get(joinKey2)).isNotNull().hasSize(3);
        assertThat(recordsBuffer.getBlockingBuffer().get(joinKey3)).isNull();

        MyAsyncFunction.release();

        waitAllDataProcessed();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(100, false, "jklk2", "jklk2", 300, false));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, false, "jklk2", "jklk2", 300, false));
        expectedOutput.add(insertRecord(201, false, "jklk2", "jklk2", 300, false));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);
        assertThat(recordsBuffer.getActiveBuffer()).isEmpty();
        assertThat(recordsBuffer.getBlockingBuffer()).isEmpty();
        assertThat(recordsBuffer.getFinishedBuffer()).isEmpty();

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowType leftRowType = testSpec.getLeftInputRowType();
            RowType rightRowType = testSpec.getRightInputRowType();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    leftRecord1.getValue(),
                                    toBinary(leftRecord3.getValue(), leftRowType),
                                    leftRecord3.getValue()),
                            binaryrow(false, "jklk2"),
                            Map.of(
                                    toBinary(leftRecord2.getValue(), leftRowType),
                                    leftRecord2.getValue(),
                                    toBinary(leftRecord4.getValue(), leftRowType),
                                    leftRecord4.getValue(),
                                    toBinary(leftRecord5.getValue(), leftRowType),
                                    leftRecord5.getValue()),
                            binaryrow(false, "unknown"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(rightRecord1.getValue(), rightRowType),
                                    rightRecord1.getValue()),
                            binaryrow(false, "jklk2"),
                            Map.of(
                                    toBinary(rightRecord2.getValue(), rightRowType),
                                    rightRecord2.getValue()));

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 3, 0, 5, 3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(5);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
        }
    }

    /**
     * This test is used to test the scenario where the right stream side joined out a record from
     * the left table that has not been sent to the delta-join operator (maybe is in flight between
     * source and delta-join).
     */
    @TestTemplate
    void testLogTableDataVisibleBeforeJoin() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec, null, false);
        initAssertor(testSpec);

        // prepare the data first to mock all following requests were in flight between source and
        // delta-join
        final StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        insertLeftTable(testSpec, leftRecord1);

        final StreamRecord<RowData> leftRecord2 = insertRecord(200, true, "jklk1");
        insertLeftTable(testSpec, leftRecord2);

        final StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        insertRightTable(testSpec, rightRecord1);

        // mismatch
        final StreamRecord<RowData> rightRecord2 = insertRecord("jklk2", 500, false);
        insertRightTable(testSpec, rightRecord2);

        final StreamRecord<RowData> leftRecord3 = insertRecord(800, true, "jklk1");
        insertLeftTable(testSpec, leftRecord3);

        final StreamRecord<RowData> rightRecord3 = insertRecord("jklk1", 1000, true);
        insertRightTable(testSpec, rightRecord3);

        testHarness.processElement1(leftRecord1);
        testHarness.processElement1(leftRecord2);
        testHarness.processElement2(rightRecord1);
        testHarness.processElement2(rightRecord2);
        testHarness.processElement1(leftRecord3);
        testHarness.processElement2(rightRecord3);

        waitAllDataProcessed();

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // left record comes
        // left can see 2 records in right log table
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 1000, true));
        // left record comes
        // left can see 2 records in right log table
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 1000, true));
        // right record comes
        // right can see 3 records in left log table
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 300, true));
        // left record comes
        // left can see 2 records in right log table
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 1000, true));
        // right record comes
        // right can see 3 records in left log table
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 1000, true));
        expectedOutput.add(insertRecord(800, true, "jklk1", "jklk1", 1000, true));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowType leftRowType = testSpec.getLeftInputRowType();
            RowType rightRowType = testSpec.getRightInputRowType();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    leftRecord1.getValue(),
                                    toBinary(leftRecord2.getValue(), leftRowType),
                                    leftRecord2.getValue(),
                                    toBinary(leftRecord3.getValue(), leftRowType),
                                    leftRecord3.getValue()),
                            binaryrow(false, "jklk2"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(rightRecord1.getValue(), rightRowType),
                                    rightRecord1.getValue(),
                                    toBinary(rightRecord3.getValue(), rightRowType),
                                    rightRecord3.getValue()));

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 3, 1, 3, 2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(3);
        }
    }

    /**
     * This test is used to test the scenario where the right stream side joined out a record from
     * the left table that has not been sent to the delta-join operator (maybe is in flight between
     * source and delta-join).
     */
    @TestTemplate
    void testPkTableDataVisibleBeforeJoin() throws Exception {
        PkPkTableJoinTestSpec testSpec = PkPkTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec, null, false);
        initAssertor(testSpec);

        // prepare the data first to mock all following requests were in flight between source and
        // delta-join
        final StreamRecord<RowData> leftRecordK1V1 = insertRecord(100, true, "Tom");
        insertLeftTable(testSpec, leftRecordK1V1);
        final StreamRecord<RowData> leftRecordK1V2 = updateAfterRecord(1000, true, "Tom");
        insertLeftTable(testSpec, leftRecordK1V2);

        final StreamRecord<RowData> leftRecordK2V1 = insertRecord(101, false, "Tom");
        insertLeftTable(testSpec, leftRecordK2V1);

        // mismatch
        final StreamRecord<RowData> leftRecordK3V1 = insertRecord(101, false, "Jim");
        insertLeftTable(testSpec, leftRecordK3V1);
        final StreamRecord<RowData> leftRecordK3V2 = updateAfterRecord(1001, false, "Jim");
        insertLeftTable(testSpec, leftRecordK3V2);

        final StreamRecord<RowData> rightRecordK1V1 = insertRecord("Tom", 200, true);
        insertRightTable(testSpec, rightRecordK1V1);
        final StreamRecord<RowData> rightRecordK1V2 = updateAfterRecord("Tom", 2000, true);
        insertRightTable(testSpec, rightRecordK1V2);
        final StreamRecord<RowData> rightRecordK1V3 = updateAfterRecord("Tom", 20000, true);
        insertRightTable(testSpec, rightRecordK1V3);

        final StreamRecord<RowData> rightRecordK2V1 = insertRecord("Tom", 201, false);
        insertRightTable(testSpec, rightRecordK2V1);

        // mismatch
        final StreamRecord<RowData> rightRecordK3V1 = insertRecord("Sam", 999, false);
        insertRightTable(testSpec, rightRecordK3V1);

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.processElement1(leftRecordK1V1);
        expectedOutput.add(insertRecord(100, true, "Tom", "Tom", 20000, true));
        expectedOutput.add(insertRecord(100, true, "Tom", "Tom", 201, false));

        testHarness.processElement1(leftRecordK1V2);
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 20000, true));
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 201, false));

        testHarness.processElement1(leftRecordK2V1);
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 20000, true));
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 201, false));

        testHarness.processElement1(leftRecordK3V1);
        testHarness.processElement1(leftRecordK3V2);

        testHarness.processElement2(rightRecordK1V1);
        expectedOutput.add(insertRecord(1000, true, "Tom", "Tom", 200, true));
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 200, true));

        testHarness.processElement2(rightRecordK1V2);
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 2000, true));
        expectedOutput.add(updateAfterRecord(101, false, "Tom", "Tom", 2000, true));

        testHarness.processElement2(rightRecordK1V3);
        expectedOutput.add(updateAfterRecord(1000, true, "Tom", "Tom", 20000, true));
        expectedOutput.add(updateAfterRecord(101, false, "Tom", "Tom", 20000, true));

        testHarness.processElement2(rightRecordK2V1);
        expectedOutput.add(insertRecord(1000, true, "Tom", "Tom", 201, false));
        expectedOutput.add(insertRecord(101, false, "Tom", "Tom", 201, false));

        testHarness.processElement2(rightRecordK3V1);

        waitAllDataProcessed();

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(
                                    binaryrow(true, "Tom"),
                                    leftRecordK1V2.getValue(),
                                    binaryrow(false, "Tom"),
                                    leftRecordK2V1.getValue()),
                            binaryrow("Sam"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow("Tom"),
                            Map.of(
                                    binaryrow("Tom", true),
                                    rightRecordK1V3.getValue(),
                                    binaryrow("Tom", false),
                                    rightRecordK2V1.getValue()),
                            binaryrow("Jim"),
                            Collections.emptyMap());

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 5, 3, 5, 3);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(5);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(5);
        }
    }

    @TestTemplate
    void testCheckpointAndRestore() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        // block the async function
        MyAsyncFunction.block();

        // in flight
        StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        testHarness.processElement1(leftRecord1);

        // blocked
        StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        testHarness.processElement2(rightRecord1);

        // blocked
        StreamRecord<RowData> leftRecord2 = insertRecord(200, true, "jklk1");
        testHarness.processElement1(leftRecord2);

        // in flight
        StreamRecord<RowData> rightRecord2 = insertRecord("unknown", 500, false);
        testHarness.processElement2(rightRecord2);

        TableAsyncExecutionController<RowData, RowData, RowData> aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(2);
        assertThat(aec.getInFlightSize()).isEqualTo(2);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        RecordsBuffer<AecRecord<RowData, RowData>, RowData> recordsBuffer = aec.getRecordsBuffer();
        assertThat(recordsBuffer.getActiveBuffer().size()).isEqualTo(2);
        assertThat(recordsBuffer.getBlockingBuffer().size()).isEqualTo(1);

        // checkpointing
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        // release async function to avoid timeout when closing
        MyAsyncFunction.release();
        testHarness.close();

        MyAsyncFunction.getLookupInvokeCount().clear();

        MyAsyncFunction.block();
        // restoring
        testHarness = createBinaryDeltaJoinOperatorTestHarness(testSpec, null);

        testHarness.setup();

        StreamingDeltaJoinOperator operator = unwrapOperator(testHarness);
        operator.setAsyncExecutionController(
                new MyAsyncExecutionControllerDelegate(
                        operator.getAsyncExecutionController(),
                        true,
                        (inputIdx, rowData) -> {
                            // split A and B
                            insertTableData(testSpec, rowData, inputIdx == 0);
                        }));

        latestException = Optional.empty();
        testHarness.initializeState(snapshot);

        testHarness.open();

        aec = unwrapAEC(testHarness);
        assertThat(aec.getBlockingSize()).isEqualTo(2);
        assertThat(aec.getInFlightSize()).isEqualTo(2);
        assertThat(aec.getFinishSize()).isEqualTo(0);

        recordsBuffer = aec.getRecordsBuffer();
        assertThat(recordsBuffer.getActiveBuffer().size()).isEqualTo(2);
        assertThat(recordsBuffer.getBlockingBuffer().size()).isEqualTo(1);

        MyAsyncFunction.release();

        waitAllDataProcessed();
        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());

        assertThat(aec.getBlockingSize()).isEqualTo(0);
        assertThat(aec.getInFlightSize()).isEqualTo(0);
        assertThat(aec.getFinishSize()).isEqualTo(0);
        assertThat(recordsBuffer.getActiveBuffer()).isEmpty();
        assertThat(recordsBuffer.getBlockingBuffer()).isEmpty();
        assertThat(recordsBuffer.getFinishedBuffer()).isEmpty();

        DeltaJoinCache cache = unwrapCache(testHarness);
        if (enableCache) {
            RowType leftRowType = testSpec.getLeftInputRowType();
            RowType rightRowType = testSpec.getRightInputRowType();
            Map<RowData, Map<RowData, RowData>> expectedLeftCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    toBinary(leftRecord1.getValue(), leftRowType),
                                    toBinary(leftRecord2.getValue(), leftRowType),
                                    toBinary(leftRecord2.getValue(), leftRowType)),
                            binaryrow(false, "unknown"),
                            Collections.emptyMap());

            Map<RowData, Map<RowData, RowData>> expectedRightCacheData =
                    Map.of(
                            binaryrow(true, "jklk1"),
                            Map.of(
                                    toBinary(rightRecord1.getValue(), rightRowType),
                                    toBinary(rightRecord1.getValue(), rightRowType)));

            verifyCacheData(
                    testSpec, cache, expectedLeftCacheData, expectedRightCacheData, 2, 0, 2, 1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(1);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        } else {
            verifyCacheData(
                    testSpec, cache, Collections.emptyMap(), Collections.emptyMap(), 0, 0, 0, 0);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(1).get()).isEqualTo(2);
            assertThat(MyAsyncFunction.getLookupInvokeCount().get(0).get()).isEqualTo(2);
        }
    }

    @TestTemplate
    void testClearLegacyStateWhenCheckpointing() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        initTestHarness(testSpec);
        initAssertor(testSpec);

        // block the async function
        MyAsyncFunction.block();

        // in flight
        StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        testHarness.processElement1(leftRecord1);

        // blocked
        StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        testHarness.processElement2(rightRecord1);

        // blocked
        StreamRecord<RowData> leftRecord2 = insertRecord(200, true, "jklk1");
        testHarness.processElement1(leftRecord2);

        // in flight
        StreamRecord<RowData> rightRecord2 = insertRecord("unknown", 500, false);
        testHarness.processElement2(rightRecord2);

        // checkpointing
        testHarness.snapshot(0L, 0L);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(2);

        MyAsyncFunction.release();
        waitAllDataProcessed();

        MyAsyncFunction.block();

        StreamRecord<RowData> leftRecord3 = insertRecord(700, true, "jklk1");
        testHarness.processElement1(leftRecord3);

        testHarness.snapshot(1L, 0L);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(1);

        MyAsyncFunction.release();
        waitAllDataProcessed();

        testHarness.snapshot(2L, 0L);
        assertThat(testHarness.numKeyedStateEntries()).isEqualTo(0);

        final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(insertRecord(100, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(200, true, "jklk1", "jklk1", 300, true));
        expectedOutput.add(insertRecord(700, true, "jklk1", "jklk1", 300, true));

        assertor.assertOutputEqualsSorted(
                "result mismatch", expectedOutput, testHarness.getOutput());
    }

    @TestTemplate
    void testMeetExceptionWhenLookup() throws Exception {
        LogLogTableJoinTestSpec testSpec = LogLogTableJoinTestSpec.WITHOUT_FILTER_ON_TABLE;
        Throwable expectedException = new IllegalStateException("Mock to fail");
        initTestHarness(testSpec, expectedException, true);
        initAssertor(testSpec);

        StreamRecord<RowData> record = insertRecord(100, true, "jklk1");
        testHarness.processElement1(record);

        // IllegalStateException(Failed to wait all data processed)
        //  +- Exception(Could not complete the stream element ...)
        //    +- RuntimeException(Failed to lookup table)
        //      +- Actual Exception
        assertThatThrownBy(this::waitAllDataProcessed)
                .cause()
                .cause()
                .cause()
                .isEqualTo(expectedException);
    }

    private void initTestHarness(AbstractBinaryTestSpec testSpec) throws Exception {
        initTestHarness(testSpec, null, true);
    }

    private void initTestHarness(
            AbstractBinaryTestSpec testSpec,
            @Nullable Throwable expectedThrownException,
            boolean insertTableDataAfterEmit)
            throws Exception {
        testHarness = createBinaryDeltaJoinOperatorTestHarness(testSpec, expectedThrownException);
        testHarness.setup();
        testHarness.open();
        StreamingDeltaJoinOperator operator = unwrapOperator(testHarness);
        // set external failure cause consumer to prevent hang
        testHarness
                .getEnvironment()
                .setExternalFailureCauseConsumer(
                        error -> {
                            latestException = Optional.of(error);
                            // DO NOT throw exception up again to avoid hang
                        });
        operator.setAsyncExecutionController(
                new MyAsyncExecutionControllerDelegate(
                        operator.getAsyncExecutionController(),
                        insertTableDataAfterEmit,
                        (inputIdx, rowData) -> {
                            // split A and B
                            insertTableData(testSpec, rowData, inputIdx == 0);
                        }));
    }

    private void initAssertor(AbstractBinaryTestSpec testSpec) {
        assertor = createAssertor(testSpec.getOutputRowType());
    }

    private void verifyCacheData(
            AbstractBinaryTestSpec testSpec,
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

    private void waitAllDataProcessed() throws Exception {
        testHarness.endAllInputs();
        if (latestException.isPresent()) {
            throw new IllegalStateException(
                    "Failed to wait all data processed", latestException.get());
        }
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createBinaryDeltaJoinOperatorTestHarness(
                    AbstractBinaryTestSpec testSpec, @Nullable Throwable expectedThrownException)
                    throws Exception {
        int[] eachBinaryInputFieldSize =
                new int[] {
                    testSpec.getLeftInputTypeInfo().getTotalFields(),
                    testSpec.getRightInputTypeInfo().getTotalFields()
                };

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnLeft =
                testSpec.getFilterOnLeftTable().map(MockGeneratedFlatMapFunction::new).orElse(null);

        GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalcOnRight =
                testSpec.getFilterOnRightTable()
                        .map(MockGeneratedFlatMapFunction::new)
                        .orElse(null);

        BinaryLookupHandler left2RightBinaryLookupHandler =
                new BinaryLookupHandler(
                        testSpec.getLeftInputTypeInfo().getDataType(),
                        testSpec.getRightInputTypeInfo().getDataType(),
                        testSpec.getRightInputTypeInfo().getDataType(),
                        InternalSerializers.create(
                                (RowType) testSpec.getRightInputTypeInfo().toLogicalType()),
                        generatedCalcOnRight,
                        new int[] {0},
                        1);

        DeltaJoinHandlerChain left2RightHandlerChain =
                DeltaJoinHandlerChain.build(
                        Collections.singletonList(left2RightBinaryLookupHandler), new int[] {0});

        BinaryLookupHandler right2LeftBinaryLookupHandler =
                new BinaryLookupHandler(
                        testSpec.getRightInputTypeInfo().getDataType(),
                        testSpec.getLeftInputTypeInfo().getDataType(),
                        testSpec.getLeftInputTypeInfo().getDataType(),
                        InternalSerializers.create(
                                (RowType) testSpec.getLeftInputTypeInfo().toLogicalType()),
                        generatedCalcOnLeft,
                        new int[] {1},
                        0);

        DeltaJoinHandlerChain right2LeftHandlerChain =
                DeltaJoinHandlerChain.build(
                        Collections.singletonList(right2LeftBinaryLookupHandler), new int[] {1});

        DeltaJoinRuntimeTree.BinaryInputNode left =
                new DeltaJoinRuntimeTree.BinaryInputNode(
                        0, generatedCalcOnLeft, testSpec.getLeftInputTypeInfo().toRowSerializer());
        DeltaJoinRuntimeTree.BinaryInputNode right =
                new DeltaJoinRuntimeTree.BinaryInputNode(
                        1,
                        generatedCalcOnRight,
                        testSpec.getRightInputTypeInfo().toRowSerializer());
        DeltaJoinRuntimeTree.JoinNode joinNode =
                new DeltaJoinRuntimeTree.JoinNode(
                        FlinkJoinType.INNER,
                        testSpec.getGeneratedJoinCondition(),
                        null, // calc on the join
                        left,
                        right,
                        InternalSerializers.create(
                                combineRowTypes(
                                        testSpec.getLeftInputTypeInfo().toRowType(),
                                        testSpec.getRightInputTypeInfo().toRowType())));
        DeltaJoinRuntimeTree joinRuntimeTree = new DeltaJoinRuntimeTree(joinNode);

        Set<Set<Integer>> left2RightDrivenSideInfo = new HashSet<>();
        left2RightDrivenSideInfo.add(Collections.singleton(0));

        Set<Set<Integer>> right2LeftDrivenSideInfo = new HashSet<>();
        right2LeftDrivenSideInfo.add(Collections.singleton(1));

        Map<Integer, GeneratedFunction<AsyncFunction<RowData, Object>>> fetchers =
                Map.of(
                        0,
                        createFetcherFunction(
                                tableCurrentDataMap,
                                testSpec.getRightJoinKeySelector(),
                                testSpec.getLeftJoinKeySelector(),
                                0,
                                expectedThrownException),
                        1,
                        createFetcherFunction(
                                tableCurrentDataMap,
                                testSpec.getLeftJoinKeySelector(),
                                testSpec.getRightJoinKeySelector(),
                                1,
                                expectedThrownException));

        return createDeltaJoinOperatorTestHarness(
                eachBinaryInputFieldSize,
                left2RightHandlerChain,
                right2LeftHandlerChain,
                null, // remainingJoinCondition
                joinRuntimeTree,
                left2RightDrivenSideInfo,
                right2LeftDrivenSideInfo,
                testSpec.getLeftJoinKeySelector(),
                testSpec.getLeftUpsertKeySelector(),
                testSpec.getRightJoinKeySelector(),
                testSpec.getRightUpsertKeySelector(),
                fetchers,
                testSpec.getLeftInputTypeInfo(),
                testSpec.getRightInputTypeInfo(),
                enableCache);
    }

    private void insertLeftTable(AbstractBinaryTestSpec testSpec, StreamRecord<RowData> record) {
        insertTableData(testSpec, record.getValue(), true);
    }

    private void insertRightTable(AbstractBinaryTestSpec testSpec, StreamRecord<RowData> record) {
        insertTableData(testSpec, record.getValue(), false);
    }

    private void insertTableData(
            AbstractBinaryTestSpec testSpec, RowData rowData, boolean insertLeftTable) {
        try {
            synchronized (tableCurrentDataMap) {
                if (insertLeftTable) {
                    RowData upsertKey = testSpec.getLeftUpsertKeySelector().getKey(rowData);
                    tableCurrentDataMap
                            .computeIfAbsent(0, k -> new LinkedHashMap<>())
                            .put(upsertKey, rowData);
                } else {
                    RowData upsertKey = testSpec.getRightUpsertKeySelector().getKey(rowData);
                    tableCurrentDataMap
                            .computeIfAbsent(1, k -> new LinkedHashMap<>())
                            .put(upsertKey, rowData);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to insert table data", e);
        }
    }

    private RowData toBinary(RowData row, RowType rowType) {
        int size = row.getArity();
        Object[] fields = new Object[size];
        for (int i = 0; i < size; i++) {
            fields[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i).getFieldOrNull(row);
        }
        return binaryrow(fields);
    }

    private abstract static class AbstractBinaryTestSpec extends AbstractBaseTestSpec {

        abstract Optional<Function<RowData, Boolean>> getFilterOnLeftTable();

        abstract Optional<Function<RowData, Boolean>> getFilterOnRightTable();

        final GeneratedFilterCondition getGeneratedJoinCondition() {
            int leftFieldCount = getLeftInputRowType().getFieldCount();
            RowType appliedRowType = combineRowTypes(getLeftInputRowType(), getRightInputRowType());
            int[] allJoinKeyFields =
                    new int[getLeftJoinKeyIndices().length + getRightJoinKeyIndices().length];
            List<Function<GenericRowData, Boolean>> conditionsPerJoinKey = new ArrayList<>();
            for (int i = 0; i < getLeftJoinKeyIndices().length; i++) {
                allJoinKeyFields[2 * i] = getLeftJoinKeyIndices()[i];
                allJoinKeyFields[2 * i + 1] = leftFieldCount + getRightJoinKeyIndices()[i];
                int finalI = i;
                conditionsPerJoinKey.add(
                        row -> row.getField(finalI).equals(row.getField(finalI + 1)));
            }
            return getFilterCondition(
                    appliedRowType,
                    allJoinKeyFields,
                    row ->
                            conditionsPerJoinKey.stream()
                                    .allMatch(condition -> condition.apply(row)));
        }
    }

    /**
     * Mock sql like the following.
     *
     * <pre>
     *      CREATE TABLE leftSrc(
     *          left_value INT,
     *          left_jk1 BOOLEAN,
     *          left_jk2_index STRING,
     *          INDEX(left_jk2_index)
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE rightSrc(
     *          right_jk2 STRING,
     *          right_value INT,
     *          right_jk1_index BOOLEAN,
     *          INDEX(right_jk1_index)
     *      )
     * </pre>
     *
     * <p>If the flag {@link #filterOnTable} is false, the query is:
     *
     * <pre>
     *     select * from leftSrc join rightSrc
     *      on leftSrc.left_jk1 = rightSrc.right_jk1_index
     *      and leftSrc.left_jk2_index = rightSrc.right_jk2
     * </pre>
     *
     * <p>If the flag {@link #filterOnTable} is true, the query is:
     *
     * <pre>
     *     select * from (
     *      select * from leftSrc where left_jk1 = 'true'
     *     ) join (
     *      select * from rightSrc where right_jk2 = 'jklk1'
     *     ) on left_jk1 = right_jk1_index
     *      and left_jk2_index = right_jk2
     * </pre>
     */
    private static class LogLogTableJoinTestSpec extends AbstractBinaryTestSpec {

        private static final LogLogTableJoinTestSpec WITHOUT_FILTER_ON_TABLE =
                new LogLogTableJoinTestSpec(false);
        private static final LogLogTableJoinTestSpec WITH_FILTER_ON_TABLE =
                new LogLogTableJoinTestSpec(true);

        private final boolean filterOnTable;

        public LogLogTableJoinTestSpec(boolean filterOnTable) {
            this.filterOnTable = filterOnTable;
        }

        @Override
        RowType getLeftInputRowType() {
            return RowType.of(
                    new LogicalType[] {new IntType(), new BooleanType(), VarCharType.STRING_TYPE},
                    new String[] {"left_value", "left_jk1", "left_jk2_index"});
        }

        @Override
        RowType getRightInputRowType() {
            return RowType.of(
                    new LogicalType[] {VarCharType.STRING_TYPE, new IntType(), new BooleanType()},
                    new String[] {"right_jk2", "right_value", "right_jk1_index"});
        }

        @Override
        Optional<int[]> getLeftUpsertKey() {
            return Optional.empty();
        }

        @Override
        Optional<int[]> getRightUpsertKey() {
            return Optional.empty();
        }

        @Override
        int[] getLeftJoinKeyIndices() {
            return new int[] {1, 2};
        }

        @Override
        int[] getRightJoinKeyIndices() {
            return new int[] {2, 0};
        }

        @Override
        Optional<Function<RowData, Boolean>> getFilterOnLeftTable() {
            if (filterOnTable) {
                return Optional.of((rowData -> rowData.getBoolean(1)));
            }
            return Optional.empty();
        }

        @Override
        Optional<Function<RowData, Boolean>> getFilterOnRightTable() {
            if (filterOnTable) {
                return Optional.of((rowData -> "jklk1".equals(rowData.getString(0).toString())));
            }
            return Optional.empty();
        }
    }

    /**
     * Mock sql like the following.
     *
     * <pre>
     *      CREATE TABLE leftSrc(
     *          left_value INT,
     *          left_pk1 BOOLEAN,
     *          left_pk2_jk_index STRING,
     *          PRIMARY KEY (left_pk1, left_pk2_jk_index) NOT ENFORCED
     *          INDEX(left_pk2_jk_index)
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE rightSrc(
     *          right_pk2_jk_index STRING,
     *          right_value INT,
     *          right_pk1 BOOLEAN,
     *          PRIMARY KEY (right_pk2_jk_index, right_pk1) NOT ENFORCED
     *          INDEX(right_pk2_jk_index)
     *      )
     * </pre>
     *
     * <p>If the flag {@link #filterOnTable} is false, the query is:
     *
     * <pre>
     *     select * from leftSrc join rightSrc
     *      on leftSrc.left_pk2_jk_index = rightSrc.right_pk2_jk_index
     * </pre>
     *
     * <p>If the flag {@link #filterOnTable} is true, the query is:
     *
     * <pre>
     *     select * from (
     *       select * from leftSrc where left_pk1 = 'true'
     *     ) join (
     *       select * form rightSrc where right_pk1 = 'false'
     *     ) on left_pk2_jk_index = right_pk2_jk_index
     * </pre>
     */
    private static class PkPkTableJoinTestSpec extends AbstractBinaryTestSpec {

        private static final PkPkTableJoinTestSpec WITHOUT_FILTER_ON_TABLE =
                new PkPkTableJoinTestSpec(false);
        private static final PkPkTableJoinTestSpec WITH_FILTER_ON_TABLE =
                new PkPkTableJoinTestSpec(true);

        private final boolean filterOnTable;

        public PkPkTableJoinTestSpec(boolean filterOnTable) {
            this.filterOnTable = filterOnTable;
        }

        @Override
        RowType getLeftInputRowType() {
            return RowType.of(
                    new LogicalType[] {new IntType(), new BooleanType(), VarCharType.STRING_TYPE},
                    new String[] {"left_value", "left_pk1", "left_pk2_jk_index"});
        }

        @Override
        RowType getRightInputRowType() {
            return RowType.of(
                    new LogicalType[] {VarCharType.STRING_TYPE, new IntType(), new BooleanType()},
                    new String[] {"right_pk2_jk_index", "right_value", "right_pk1"});
        }

        @Override
        Optional<int[]> getLeftUpsertKey() {
            return Optional.of(new int[] {1, 2});
        }

        @Override
        Optional<int[]> getRightUpsertKey() {
            return Optional.of(new int[] {0, 2});
        }

        @Override
        int[] getLeftJoinKeyIndices() {
            return new int[] {2};
        }

        @Override
        int[] getRightJoinKeyIndices() {
            return new int[] {0};
        }

        @Override
        Optional<Function<RowData, Boolean>> getFilterOnLeftTable() {
            if (filterOnTable) {
                return Optional.of((rowData -> rowData.getBoolean(1)));
            }
            return Optional.empty();
        }

        @Override
        Optional<Function<RowData, Boolean>> getFilterOnRightTable() {
            if (filterOnTable) {
                return Optional.of((rowData -> !rowData.getBoolean(2)));
            }
            return Optional.empty();
        }
    }
}
