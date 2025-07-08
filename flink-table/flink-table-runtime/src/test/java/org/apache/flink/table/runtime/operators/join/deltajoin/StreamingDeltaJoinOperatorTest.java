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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunctionWrapper;
import org.apache.flink.table.runtime.generated.GeneratedResultFutureWrapper;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.AecRecord;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.RecordsBuffer;
import org.apache.flink.table.runtime.operators.join.lookup.keyordered.TableAsyncExecutionController;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test class for {@link StreamingDeltaJoinOperator}. */
public class StreamingDeltaJoinOperatorTest {

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> testHarness;

    private static final int AEC_CAPACITY = 100;

    // the data snapshot of the left/right table when joining
    private static final LinkedList<RowData> leftTableCurrentData = new LinkedList<>();
    private static final LinkedList<RowData> rightTableCurrentData = new LinkedList<>();

    /**
     * Mock sql like the following.
     *
     * <pre>
     *      CREATE TABLE leftSrc(
     *          left_value INT,
     *          left_jk1 BOOLEAN,
     *          left_jk2_lk VARCHAR,
     *          INDEX(left_jk2_lk)
     *      )
     * </pre>
     *
     * <pre>
     *      CREATE TABLE rightSrc(
     *          right_jk2 STRING,
     *          right_value INT,
     *          right_jk1_lk VARCHAR,
     *          INDEX(right_jk1_lk)
     *      )
     * </pre>
     *
     * <pre>
     *     select * from leftSrc join rightSrc
     *      on leftSrc.left_jk1 = rightSrc.right_jk1_lk
     *      and leftSrc.left_jk2_lk = rightSrc.right_jk2
     * </pre>
     *
     * <p>For right lookup table(left stream side delta join right table), the join key is
     * [right_jk1_lk, right_jk2], and it will be split into lookup key [right_jk1_lk] and {@code
     * DeltaJoinSpec#remainingCondition} [right_jk2].
     *
     * <p>For left lookup table(right stream side delta join left table), the join key is [left_jk1,
     * left_jk2_lk], and it will be split into lookup key [left_jk2_lk] and {@code
     * DeltaJoinSpec#remainingCondition} [left_jk1].
     */

    // left join key: <left_jk1, left_jk2_lk>
    // left lookup key: <left_jk2_lk>
    private static final InternalTypeInfo<RowData> leftTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                new IntType(), new BooleanType(), VarCharType.STRING_TYPE
                            },
                            new String[] {"left_value", "left_jk1", "left_jk2_lk"}));

    private static final int[] leftJoinKeyIndices = new int[] {1, 2};

    // right join key: <right_jk1_lk, right_jk2>
    // right lookup key: <right_jk1_lk>
    private static final InternalTypeInfo<RowData> rightTypeInfo =
            InternalTypeInfo.of(
                    RowType.of(
                            new LogicalType[] {
                                VarCharType.STRING_TYPE, new IntType(), new BooleanType()
                            },
                            new String[] {"right_jk2", "right_value", "right_jk1_lk"}));
    private static final int[] rightJoinKeyIndices = new int[] {2, 0};

    private static final RowDataKeySelector leftJoinKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    leftJoinKeyIndices,
                    leftTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));
    private static final RowDataKeySelector rightJoinKeySelector =
            HandwrittenSelectorUtil.getRowDataSelector(
                    rightJoinKeyIndices,
                    rightTypeInfo.toRowType().getChildren().toArray(new LogicalType[0]));

    private static final int[] outputUpsertKeyIndices = leftJoinKeyIndices;

    private RowDataHarnessAssertor assertor;

    private Optional<Throwable> latestException = Optional.empty();

    @BeforeEach
    public void beforeEach() throws Exception {
        testHarness = createDeltaJoinOperatorTestHarness();
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
                new MyAsyncExecutionControllerDelegate(operator.getAsyncExecutionController()));
        prepareOperatorRuntimeInfo(operator);

        assertor =
                new RowDataHarnessAssertor(
                        getOutputType().getChildren().toArray(new LogicalType[0]),
                        // sort the result by the output upsert key
                        (o1, o2) -> {
                            for (int keyIndex : outputUpsertKeyIndices) {
                                LogicalType type = getOutputType().getChildren().get(keyIndex);
                                RowData.FieldGetter getter =
                                        RowData.createFieldGetter(type, keyIndex);

                                int compareResult =
                                        Objects.requireNonNull(getter.getFieldOrNull(o1))
                                                .toString()
                                                .compareTo(
                                                        Objects.requireNonNull(
                                                                        getter.getFieldOrNull(o2))
                                                                .toString());

                                if (compareResult != 0) {
                                    return compareResult;
                                }
                            }
                            return o1.toString().compareTo(o2.toString());
                        });
        MyAsyncFunction.leftInvokeCount.set(0);
        MyAsyncFunction.rightInvokeCount.set(0);
        MyAsyncExecutionControllerDelegate.insertTableDataAfterEmit = true;
    }

    @AfterEach
    public void afterEach() throws Exception {
        testHarness.close();
        leftTableCurrentData.clear();
        rightTableCurrentData.clear();
        latestException = Optional.empty();
        MyAsyncFunction.clearExpectedThrownException();
    }

    @Test
    void testJoinBothAppendOnlyTables() throws Exception {
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
    }

    @Test
    void testBlockingWithSameJoinKey() throws Exception {
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
    }

    /**
     * This test is used to test the scenario where the right stream side joined out a record from
     * the left table that has not been sent to the delta-join operator (maybe is in flight between
     * source and delta-join).
     */
    @Test
    void testTableDataVisibleBeforeJoin() throws Exception {
        MyAsyncExecutionControllerDelegate.insertTableDataAfterEmit = false;

        // prepare the data first to mock all following requests were in flight between source and
        // delta-join
        final StreamRecord<RowData> leftRecord1 = insertRecord(100, true, "jklk1");
        insertLeftTable(leftRecord1);

        final StreamRecord<RowData> leftRecord2 = insertRecord(200, true, "jklk1");
        insertLeftTable(leftRecord2);

        final StreamRecord<RowData> rightRecord1 = insertRecord("jklk1", 300, true);
        insertRightTable(rightRecord1);

        // mismatch
        final StreamRecord<RowData> rightRecord2 = insertRecord("jklk2", 500, false);
        insertRightTable(rightRecord2);

        final StreamRecord<RowData> leftRecord3 = insertRecord(800, true, "jklk1");
        insertLeftTable(leftRecord3);

        final StreamRecord<RowData> rightRecord3 = insertRecord("jklk1", 1000, true);
        insertRightTable(rightRecord3);

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
    }

    @Test
    void testCheckpointAndRestore() throws Exception {
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

        MyAsyncFunction.block();
        // restoring
        testHarness = createDeltaJoinOperatorTestHarness();

        testHarness.setup();

        StreamingDeltaJoinOperator operator = unwrapOperator(testHarness);
        operator.setAsyncExecutionController(
                new MyAsyncExecutionControllerDelegate(operator.getAsyncExecutionController()));

        latestException = Optional.empty();
        testHarness.initializeState(snapshot);

        testHarness.open();
        prepareOperatorRuntimeInfo(operator);

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
    }

    @Test
    void testClearLegacyStateWhenCheckpointing() throws Exception {
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

    @Test
    void testMeetExceptionWhenLookup() throws Exception {
        Throwable expectedException = new IllegalStateException("Mock to fail");
        MyAsyncFunction.setExpectedThrownException(expectedException);

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

    private void waitAllDataProcessed() throws Exception {
        testHarness.endAllInputs();
        if (latestException.isPresent()) {
            throw new IllegalStateException(
                    "Failed to wait all data processed", latestException.get());
        }
    }

    private KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            createDeltaJoinOperatorTestHarness() throws Exception {
        TaskMailbox mailbox = new TaskMailboxImpl();
        MailboxProcessor mailboxProcessor =
                new MailboxProcessor(controller -> {}, mailbox, StreamTaskActionExecutor.IMMEDIATE);

        DataStructureConverter<RowData, Object> leftFetcherConverter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(leftTypeInfo.getDataType());

        AsyncDeltaJoinRunner leftAsyncFunction =
                new AsyncDeltaJoinRunner(
                        new GeneratedFunctionWrapper<>(new MyAsyncFunction()),
                        leftFetcherConverter,
                        new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
                        leftTypeInfo.toRowSerializer(),
                        AEC_CAPACITY,
                        false);

        DataStructureConverter<RowData, Object> rightFetcherConverter =
                (DataStructureConverter)
                        DataStructureConverters.getConverter(rightTypeInfo.getDataType());

        AsyncDeltaJoinRunner rightAsyncFunction =
                new AsyncDeltaJoinRunner(
                        new GeneratedFunctionWrapper<>(new MyAsyncFunction()),
                        rightFetcherConverter,
                        new GeneratedResultFutureWrapper<>(new TestingFetcherResultFuture()),
                        rightTypeInfo.toRowSerializer(),
                        AEC_CAPACITY,
                        true);

        InternalTypeInfo<RowData> joinKeyTypeInfo = leftJoinKeySelector.getProducedType();

        StreamingDeltaJoinOperator operator =
                new StreamingDeltaJoinOperator(
                        rightAsyncFunction,
                        leftAsyncFunction,
                        leftJoinKeySelector,
                        rightJoinKeySelector,
                        -1L,
                        AEC_CAPACITY,
                        new TestProcessingTimeService(),
                        new MailboxExecutorImpl(
                                mailbox, 0, StreamTaskActionExecutor.IMMEDIATE, mailboxProcessor),
                        (RowType) leftTypeInfo.toLogicalType(),
                        (RowType) rightTypeInfo.toLogicalType());

        return new KeyedTwoInputStreamOperatorTestHarness<>(
                operator,
                leftJoinKeySelector,
                rightJoinKeySelector,
                joinKeyTypeInfo,
                1,
                1,
                0,
                leftTypeInfo.toSerializer(),
                rightTypeInfo.toSerializer());
    }

    private void prepareOperatorRuntimeInfo(StreamingDeltaJoinOperator operator) {
        unwrapAsyncFunction(operator, true).tagInvokingSideDuringRuntime(true);
        unwrapAsyncFunction(operator, false).tagInvokingSideDuringRuntime(false);
    }

    private MyAsyncFunction unwrapAsyncFunction(
            StreamingDeltaJoinOperator operator, boolean unwrapLeft) {
        if (unwrapLeft) {
            return (MyAsyncFunction) operator.getLeftTriggeredUserFunction().getFetcher();
        } else {
            return (MyAsyncFunction) operator.getRightTriggeredUserFunction().getFetcher();
        }
    }

    private TableAsyncExecutionController<RowData, RowData, RowData> unwrapAEC(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    testHarness) {
        return unwrapOperator(testHarness).getAsyncExecutionController();
    }

    private StreamingDeltaJoinOperator unwrapOperator(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
                    testHarness) {
        return (StreamingDeltaJoinOperator) testHarness.getOperator();
    }

    private RowType getOutputType() {
        return RowType.of(
                Stream.concat(
                                leftTypeInfo.toRowType().getChildren().stream(),
                                rightTypeInfo.toRowType().getChildren().stream())
                        .toArray(LogicalType[]::new),
                Stream.concat(
                                leftTypeInfo.toRowType().getFieldNames().stream(),
                                rightTypeInfo.toRowType().getFieldNames().stream())
                        .toArray(String[]::new));
    }

    private void insertLeftTable(StreamRecord<RowData> record) {
        insertTableData(record, true);
    }

    private void insertRightTable(StreamRecord<RowData> record) {
        insertTableData(record, false);
    }

    private static void insertTableData(StreamRecord<RowData> record, boolean insertLeftTable) {
        RowData rowData = record.getValue();
        try {
            if (insertLeftTable) {
                synchronized (leftTableCurrentData) {
                    leftTableCurrentData.add(rowData);
                }
            } else {
                synchronized (rightTableCurrentData) {
                    rightTableCurrentData.add(rowData);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to insert table data", e);
        }
    }

    /** An async function used for test. */
    public static class MyAsyncFunction extends RichAsyncFunction<RowData, Object> {

        private static final long serialVersionUID = 1L;

        private static final long TERMINATION_TIMEOUT = 5000L;
        private static final int THREAD_POOL_SIZE = 10;

        private static ExecutorService executorService;

        private static @Nullable CountDownLatch lock;

        private static final AtomicInteger leftInvokeCount = new AtomicInteger(0);

        private static final AtomicInteger rightInvokeCount = new AtomicInteger(0);

        private static Optional<Throwable> expectedThrownException = Optional.empty();

        // ===== runtime info =====
        private Boolean treatRightAsLookupTable;

        public void tagInvokingSideDuringRuntime(boolean isLeftInvoking) {
            this.treatRightAsLookupTable = isLeftInvoking;
        }

        public static void block() throws Exception {
            lock = new CountDownLatch(1);
        }

        public static void release() {
            Objects.requireNonNull(lock).countDown();
        }

        public static void setExpectedThrownException(Throwable t) {
            expectedThrownException = Optional.of(t);
        }

        public static void clearExpectedThrownException() {
            expectedThrownException = Optional.empty();
        }

        @Override
        public void asyncInvoke(final RowData input, final ResultFuture<Object> resultFuture) {
            executorService.submit(
                    () -> {
                        try {
                            if (expectedThrownException.isPresent()) {
                                throw expectedThrownException.get();
                            }

                            if (lock != null) {
                                lock.await();
                            }

                            LinkedList<RowData> lookupTableData;
                            RowDataKeySelector streamSideJoinKeySelector;
                            RowDataKeySelector lookupSideJoinKeySelector;
                            if (Objects.requireNonNull(treatRightAsLookupTable)) {
                                synchronized (rightTableCurrentData) {
                                    lookupTableData = new LinkedList<>(rightTableCurrentData);
                                }

                                streamSideJoinKeySelector = leftJoinKeySelector.copy();
                                lookupSideJoinKeySelector = rightJoinKeySelector.copy();
                                leftInvokeCount.incrementAndGet();
                            } else {
                                synchronized (leftTableCurrentData) {
                                    lookupTableData = new LinkedList<>(leftTableCurrentData);
                                }

                                streamSideJoinKeySelector = rightJoinKeySelector.copy();
                                lookupSideJoinKeySelector = leftJoinKeySelector.copy();
                                rightInvokeCount.incrementAndGet();
                            }

                            List<Object> results = new ArrayList<>();
                            for (RowData row : lookupTableData) {
                                if (streamSideJoinKeySelector
                                        .getKey(input)
                                        .equals(lookupSideJoinKeySelector.getKey(row))) {
                                    results.add(row);
                                }
                            }

                            resultFuture.complete(results);
                        } catch (Throwable e) {
                            resultFuture.completeExceptionally(
                                    new RuntimeException("Failed to look up table", e));
                        }
                    });
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);

            synchronized (MyAsyncFunction.class) {
                if (executorService == null) {
                    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
                }
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            freeExecutor();
        }

        private void freeExecutor() {
            synchronized (MyAsyncFunction.class) {
                if (executorService == null) {
                    return;
                }

                executorService.shutdown();

                try {
                    if (!executorService.awaitTermination(
                            TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        executorService.shutdownNow();
                    }
                } catch (InterruptedException interrupted) {
                    executorService.shutdownNow();

                    Thread.currentThread().interrupt();
                }
                executorService = null;
            }
        }
    }

    /** Provide some callback methods for test. */
    private static class MyAsyncExecutionControllerDelegate
            extends TableAsyncExecutionController<RowData, RowData, RowData> {

        private static boolean insertTableDataAfterEmit = true;

        public MyAsyncExecutionControllerDelegate(
                TableAsyncExecutionController<RowData, RowData, RowData> innerAec) {
            super(
                    innerAec.getAsyncInvoke(),
                    innerAec.getEmitWatermark(),
                    entry -> {
                        if (insertTableDataAfterEmit) {
                            StreamingDeltaJoinOperator.InputIndexAwareStreamRecordQueueEntry
                                    inputIndexAwareEntry =
                                            ((StreamingDeltaJoinOperator
                                                            .InputIndexAwareStreamRecordQueueEntry)
                                                    entry);
                            int inputIndex = inputIndexAwareEntry.getInputIndex();
                            //noinspection unchecked
                            insertTableData(
                                    (StreamRecord<RowData>) inputIndexAwareEntry.getInputElement(),
                                    inputIndex == 0);
                        }

                        innerAec.getEmitResult().accept(entry);
                    },
                    innerAec.getInferDrivenInputIndex(),
                    innerAec.getInferBlockingKey());
        }
    }

    /**
     * The {@link TestingFetcherResultFuture} is a simple implementation of {@link
     * TableFunctionCollector} which forwards the collected collection.
     */
    public static final class TestingFetcherResultFuture
            extends TableFunctionResultFuture<RowData> {
        private static final long serialVersionUID = -312754413938303160L;

        @Override
        public void complete(Collection<RowData> result) {
            //noinspection unchecked
            getResultFuture().complete((Collection) result);
        }
    }
}
