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

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTestHarness;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.runtime.operators.sort.IntNormalizedKeyComputer;
import org.apache.flink.table.runtime.operators.sort.IntRecordComparator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import static java.lang.Long.valueOf;
import static org.apache.flink.table.runtime.util.JoinUtil.getJoinType;
import static org.assertj.core.api.Assertions.assertThat;

/** Base test class for {@link HashJoinOperator}. */
public abstract class Int2HashJoinOperatorTestBase implements Serializable {

    public void buildJoin(
            MutableObjectIterator<BinaryRowData> buildInput,
            MutableObjectIterator<BinaryRowData> probeInput,
            boolean leftOut,
            boolean rightOut,
            boolean buildLeft,
            int expectOutSize,
            int expectOutKeySize,
            int expectOutVal)
            throws Exception {
        FlinkJoinType flinkJoinType = getJoinType(leftOut, rightOut);
        HashJoinType hashJoinType = HashJoinType.of(buildLeft, leftOut, rightOut);
        Object operator =
                newOperator(33 * 32 * 1024, flinkJoinType, hashJoinType, buildLeft, !buildLeft);
        joinAndAssert(
                operator,
                buildInput,
                probeInput,
                expectOutSize,
                expectOutKeySize,
                expectOutVal,
                false);
    }

    public Object newOperator(
            long memorySize,
            FlinkJoinType flinkJoinType,
            HashJoinType hashJoinType,
            boolean buildLeft,
            boolean reverseJoinFunction) {
        GeneratedJoinCondition condFuncCode =
                new GeneratedJoinCondition("", "", new Object[0]) {
                    @Override
                    public JoinCondition newInstance(ClassLoader classLoader) {
                        return new TrueCondition();
                    }
                };
        GeneratedProjection buildProjectionCode =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        return new MyProjection();
                    }
                };
        GeneratedProjection probeProjectionCode =
                new GeneratedProjection("", "", new Object[0]) {
                    @Override
                    public Projection newInstance(ClassLoader classLoader) {
                        return new MyProjection();
                    }
                };
        GeneratedNormalizedKeyComputer computer1 =
                new GeneratedNormalizedKeyComputer("", "") {
                    @Override
                    public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                        return new IntNormalizedKeyComputer();
                    }
                };
        GeneratedRecordComparator comparator1 =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };

        GeneratedNormalizedKeyComputer computer2 =
                new GeneratedNormalizedKeyComputer("", "") {
                    @Override
                    public NormalizedKeyComputer newInstance(ClassLoader classLoader) {
                        return new IntNormalizedKeyComputer();
                    }
                };
        GeneratedRecordComparator comparator2 =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };
        GeneratedRecordComparator genKeyComparator =
                new GeneratedRecordComparator("", "", new Object[0]) {
                    @Override
                    public RecordComparator newInstance(ClassLoader classLoader) {
                        return new IntRecordComparator();
                    }
                };
        boolean[] filterNulls = new boolean[] {true};

        int maxNumFileHandles =
                ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES.defaultValue();
        boolean compressionEnabled =
                ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED.defaultValue();
        int compressionBlockSize =
                (int)
                        ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE
                                .defaultValue()
                                .getBytes();
        boolean asyncMergeEnabled =
                ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED.defaultValue();

        SortMergeJoinFunction sortMergeJoinFunction;
        if (buildLeft) {
            sortMergeJoinFunction =
                    new SortMergeJoinFunction(
                            0,
                            flinkJoinType,
                            buildLeft,
                            maxNumFileHandles,
                            compressionEnabled,
                            compressionBlockSize,
                            asyncMergeEnabled,
                            condFuncCode,
                            buildProjectionCode,
                            probeProjectionCode,
                            computer1,
                            comparator1,
                            computer2,
                            comparator2,
                            genKeyComparator,
                            filterNulls);
        } else {
            sortMergeJoinFunction =
                    new SortMergeJoinFunction(
                            0,
                            flinkJoinType,
                            buildLeft,
                            maxNumFileHandles,
                            compressionEnabled,
                            compressionBlockSize,
                            asyncMergeEnabled,
                            condFuncCode,
                            probeProjectionCode,
                            buildProjectionCode,
                            computer2,
                            comparator2,
                            computer1,
                            comparator1,
                            genKeyComparator,
                            filterNulls);
        }

        return HashJoinOperator.newHashJoinOperator(
                hashJoinType,
                buildLeft,
                compressionEnabled,
                compressionBlockSize,
                condFuncCode,
                reverseJoinFunction,
                filterNulls,
                buildProjectionCode,
                probeProjectionCode,
                false,
                20,
                10000,
                10000,
                RowType.of(new IntType()),
                sortMergeJoinFunction);
    }

    @SuppressWarnings("unchecked")
    public static void joinAndAssert(
            Object operator,
            MutableObjectIterator<BinaryRowData> input1,
            MutableObjectIterator<BinaryRowData> input2,
            int expectOutSize,
            int expectOutKeySize,
            int expectOutVal,
            boolean semiJoin)
            throws Exception {
        InternalTypeInfo<RowData> typeInfo =
                InternalTypeInfo.ofFields(new IntType(), new IntType());
        InternalTypeInfo<RowData> rowDataTypeInfo =
                InternalTypeInfo.ofFields(
                        new IntType(), new IntType(), new IntType(), new IntType());
        TwoInputStreamTaskTestHarness<BinaryRowData, BinaryRowData, JoinedRowData> testHarness =
                new TwoInputStreamTaskTestHarness<>(
                        TwoInputStreamTask::new,
                        2,
                        1,
                        new int[] {1, 2},
                        typeInfo,
                        (TypeInformation) typeInfo,
                        rowDataTypeInfo);
        testHarness.memorySize = 3 * 1024 * 1024;
        testHarness.getExecutionConfig().enableObjectReuse();
        testHarness.setupOutputForSingletonOperatorChain();
        if (operator instanceof StreamOperator) {
            testHarness.getStreamConfig().setStreamOperator((StreamOperator<?>) operator);
        } else {
            testHarness
                    .getStreamConfig()
                    .setStreamOperatorFactory((StreamOperatorFactory<?>) operator);
        }
        testHarness.getStreamConfig().setOperatorID(new OperatorID());
        testHarness
                .getStreamConfig()
                .setManagedMemoryFractionOperatorOfUseCase(ManagedMemoryUseCase.OPERATOR, 0.99);

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        Random random = new Random();
        do {
            BinaryRowData row1 = null;
            BinaryRowData row2 = null;

            if (random.nextInt(2) == 0) {
                row1 = input1.next();
                if (row1 == null) {
                    row2 = input2.next();
                }
            } else {
                row2 = input2.next();
                if (row2 == null) {
                    row1 = input1.next();
                }
            }

            if (row1 == null && row2 == null) {
                break;
            }

            if (row1 != null) {
                testHarness.processElement(new StreamRecord<>(row1), 0, 0);
            } else {
                testHarness.processElement(new StreamRecord<>(row2), 1, 0);
            }
        } while (true);

        testHarness.endInput(0, 0);
        testHarness.endInput(1, 0);

        testHarness.waitForInputProcessing();
        testHarness.waitForTaskCompletion();

        Queue<Object> actual = testHarness.getOutput();

        assertThat(actual).as("Output was not correct.").hasSize(expectOutSize);

        // Don't verify the output value when experOutVal is -1
        if (expectOutVal != -1) {
            if (semiJoin) {
                HashMap<Integer, Long> map = new HashMap<>(expectOutKeySize);

                for (Object o : actual) {
                    StreamRecord<RowData> record = (StreamRecord<RowData>) o;
                    RowData row = record.getValue();
                    int key = row.getInt(0);
                    int val = row.getInt(1);
                    Long contained = map.get(key);
                    if (contained == null) {
                        contained = (long) val;
                    } else {
                        contained = valueOf(contained + val);
                    }
                    map.put(key, contained);
                }

                assertThat(map).as("Wrong number of keys").hasSize(expectOutKeySize);
                for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                    long val = entry.getValue();
                    int key = entry.getKey();

                    assertThat(val)
                            .as("Wrong number of values in per-key cross product for key " + key)
                            .isEqualTo(expectOutVal);
                }
            } else {
                // create the map for validating the results
                HashMap<Integer, Long> map = new HashMap<>(expectOutKeySize);

                for (Object o : actual) {
                    StreamRecord<RowData> record = (StreamRecord<RowData>) o;
                    RowData row = record.getValue();
                    int key = row.isNullAt(0) ? row.getInt(2) : row.getInt(0);

                    int val1 = 0;
                    int val2 = 0;
                    if (!row.isNullAt(1)) {
                        val1 = row.getInt(1);
                    }
                    if (!row.isNullAt(3)) {
                        val2 = row.getInt(3);
                    }
                    int val = val1 + val2;

                    Long contained = map.get(key);
                    if (contained == null) {
                        contained = (long) val;
                    } else {
                        contained = valueOf(contained + val);
                    }
                    map.put(key, contained);
                }

                assertThat(map).as("Wrong number of keys").hasSize(expectOutKeySize);
                for (Map.Entry<Integer, Long> entry : map.entrySet()) {
                    long val = entry.getValue();
                    int key = entry.getKey();

                    assertThat(val)
                            .as("Wrong number of values in per-key cross product for key " + key)
                            .isEqualTo(expectOutVal);
                }
            }
        }
    }

    /** my projection. */
    public static final class MyProjection implements Projection<RowData, BinaryRowData> {

        BinaryRowData innerRow = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(innerRow);

        @Override
        public BinaryRowData apply(RowData row) {
            writer.reset();
            if (row.isNullAt(0)) {
                writer.setNullAt(0);
            } else {
                writer.writeInt(0, row.getInt(0));
            }
            writer.complete();
            return innerRow;
        }
    }

    /** Test util. */
    public static class TrueCondition extends AbstractRichFunction implements JoinCondition {

        @Override
        public boolean apply(RowData in1, RowData in2) {
            return true;
        }
    }

    /** Test cond. */
    public static class MyJoinCondition extends AbstractRichFunction implements JoinCondition {

        public MyJoinCondition(Object[] reference) {}

        @Override
        public boolean apply(RowData in1, RowData in2) {
            return true;
        }
    }
}
