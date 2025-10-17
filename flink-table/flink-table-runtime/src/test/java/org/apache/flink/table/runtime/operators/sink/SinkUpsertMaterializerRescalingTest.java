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

package org.apache.flink.table.runtime.operators.sink;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedHashFunction;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetStateConfig;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.utils.HandwrittenSelectorUtil;
import org.apache.flink.types.RowKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;

/** Rescaling and migration unit tests for {@link SinkUpsertMaterializer}. */
@RunWith(Parameterized.class)
public class SinkUpsertMaterializerRescalingTest {

    @Parameter(0)
    public SinkUpsertMaterializerVersion sumVersion;

    @Parameter(1)
    public SinkUpsertMaterializerStateBackend backend;

    @Parameterized.Parameters(name = "sumVersion={0}, stateBackend={1}")
    public static Object[][] generateTestParameters() {
        List<Object[]> result = new ArrayList<>();
        for (SinkUpsertMaterializerVersion sumVersion : SinkUpsertMaterializerVersion.values()) {
            for (SinkUpsertMaterializerStateBackend backend :
                    SinkUpsertMaterializerStateBackend.values()) {
                result.add(new Object[] {sumVersion, backend});
            }
        }
        return result.toArray(new Object[0][]);
    }

    @Test
    public void testScaleUpThenDown() throws Exception {
        testRescaleFromToFrom(10, 2, 3, backend, backend, sumVersion);
    }

    @Test
    public void testScaleDownThenUp() throws Exception {
        testRescaleFromToFrom(10, 3, 2, backend, backend, sumVersion);
    }

    @Test
    public void testRecovery() throws Exception {
        testRescaleFromToFrom(1, 1, 1, backend, backend, sumVersion);
    }

    @Test
    public void testForwardAndBackwardMigration() throws Exception {
        testRescaleFromToFrom(7, 3, 3, backend, getOtherBackend(backend), sumVersion);
    }

    @Test
    public void testScaleUpThenDownWithMigration() throws Exception {
        testRescaleFromToFrom(7, 1, 5, backend, getOtherBackend(backend), sumVersion);
    }

    @Test
    public void testScaleDownThenUpWithMigration() throws Exception {
        testRescaleFromToFrom(
                7,
                5,
                1,
                backend,
                getOtherBackend(SinkUpsertMaterializerStateBackend.HEAP),
                sumVersion);
    }

    private SinkUpsertMaterializerStateBackend getOtherBackend(
            SinkUpsertMaterializerStateBackend backend) {
        return backend == SinkUpsertMaterializerStateBackend.HEAP
                ? SinkUpsertMaterializerStateBackend.ROCKSDB
                : SinkUpsertMaterializerStateBackend.HEAP;
    }

    @SuppressWarnings("unchecked")
    private void testRescaleFromToFrom(
            final int maxParallelism,
            final int fromParallelism,
            final int toParallelism,
            final SinkUpsertMaterializerStateBackend fromBackend,
            final SinkUpsertMaterializerStateBackend toBackend,
            final SinkUpsertMaterializerVersion sumVersion)
            throws Exception {

        int[] currentParallelismRef = new int[] {fromParallelism};

        boolean useSavepoint = fromBackend != toBackend;

        OneInputStreamOperator<RowData, RowData>[] materializers =
                new OneInputStreamOperator[maxParallelism];
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>[] harnesses =
                new KeyedOneInputStreamOperatorTestHarness[maxParallelism];

        final ToIntFunction<StreamRecord<RowData>> combinedHarnesses =
                (r) -> {
                    try {
                        int subtaskIndex =
                                KeyGroupRangeAssignment.assignKeyToParallelOperator(
                                        KEY_SELECTOR.getKey(r.getValue()),
                                        maxParallelism,
                                        currentParallelismRef[0]);

                        harnesses[subtaskIndex].processElement(r);
                        return subtaskIndex;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                };

        initHarnessesAndMaterializers(
                harnesses,
                materializers,
                fromBackend,
                maxParallelism,
                fromParallelism,
                null,
                sumVersion);

        int idx = combinedHarnesses.applyAsInt(insertRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        idx = combinedHarnesses.applyAsInt(insertRecord(2L, 1, "a2"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2"));

        List<OperatorSubtaskState> subtaskStates =
                snapshotHarnesses(harnesses, fromParallelism, 1L, useSavepoint);

        currentParallelismRef[0] = toParallelism;
        initHarnessesAndMaterializers(
                harnesses,
                materializers,
                toBackend,
                maxParallelism,
                toParallelism,
                subtaskStates,
                sumVersion);

        idx = combinedHarnesses.applyAsInt(insertRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        idx = combinedHarnesses.applyAsInt(insertRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.UPDATE_AFTER, 4L, 1, "a4"));

        subtaskStates = snapshotHarnesses(harnesses, toParallelism, 2L, useSavepoint);

        currentParallelismRef[0] = fromParallelism;
        initHarnessesAndMaterializers(
                harnesses,
                materializers,
                fromBackend,
                maxParallelism,
                fromParallelism,
                subtaskStates,
                sumVersion);

        idx = combinedHarnesses.applyAsInt(deleteRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        idx = combinedHarnesses.applyAsInt(deleteRecord(2L, 1, "a2"));
        ASSERTOR.shouldEmitNothing(harnesses[idx]);

        idx = combinedHarnesses.applyAsInt(deleteRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a1"));

        idx = combinedHarnesses.applyAsInt(deleteRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.DELETE, 1L, 1, "a1"));

        idx = combinedHarnesses.applyAsInt(insertRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        Arrays.stream(harnesses)
                .filter(Objects::nonNull)
                .forEach(h -> h.setStateTtlProcessingTime(1002));

        idx = combinedHarnesses.applyAsInt(deleteRecord(4L, 1, "a4"));
        if (sumVersion.isTtlSupported()) {
            ASSERTOR.shouldEmitNothing(harnesses[idx]);
        } else {
            ASSERTOR.shouldEmit(harnesses[idx], rowOfKind(RowKind.DELETE, 4L, 1, "a4"));
        }

        Arrays.stream(harnesses)
                .filter(Objects::nonNull)
                .forEach(
                        h -> {
                            try {
                                h.close();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private void initHarnessesAndMaterializers(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>[] harnesses,
            OneInputStreamOperator<RowData, RowData>[] materializers,
            SinkUpsertMaterializerStateBackend backend,
            int maxParallelism,
            int parallelism,
            @Nullable List<OperatorSubtaskState> subtaskStates,
            SinkUpsertMaterializerVersion sumVersion)
            throws Exception {
        for (int i = 0; i < parallelism; ++i) {
            switch (sumVersion) {
                case V1:
                    materializers[i] =
                            SinkUpsertMaterializer.create(
                                    TTL_CONFIG,
                                    RowType.of(LOGICAL_TYPES),
                                    EQUALISER,
                                    UPSERT_KEY_EQUALISER,
                                    null);
                    break;
                case V2:
                    materializers[i] =
                            SinkUpsertMaterializerV2.create(
                                    RowType.of(LOGICAL_TYPES),
                                    EQUALISER,
                                    UPSERT_KEY_EQUALISER,
                                    HASH_FUNCTION,
                                    UPSERT_KEY_HASH_FUNCTION,
                                    null,
                                    SequencedMultiSetStateConfig.defaults(
                                            TimeDomain.PROCESSING_TIME,
                                            sumVersion.reconfigureTtl(TTL_CONFIG)));
                    break;
                default:
                    throw new IllegalArgumentException("unknown version: " + sumVersion);
            }
            harnesses[i] =
                    new KeyedOneInputStreamOperatorTestHarness<>(
                            materializers[i],
                            KEY_SELECTOR,
                            KEY_SELECTOR.getProducedType(),
                            maxParallelism,
                            parallelism,
                            i);

            harnesses[i].setStateBackend(backend.create(false));

            if (subtaskStates != null) {
                OperatorSubtaskState operatorSubtaskState =
                        AbstractStreamOperatorTestHarness.repackageState(
                                subtaskStates.toArray(new OperatorSubtaskState[0]));

                harnesses[i].initializeState(
                        AbstractStreamOperatorTestHarness.repartitionOperatorState(
                                operatorSubtaskState,
                                maxParallelism,
                                subtaskStates.size(),
                                parallelism,
                                i));
            }

            harnesses[i].open();
            harnesses[i].setStateTtlProcessingTime(1);
        }
    }

    private List<OperatorSubtaskState> snapshotHarnesses(
            KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData>[] harnesses,
            int parallelism,
            long checkpointId,
            boolean useSavepoint) {
        return Arrays.stream(harnesses, 0, parallelism)
                .map(
                        h -> {
                            try {
                                return h.snapshotWithLocalState(
                                                checkpointId,
                                                0L,
                                                useSavepoint
                                                        ? SavepointType.savepoint(
                                                                SavepointFormatType.CANONICAL)
                                                        : CheckpointType.CHECKPOINT)
                                        .getJobManagerOwnedState();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                .collect(Collectors.toList());
    }

    /** Test equalizer for records. */
    protected static class TestRecordEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(0) == row2.getLong(0)
                    && row1.getInt(1) == row2.getInt(1)
                    && row1.getString(2).equals(row2.getString(2));
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(rd.getRowKind(), rd.getLong(0), rd.getInt(1), rd.getString(2));
        }
    }

    /** Test equalizer for upsert keys. */
    protected static class TestUpsertKeyEqualiser implements RecordEqualiser, HashFunction {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind() && row1.getLong(0) == row2.getLong(0);
        }

        @Override
        public int hashCode(Object data) {
            RowData rd = (RowData) data;
            return Objects.hash(rd.getRowKind(), rd.getLong(0));
        }
    }

    private static class MyGeneratedRecordEqualiser extends GeneratedRecordEqualiser {

        public MyGeneratedRecordEqualiser() {
            super("", "", new Object[0]);
        }

        @Override
        public RecordEqualiser newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser();
        }
    }

    private static class MyGeneratedHashFunction extends GeneratedHashFunction {

        public MyGeneratedHashFunction() {
            super("", "", new Object[0], new Configuration());
        }

        @Override
        public HashFunction newInstance(ClassLoader classLoader) {
            return new TestRecordEqualiser();
        }
    }

    private static final StateTtlConfig TTL_CONFIG = StateConfigUtil.createTtlConfig(1000);

    private static final LogicalType[] LOGICAL_TYPES =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};

    private static final RowDataKeySelector KEY_SELECTOR =
            HandwrittenSelectorUtil.getRowDataSelector(new int[] {1}, LOGICAL_TYPES);

    private static final RowDataHarnessAssertor ASSERTOR =
            new RowDataHarnessAssertor(LOGICAL_TYPES);

    private static final GeneratedRecordEqualiser EQUALISER = new MyGeneratedRecordEqualiser();

    private static final GeneratedHashFunction HASH_FUNCTION = new MyGeneratedHashFunction();

    private static final GeneratedRecordEqualiser UPSERT_KEY_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestUpsertKeyEqualiser();
                }
            };

    private static final GeneratedHashFunction UPSERT_KEY_HASH_FUNCTION =
            new GeneratedHashFunction("", "", new Object[0], new Configuration()) {

                @Override
                public HashFunction newInstance(ClassLoader classLoader) {
                    return new TestUpsertKeyEqualiser();
                }
            };
}
