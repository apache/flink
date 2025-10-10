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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFinalizer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.generated.RecordEqualiser;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.api.java.tuple.Tuple2.of;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.rowOfKind;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Test for {@link SinkUpsertMaterializer}. */
@RunWith(Parameterized.class)
public class SinkUpsertMaterializerTest {

    static final int UPSERT_KEY = 0;

    @Parameter public SinkUpsertMaterializerStateBackend stateBackend;

    @Parameterized.Parameters(name = "stateBackend={0}")
    public static Object[][] generateTestParameters() {
        List<Object[]> result = new ArrayList<>();
        for (SinkUpsertMaterializerStateBackend backend :
                SinkUpsertMaterializerStateBackend.values()) {
            result.add(new Object[] {backend});
        }
        return result.toArray(new Object[0][]);
    }

    static final StateTtlConfig TTL_CONFIG = StateConfigUtil.createTtlConfig(1000);
    static final LogicalType[] LOGICAL_TYPES =
            new LogicalType[] {new BigIntType(), new IntType(), new VarCharType()};
    static final RowDataHarnessAssertor ASSERTOR = new RowDataHarnessAssertor(LOGICAL_TYPES);

    static final GeneratedRecordEqualiser EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestRecordEqualiser();
                }
            };

    static final GeneratedRecordEqualiser UPSERT_KEY_EQUALISER =
            new GeneratedRecordEqualiser("", "", new Object[0]) {

                @Override
                public RecordEqualiser newInstance(ClassLoader classLoader) {
                    return new TestUpsertKeyEqualiser();
                }
            };

    @Test
    public void testNoUpsertKeyFlow() throws Exception {
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                createHarness(createOperatorWithoutUpsertKey());

        testHarness.open();

        testHarness.setStateTtlProcessingTime(1);

        testHarness.processElement(insertRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        testHarness.processElement(insertRecord(2L, 1, "a2"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2"));

        testHarness.processElement(insertRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        testHarness.processElement(deleteRecord(2L, 1, "a2"));
        ASSERTOR.shouldEmitNothing(testHarness);

        testHarness.processElement(deleteRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a1"));

        testHarness.processElement(deleteRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, 1L, 1, "a1"));

        testHarness.processElement(insertRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        testHarness.setStateTtlProcessingTime(1002);

        testHarness.processElement(deleteRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmitNothing(testHarness);

        testHarness.close();
    }

    @Test
    public void testInputHasUpsertKeyWithNonDeterministicColumn() throws Exception {
        OneInputStreamOperator<RowData, RowData> materializer =
                createOperator(LOGICAL_TYPES, UPSERT_KEY);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                createHarness(materializer);

        testHarness.open();

        testHarness.setStateTtlProcessingTime(1);

        testHarness.processElement(insertRecord(1L, 1, "a1"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 1L, 1, "a1"));

        testHarness.processElement(updateAfterRecord(1L, 1, "a11"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a11"));

        testHarness.processElement(insertRecord(3L, 1, "a3"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));

        testHarness.processElement(deleteRecord(1L, 1, "a111"));
        ASSERTOR.shouldEmitNothing(testHarness);

        testHarness.processElement(deleteRecord(3L, 1, "a33"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.DELETE, 3L, 1, "a33"));

        testHarness.processElement(insertRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmit(testHarness, rowOfKind(RowKind.INSERT, 4L, 1, "a4"));

        testHarness.setStateTtlProcessingTime(1002);

        testHarness.processElement(deleteRecord(4L, 1, "a4"));
        ASSERTOR.shouldEmitNothing(testHarness);

        testHarness.close();
    }

    @Test
    public void testRetractionWithoutUpsertKey() throws Exception {
        testRetractions((int[]) null);
    }

    @Test
    public void testRetractionWithUpsertKey() throws Exception {
        testRetractions(UPSERT_KEY);
    }

    public void testRetractions(int... upsertKey) throws Exception {
        testThreeElementProcessing(
                "retract first - should emit nothing until empty - then delete",
                upsertKey,
                of(deleteRecord(1L, 1, "a1"), null),
                of(deleteRecord(2L, 1, "a2"), null),
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.DELETE, 3L, 1, "a3")));
        testThreeElementProcessing(
                "retract middle - should emit nothing until empty - then delete",
                upsertKey,
                of(deleteRecord(2L, 1, "a2"), null),
                of(deleteRecord(1L, 1, "a1"), null),
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.DELETE, 3L, 1, "a3")));
        testThreeElementProcessing(
                "retract last - should emit penultimate until empty - then delete",
                upsertKey,
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2")),
                of(deleteRecord(2L, 1, "a2"), rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a1")),
                of(deleteRecord(1L, 1, "a1"), rowOfKind(RowKind.DELETE, 1L, 1, "a1")));
        testThreeElementProcessing(
                "retract in arbitrary order: 1,3,2",
                upsertKey,
                of(deleteRecord(1L, 1, "a1"), null),
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2")),
                of(deleteRecord(2L, 1, "a2"), rowOfKind(RowKind.DELETE, 2L, 1, "a2")));
        testThreeElementProcessing(
                "retract in arbitrary order: 2,3,1",
                upsertKey,
                of(deleteRecord(2L, 1, "a2"), null),
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.UPDATE_AFTER, 1L, 1, "a1")),
                of(deleteRecord(1L, 1, "a1"), rowOfKind(RowKind.DELETE, 1L, 1, "a1")));
        testThreeElementProcessing(
                "retract in arbitrary order: 3,1,2",
                upsertKey,
                of(deleteRecord(3L, 1, "a3"), rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2")),
                of(deleteRecord(1L, 1, "a1"), null),
                of(deleteRecord(2L, 1, "a2"), rowOfKind(RowKind.DELETE, 2L, 1, "a2")));
    }

    // boilerplate for common test case of processing starting with three elements
    @SafeVarargs
    private void testThreeElementProcessing(
            String description,
            int[] upsertKey,
            Tuple2<StreamRecord<RowData>, RowData>... inputAndOutput)
            throws Exception {
        @SuppressWarnings("rawtypes")
        Tuple2[] merged = new Tuple2[inputAndOutput.length + 3];
        merged[0] = of(insertRecord(1L, 1, "a1"), rowOfKind(RowKind.INSERT, 1L, 1, "a1"));
        merged[1] = of(insertRecord(2L, 1, "a2"), rowOfKind(RowKind.UPDATE_AFTER, 2L, 1, "a2"));
        merged[2] = of(insertRecord(3L, 1, "a3"), rowOfKind(RowKind.UPDATE_AFTER, 3L, 1, "a3"));
        System.arraycopy(inputAndOutput, 0, merged, 3, inputAndOutput.length);
        testElementProcessing(description, upsertKey, merged);
    }

    @SafeVarargs
    private void testElementProcessing(
            String description,
            int[] upsertKey,
            Tuple2<StreamRecord<RowData>, RowData>... inputAndOutput)
            throws Exception {
        OneInputStreamOperator<RowData, RowData> materializer =
                createOperator(LOGICAL_TYPES, upsertKey);
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                createHarness(materializer);

        testHarness.open();

        for (Tuple2<StreamRecord<RowData>, RowData> el0 : inputAndOutput) {
            testHarness.processElement(el0.f0);
            if (el0.f1 == null) {
                ASSERTOR.shouldEmitNothing(testHarness);
            } else {
                ASSERTOR.shouldEmit(testHarness, description, el0.f1);
            }
        }

        testHarness.close();
    }

    private static class TestRecordEqualiser implements RecordEqualiser {
        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(0) == row2.getLong(0)
                    && row1.getInt(1) == row2.getInt(1)
                    && row1.getString(2).equals(row2.getString(2));
        }
    }

    private static class TestUpsertKeyEqualiser implements RecordEqualiser {

        @Override
        public boolean equals(RowData row1, RowData row2) {
            return row1.getRowKind() == row2.getRowKind()
                    && row1.getLong(UPSERT_KEY) == row2.getLong(UPSERT_KEY);
        }
    }

    private OneInputStreamOperator<RowData, RowData> createOperatorWithoutUpsertKey() {
        return createOperator(LOGICAL_TYPES, (int[]) null);
    }

    private OneInputStreamOperator<RowData, RowData> createOperator(
            LogicalType[] types, int... upsertKey) {
        return SinkUpsertMaterializer.create(
                TTL_CONFIG, RowType.of(types), EQUALISER, UPSERT_KEY_EQUALISER, upsertKey);
    }

    private KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            OneInputStreamOperator<RowData, RowData> m2) throws Exception {
        return createHarness(m2, stateBackend, LOGICAL_TYPES);
    }

    static KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> createHarness(
            OneInputStreamOperator<RowData, RowData> materializer,
            SinkUpsertMaterializerStateBackend backend,
            LogicalType[] types)
            throws Exception {
        KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        materializer,
                        HandwrittenSelectorUtil.getRowDataSelector(new int[] {1}, types),
                        HandwrittenSelectorUtil.getRowDataSelector(new int[] {1}, types)
                                .getProducedType());
        testHarness.setStateBackend(backend.create(true));
        return testHarness;
    }

    @Test
    public void testEmptyUpsertKey() throws Exception {
        testRecovery(createOperator(LOGICAL_TYPES), createOperatorWithoutUpsertKey());
        testRecovery(createOperatorWithoutUpsertKey(), createOperator(LOGICAL_TYPES));
    }

    private void testRecovery(
            OneInputStreamOperator<RowData, RowData> from,
            OneInputStreamOperator<RowData, RowData> to)
            throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                createHarness(from)) {
            testHarness.open();
            snapshot = testHarness.snapshot(1L, 1L);
        }
        try (KeyedOneInputStreamOperatorTestHarness<RowData, RowData, RowData> testHarness =
                createHarness(to)) {
            testHarness.initializeState(snapshot);
            testHarness.open();
        }
    }

    @Test
    public void testStateIsBounded() throws Exception {
        int dop = 2;
        int numIterations = 10;
        OperatorSnapshotFinalizer[] snapshots = new OperatorSnapshotFinalizer[dop];
        long[] prevStateSizes = new long[dop];
        for (int i = 0; i < numIterations; i++) {
            for (int subtask = 0; subtask < dop; subtask++) {
                snapshots[subtask] = initAndSnapshot(snapshots[subtask], i);
                long currentStateSize =
                        snapshots[subtask]
                                .getJobManagerOwnedState()
                                .getManagedOperatorState()
                                .stream()
                                .mapToLong(StateObject::getStateSize)
                                .sum();
                if (i > 0) {
                    assertEquals(prevStateSizes[subtask], currentStateSize);
                }
                prevStateSizes[subtask] = currentStateSize;
            }
            List<OperatorStateHandle> union =
                    Arrays.stream(snapshots)
                            .flatMap(
                                    s ->
                                            s
                                                    .getJobManagerOwnedState()
                                                    .getManagedOperatorState()
                                                    .stream())
                            .collect(Collectors.toList());
            for (int j = 0; j < dop; j++) {
                snapshots[j] =
                        new OperatorSnapshotFinalizer(
                                snapshots[j].getJobManagerOwnedState().toBuilder()
                                        .setManagedOperatorState(new StateObjectCollection<>(union))
                                        .build(),
                                snapshots[j].getTaskLocalState());
            }
        }
    }

    private OperatorSnapshotFinalizer initAndSnapshot(
            OperatorSnapshotFinalizer from, int newCheckpointID) throws Exception {
        try (OneInputStreamOperatorTestHarness<RowData, RowData> harness =
                createHarness(
                        createOperator(LOGICAL_TYPES, UPSERT_KEY), stateBackend, LOGICAL_TYPES)) {
            if (from != null) {
                harness.initializeState(from.getJobManagerOwnedState());
            }
            harness.open();
            return harness.snapshotWithLocalState(newCheckpointID, newCheckpointID);
        }
    }
}
