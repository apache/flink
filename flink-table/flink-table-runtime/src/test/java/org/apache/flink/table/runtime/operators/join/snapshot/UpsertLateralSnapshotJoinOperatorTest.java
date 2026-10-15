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

package org.apache.flink.table.runtime.operators.join.snapshot;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator.Phase;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness tests for {@link UpsertLateralSnapshotJoinOperator}, the build-side layout keyed by the
 * build upsert key. Verifies upsert overwrite, partial delete by key, coexistence of multiple
 * primary keys within a join key, and left-outer behaviour.
 */
class UpsertLateralSnapshotJoinOperatorTest {

    /** Probe row schema: (id BIGINT, jk VARCHAR, val VARCHAR); join key is jk at field 1. */
    private static final InternalTypeInfo<RowData> PROBE_TYPE =
            InternalTypeInfo.ofFields(
                    new BigIntType(), VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

    /**
     * Build row schema: (jk VARCHAR, pk VARCHAR, val VARCHAR, rt BIGINT). Join key is jk at field
     * 0; upsert key is (jk, pk) at fields {0, 1}; row-time is rt at field 3.
     */
    private static final InternalTypeInfo<RowData> BUILD_TYPE =
            InternalTypeInfo.ofFields(
                    VarCharType.STRING_TYPE,
                    VarCharType.STRING_TYPE,
                    VarCharType.STRING_TYPE,
                    new BigIntType());

    /** Joined output schema: probe ++ build = (id, pJk, pVal, bJk, bPk, bVal, bRt). */
    private static final LogicalType[] OUTPUT_TYPES = {
        new BigIntType(),
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        new BigIntType()
    };

    private static final int PROBE_JK_IDX = 1;
    private static final int BUILD_JK_IDX = 0;
    private static final int BUILD_RT_IDX = 3;
    private static final int[] BUILD_UPSERT_KEY_IDX = {0, 1};

    private static final InternalTypeInfo<RowData> KEY_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE);

    private static final KeySelector<RowData, RowData> PROBE_KEY_SELECTOR =
            nullSafeStringKeySelector(PROBE_JK_IDX);
    private static final KeySelector<RowData, RowData> BUILD_KEY_SELECTOR =
            nullSafeStringKeySelector(BUILD_JK_IDX);

    private static final RowDataHarnessAssertor JOINED_ASSERTOR =
            new RowDataHarnessAssertor(OUTPUT_TYPES);

    /** Trivial join condition that always matches (equality is enforced by partitioning on jk). */
    private static final String ALWAYS_TRUE_JOIN_FUNC_CODE =
            "public class LateralSnapshotUpsertJoinConditionStub extends "
                    + "org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "    public LateralSnapshotUpsertJoinConditionStub(Object[] reference) {}\n"
                    + "    @Override public boolean apply("
                    + "        org.apache.flink.table.data.RowData in1,"
                    + "        org.apache.flink.table.data.RowData in2) { return true; }\n"
                    + "}\n";

    private static GeneratedJoinCondition newTrueCondition() {
        return new GeneratedJoinCondition(
                "LateralSnapshotUpsertJoinConditionStub",
                ALWAYS_TRUE_JOIN_FUNC_CODE,
                new Object[0]);
    }

    private static UpsertLateralSnapshotJoinOperator newOperator(
            boolean isLeftOuterJoin, Long loadCompletedTime) {
        return new UpsertLateralSnapshotJoinOperator(
                isLeftOuterJoin,
                PROBE_TYPE,
                BUILD_TYPE,
                BUILD_RT_IDX,
                newTrueCondition(),
                new boolean[] {true},
                loadCompletedTime,
                null,
                null,
                BUILD_UPSERT_KEY_IDX);
    }

    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            newHarness(UpsertLateralSnapshotJoinOperator op) throws Exception {
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                op, PROBE_KEY_SELECTOR, BUILD_KEY_SELECTOR, KEY_TYPE);
    }

    @Test
    void multiplePrimaryKeysCoexistWithinJoinKey() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processElement2(insertRecord("a", "2", "v2", 1L));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            assertThat(op.getPhase()).isEqualTo(Phase.JOIN);

            h.processElement1(insertRecord(10L, "a", "p")); // applies builds and joins both
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(10L, "a", "p", "a", "1", "v1", 1L),
                    row(10L, "a", "p", "a", "2", "v2", 1L));
        }
    }

    @Test
    void upsertOverwritesRowWithSamePrimaryKey() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN

            // +U for the same upsert key (a, 1) overwrites the value.
            h.processElement2(updateAfterRecord("a", "1", "v2", 2L));
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p")); // applies +U and joins once

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "1", "v2", 2L));
        }
    }

    @Test
    void partialDeleteRemovesRowByPrimaryKey() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processElement2(insertRecord("a", "2", "v2", 1L));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN

            // Partial delete carries only the upsert key (a, 1) and the row-time; payload is null.
            h.processElement2(deleteRecord("a", "1", null, 2L));
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p")); // applies delete and joins remainder

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "2", "v2", 1L));
        }
    }

    @Test
    void tombstoneWithoutRowtimeAppliesAfterBufferedChangesInLoad() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processElement2(insertRecord("a", "2", "v2", 5L));
            // The row-time is not part of the key, so a real tombstone carries none.
            h.processElement2(deleteRecord("a", "1", null, null));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN

            h.processElement1(insertRecord(10L, "a", "p"));
            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "2", "v2", 5L));
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isZero();
        }
    }

    @Test
    void tombstoneWithoutRowtimeAppliesAfterBufferedChangesInJoin() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN

            h.processElement2(insertRecord("a", "1", "v1", 150L));
            h.processElement2(deleteRecord("a", "1", null, null));
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p"));

            assertThat(h.extractOutputStreamRecords()).isEmpty();
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isZero();
        }
    }

    @Test
    void partialDeleteOfAbsentPrimaryKeyCountsUnmatchedRetraction() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN with an empty build table

            h.processElement2(deleteRecord("a", "9", null, 2L)); // nothing to remove
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p")); // applies the (no-op) delete

            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(1L);
            assertThat(h.extractOutputStreamRecords()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noMatchHonorsJoinType(boolean leftOuter) throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(leftOuter, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN with an empty build table
            h.processElement1(insertRecord(10L, "a", "p"));

            if (leftOuter) {
                JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", null, null, null, null));
            } else {
                assertThat(h.extractOutputStreamRecords()).isEmpty();
            }
        }
    }

    @Test
    void updateBeforeThenAfterReplacesRowByKey() throws Exception {
        UpsertLateralSnapshotJoinOperator op = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN

            // A -U/+U pair for the same upsert key (a, 1): remove then re-insert with the new
            // value.
            h.processElement2(updateBeforeRecord("a", "1", "v1", 2L));
            h.processElement2(updateAfterRecord("a", "1", "v2", 2L));
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p"));

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "1", "v2", 2L));
        }
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void restorePreservesUpsertBuildTable(StateBackend backend) throws Exception {
        UpsertLateralSnapshotJoinOperator op1 = newOperator(false, 1L);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setStateBackend(backend);
            h.open();
            h.processElement2(insertRecord("a", "1", "v1", 1L));
            h.processElement2(insertRecord("a", "2", "v2", 1L));
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            h.processElement1(insertRecord(1L, "a", "warmup")); // materialize the build table
            assertThat(buildTableForKey(h, op1, "a"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("1", "v1", "2", "v2"));
            state = h.snapshot(0L, 0L);
        }

        UpsertLateralSnapshotJoinOperator op2 = newOperator(false, 1L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setStateBackend(backend);
            h.initializeState(state);
            h.open();
            // The upsert-keyed build table survived the restore.
            assertThat(buildTableForKey(h, op2, "a"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("1", "v1", "2", "v2"));

            // A partial delete by key still applies after restore.
            h.processElement2(deleteRecord("a", "1", null, 2L));
            h.processWatermark2(new Watermark(200L));
            h.processElement1(insertRecord(10L, "a", "p"));
            stripOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "2", "v2", 1L));
        }
    }

    private static Stream<Named<StateBackend>> stateBackends() {
        return Stream.of(
                Named.of("heap", new HashMapStateBackend()),
                Named.of("rocksdb", new EmbeddedRocksDBStateBackend()));
    }

    // ----------------------------------------------------------------- helpers

    private static GenericRowData row(
            Long id, String pJk, String pVal, String bJk, String bPk, String bVal, Long bRt) {
        GenericRowData r = new GenericRowData(7);
        r.setField(0, id);
        r.setField(1, StringData.fromString(pJk));
        r.setField(2, StringData.fromString(pVal));
        r.setField(3, bJk == null ? null : StringData.fromString(bJk));
        r.setField(4, bPk == null ? null : StringData.fromString(bPk));
        r.setField(5, bVal == null ? null : StringData.fromString(bVal));
        r.setField(6, bRt);
        return r;
    }

    private static KeySelector<RowData, RowData> nullSafeStringKeySelector(final int keyIdx) {
        return value -> {
            BinaryRowData ret = new BinaryRowData(1);
            BinaryRowWriter writer = new BinaryRowWriter(ret);
            if (value.isNullAt(keyIdx)) {
                writer.setNullAt(0);
            } else {
                writer.writeString(0, value.getString(keyIdx));
            }
            writer.complete();
            return ret;
        };
    }

    private static BinaryRowData stringKey(String key) {
        BinaryRowData ret = new BinaryRowData(1);
        BinaryRowWriter writer = new BinaryRowWriter(ret);
        writer.writeString(0, StringData.fromString(key));
        writer.complete();
        return ret;
    }

    /** Returns the committed build rows for a join key as a map pk → val. */
    private static Map<String, String> buildTableForKey(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            UpsertLateralSnapshotJoinOperator op,
            String joinKey)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(joinKey));
        Map<String, String> result = new LinkedHashMap<>();
        for (RowData buildRow : op.getBuildTableByKey().values()) {
            result.put(buildRow.getString(1).toString(), buildRow.getString(2).toString());
        }
        return result;
    }

    private static void stripOutput(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h) {
        h.getOutput().removeIf(o -> !(o instanceof StreamRecord));
    }
}
