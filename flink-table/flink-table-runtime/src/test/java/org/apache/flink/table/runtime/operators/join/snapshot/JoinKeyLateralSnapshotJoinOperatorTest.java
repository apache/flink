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

import java.util.stream.Stream;

import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Harness tests for {@link LateralSnapshotJoinOperator} with the {@link JoinKeyBuildStateView}
 * layout, used when the build upsert key is the join key.
 */
class JoinKeyLateralSnapshotJoinOperatorTest {

    /** Probe row schema: (id BIGINT, jk VARCHAR, val VARCHAR); join key is jk at field 1. */
    private static final InternalTypeInfo<RowData> PROBE_TYPE =
            InternalTypeInfo.ofFields(
                    new BigIntType(), VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

    /**
     * Build row schema: (jk VARCHAR, val VARCHAR, rt BIGINT). The join key and upsert key is jk at
     * field 0; row-time is rt at field 2.
     */
    private static final InternalTypeInfo<RowData> BUILD_TYPE =
            InternalTypeInfo.ofFields(
                    VarCharType.STRING_TYPE, VarCharType.STRING_TYPE, new BigIntType());

    /** Joined output schema: probe ++ build = (id, pJk, pVal, bJk, bVal, bRt). */
    private static final LogicalType[] OUTPUT_TYPES = {
        new BigIntType(),
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        new BigIntType()
    };

    private static final int PROBE_JK_IDX = 1;
    private static final int BUILD_JK_IDX = 0;
    private static final int BUILD_RT_IDX = 2;

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
            "public class LateralSnapshotJoinKeyConditionStub extends "
                    + "org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "    public LateralSnapshotJoinKeyConditionStub(Object[] reference) {}\n"
                    + "    @Override public boolean apply("
                    + "        org.apache.flink.table.data.RowData in1,"
                    + "        org.apache.flink.table.data.RowData in2) { return true; }\n"
                    + "}\n";

    private static LateralSnapshotJoinOperator newOperator() {
        return new LateralSnapshotJoinOperator(
                false,
                PROBE_TYPE,
                BUILD_TYPE,
                BUILD_RT_IDX,
                new GeneratedJoinCondition(
                        "LateralSnapshotJoinKeyConditionStub",
                        ALWAYS_TRUE_JOIN_FUNC_CODE,
                        new Object[0]),
                new boolean[] {true},
                1L,
                null,
                null,
                BuildSideSpec.joinKey());
    }

    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            newHarness(LateralSnapshotJoinOperator op) throws Exception {
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                op, PROBE_KEY_SELECTOR, BUILD_KEY_SELECTOR, KEY_TYPE);
    }

    @ParameterizedTest(name = "flipFirst={0}")
    @ValueSource(booleans = {true, false})
    void upsertOverwritesRowInArrivalOrder(boolean flipFirst) throws Exception {
        LateralSnapshotJoinOperator op = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            if (flipFirst) {
                h.processWatermark2(new Watermark(100L));
            }
            h.processElement2(insertRecord("a", "v1", 10L));
            // The +U has a smaller row-time but arrives later, so it wins.
            h.processElement2(updateAfterRecord("a", "v2", 5L));
            if (!flipFirst) {
                h.processWatermark2(new Watermark(100L));
            }
            h.processElement1(insertRecord(10L, "a", "p"));

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "v2", 5L));
        }
    }

    @Test
    void keysAreIndependent() throws Exception {
        LateralSnapshotJoinOperator op = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            h.processElement2(insertRecord("a", "va", 1L));
            h.processElement2(insertRecord("b", "vb", 1L));

            h.processElement1(insertRecord(10L, "a", "p"));
            h.processElement1(insertRecord(11L, "b", "q"));

            JOINED_ASSERTOR.shouldEmitAll(
                    h, row(10L, "a", "p", "a", "va", 1L), row(11L, "b", "q", "b", "vb", 1L));
        }
    }

    @Test
    void partialDeleteClearsRow() throws Exception {
        LateralSnapshotJoinOperator op = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            h.processElement2(insertRecord("a", "v1", 1L));

            // Partial delete carries only the key; payload and row-time are null.
            h.processElement2(deleteRecord("a", null, null));
            h.processElement1(insertRecord(10L, "a", "p"));

            assertThat(h.extractOutputStreamRecords()).isEmpty();
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isZero();
        }
    }

    @Test
    void deleteOfAbsentKeyCountsUnmatchedRetraction() throws Exception {
        LateralSnapshotJoinOperator op = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN with an empty build table

            h.processElement2(deleteRecord("a", null, null));

            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(1L);
        }
    }

    @Test
    void updateBeforeIsIgnored() throws Exception {
        LateralSnapshotJoinOperator op = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            h.processElement2(insertRecord("a", "v1", 1L));

            h.processElement2(updateBeforeRecord("a", "v1", 1L));
            h.processElement1(insertRecord(10L, "a", "p"));

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "v1", 1L));
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isZero();
        }
    }

    @ParameterizedTest(name = "backend={0}")
    @MethodSource("stateBackends")
    void restorePreservesBuildRow(StateBackend backend) throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator();
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setStateBackend(backend);
            h.open();
            h.processWatermark2(new Watermark(100L)); // flip to JOIN
            h.processElement2(insertRecord("a", "v1", 1L));
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator();
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setStateBackend(backend);
            h.initializeState(state);
            h.open();
            h.processElement1(insertRecord(10L, "a", "p"));
            h.getOutput().removeIf(o -> !(o instanceof StreamRecord));

            JOINED_ASSERTOR.shouldEmitAll(h, row(10L, "a", "p", "a", "v1", 1L));
        }
    }

    private static Stream<Named<StateBackend>> stateBackends() {
        return Stream.of(
                Named.of("heap", new HashMapStateBackend()),
                Named.of("rocksdb", new EmbeddedRocksDBStateBackend()));
    }

    // ----------------------------------------------------------------- helpers

    private static GenericRowData row(
            Long id, String pJk, String pVal, String bJk, String bVal, Long bRt) {
        GenericRowData r = new GenericRowData(6);
        r.setField(0, id);
        r.setField(1, StringData.fromString(pJk));
        r.setField(2, StringData.fromString(pVal));
        r.setField(3, StringData.fromString(bJk));
        r.setField(4, StringData.fromString(bVal));
        r.setField(5, bRt);
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
}
