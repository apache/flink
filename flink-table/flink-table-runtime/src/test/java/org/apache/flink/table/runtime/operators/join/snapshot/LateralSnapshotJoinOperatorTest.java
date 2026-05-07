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
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
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
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator.BUILD_CHANGE_BUFFER_STATE_NAME;
import static org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator.BUILD_TABLE_STATE_NAME;
import static org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator.PROBE_BUFFER_STATE_NAME;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.deleteRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.insertRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateAfterRecord;
import static org.apache.flink.table.runtime.util.StreamRecordUtils.updateBeforeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Harness tests for {@link LateralSnapshotJoinOperator}. */
class LateralSnapshotJoinOperatorTest {

    // ----------------------------------------------------------------- Schema

    /** Probe row schema: (id BIGINT, key VARCHAR, val VARCHAR). */
    private static final InternalTypeInfo<RowData> PROBE_TYPE =
            InternalTypeInfo.ofFields(
                    new BigIntType(), VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

    /** Build row schema: (key VARCHAR, val VARCHAR, rt BIGINT). */
    private static final InternalTypeInfo<RowData> BUILD_TYPE =
            InternalTypeInfo.ofFields(
                    VarCharType.STRING_TYPE, VarCharType.STRING_TYPE, new BigIntType());

    /** Joined output schema: probe ++ build = (id, pKey, pVal, bKey, bVal, bRt). */
    private static final LogicalType[] OUTPUT_TYPES = {
        new BigIntType(),
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        VarCharType.STRING_TYPE,
        new BigIntType()
    };

    /** Probe key column index (key VARCHAR is at field 1). */
    private static final int PROBE_KEY_IDX = 1;

    /** Build key column index (key VARCHAR is at field 0). */
    private static final int BUILD_KEY_IDX = 0;

    /** Build row-time column index (rt BIGINT is at field 2). */
    private static final int BUILD_RT_IDX = 2;

    private static final InternalTypeInfo<RowData> KEY_TYPE =
            InternalTypeInfo.ofFields(VarCharType.STRING_TYPE);

    private static final KeySelector<RowData, RowData> PROBE_KEY_SELECTOR =
            nullSafeStringKeySelector(PROBE_KEY_IDX);
    private static final KeySelector<RowData, RowData> BUILD_KEY_SELECTOR =
            nullSafeStringKeySelector(BUILD_KEY_IDX);

    private static final RowDataHarnessAssertor JOINED_ASSERTOR =
            new RowDataHarnessAssertor(OUTPUT_TYPES);

    // ----------------------------------------------------------------- Join conditions

    /** Trivial join condition that always matches (equality is enforced by partitioning). */
    private static final String ALWAYS_TRUE_JOIN_FUNC_CODE =
            "public class LateralSnapshotJoinConditionStub extends "
                    + "org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "    public LateralSnapshotJoinConditionStub(Object[] reference) {}\n"
                    + "    @Override public boolean apply("
                    + "        org.apache.flink.table.data.RowData in1,"
                    + "        org.apache.flink.table.data.RowData in2) { return true; }\n"
                    + "}\n";

    /**
     * Join condition that only matches when the probe value (field 2) equals {@code "match"}. Used
     * to verify that the codegen'd condition is actually invoked at join time.
     */
    private static final String MATCH_VAL_JOIN_FUNC_CODE =
            "public class LateralSnapshotJoinConditionMatchVal extends "
                    + "org.apache.flink.api.common.functions.AbstractRichFunction "
                    + "implements org.apache.flink.table.runtime.generated.JoinCondition {\n"
                    + "    public LateralSnapshotJoinConditionMatchVal(Object[] reference) {}\n"
                    + "    @Override public boolean apply("
                    + "        org.apache.flink.table.data.RowData in1,"
                    + "        org.apache.flink.table.data.RowData in2) {\n"
                    + "        if (in1.isNullAt(2)) { return false; }\n"
                    + "        return \"match\".equals(in1.getString(2).toString());\n"
                    + "    }\n"
                    + "}\n";

    private static GeneratedJoinCondition newTrueCondition() {
        return new GeneratedJoinCondition(
                "LateralSnapshotJoinConditionStub", ALWAYS_TRUE_JOIN_FUNC_CODE, new Object[0]);
    }

    private static GeneratedJoinCondition newMatchValCondition() {
        return new GeneratedJoinCondition(
                "LateralSnapshotJoinConditionMatchVal", MATCH_VAL_JOIN_FUNC_CODE, new Object[0]);
    }

    // ----------------------------------------------------------------- Operator / harness
    // factories

    private static LateralSnapshotJoinOperator newOperator(
            boolean isLeftOuterJoin,
            GeneratedJoinCondition joinCondition,
            boolean[] filterNullKeys,
            Long loadCompletedTime,
            Long loadCompletedIdleTimeoutMs,
            Long stateTtlMs) {

        return new LateralSnapshotJoinOperator(
                isLeftOuterJoin,
                PROBE_TYPE,
                BUILD_TYPE,
                BUILD_RT_IDX,
                joinCondition,
                filterNullKeys,
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }

    private static LateralSnapshotJoinOperator newOperator(
            boolean isLeftOuterJoin,
            Long loadCompletedTime,
            Long loadCompletedIdleTimeoutMs,
            Long stateTtlMs) {

        return newOperator(
                isLeftOuterJoin,
                newTrueCondition(),
                new boolean[] {true},
                loadCompletedTime,
                loadCompletedIdleTimeoutMs,
                stateTtlMs);
    }

    private static KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData>
            newHarness(LateralSnapshotJoinOperator op) throws Exception {
        return new KeyedTwoInputStreamOperatorTestHarness<>(
                op, PROBE_KEY_SELECTOR, BUILD_KEY_SELECTOR, KEY_TYPE);
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

    // ----------------------------------------------------------------- LOAD phase

    @Test
    void loadPhaseBuildSideChangeProcessing() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // During LOAD, build-side changes are buffered and later applied in event-time order.
            // -D for a never-inserted (key, value) pair is defensively ignored.
            addBuildChange(h, deleteRecord("k1", "ghost", 5L));
            // Two identical records (same key/val/row-time) → count(k1, v1, 20) = 2.
            addBuildChange(h, insertRecord("k1", "v1", 20L));
            addBuildChange(h, insertRecord("k1", "v1", 20L));
            // Earlier row-time than v1, but arrives later.
            addBuildChange(h, insertRecord("k1", "v2", 10L));

            // Still LOAD: changes are buffered, nothing applied or emitted yet.
            assertPhase(op, Phase.LOAD);
            assertThat(h.getOutput()).isEmpty();
            assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(4);
            assertThat(buildTableKeys(h)).isEmpty();

            // Advance the build watermark (still below the flip point) and access k1 again. The
            // access drains the buffered batch in event-time order before buffering the new change.
            addBuildWm(h, 50L);
            addBuildChange(h, insertRecord("k1", "v3", 30L));
            // TODO: also add updateBefore and updateAfter changes, to ensure that these are handled
            // correctly

            assertPhase(op, Phase.LOAD);
            // Applied in row-time order: -D(ghost)@5 (ignored), +I(v2)@10, +I(v1)@20 ×2.
            assertThat(buildTableKeys(h)).containsExactly("k1");
            assertThat(buildTableForKey(h, op, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 2L, "v2", 1L));
            // The triggering v3 change is now buffered, awaiting the next drain.
            assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(1);
        }
    }

    @Test
    void loadPhaseProbeSideInputProcessing() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();

            addBuildChange(h, insertRecord("k1", "build1", 1L));
            addBuildWm(h, 20L);
            addProbeRecord(h, 1L, "k1", "probe-load-1");
            addProbeRecord(h, 2L, "k1", "probe-load-2");
            addProbeWm(h, 50L);

            assertPhase(op, Phase.LOAD);
            // No output (records buffered, watermarks held back).
            assertThat(h.getOutput()).isEmpty();

            assertThat(op.getCurrentProbeSideWm()).isEqualTo(50L);
            assertThat(probeBufferKeys(h)).containsExactly("k1");
            assertThat(probeBufferForKey(h, op, "k1")).hasSize(2);
            // Build-side change was applied to the build table.
            assertThat(bufferedChangesForKey(h, op, "k1")).isEmpty();
            assertThat(buildTableForKey(h, op, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("build1", 1L));
        }
    }

    // ----------------------------------------------------------------- Flip / transition

    @ParameterizedTest(name = "leftOuter={0}, wmFlip={1}")
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void flipDrainsProbeBufferAndJoins(boolean leftOuter, boolean wmFlip) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(leftOuter, 100L, 200L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // LOAD: build state and buffered probes (one without a matching build row).
            // k1 is a multi-set: count(v1)=2, count(v2)=1; k2 is a single row.
            addBuildChange(h, insertRecord("k1", "build-k1-v1", 1L));
            addBuildChange(h, insertRecord("k1", "build-k1-v1", 1L));
            addBuildChange(h, insertRecord("k1", "build-k1-v2", 1L));
            addBuildChange(h, insertRecord("k2", "build-k2", 1L));
            // LOAD: add probe-side records
            addProbeRecord(h, 1L, "k1", "probe-1");
            addProbeRecord(h, 2L, "k2", "probe-2");
            addProbeRecord(h, 3L, "k2", "probe-3");
            addProbeRecord(h, 4L, "k3", "probe-no-match");
            addProbeWm(h, 80L);
            assertPhase(op, Phase.LOAD);

            // assert that probe-side buffer is filled
            assertThat(probeBufferForKey(h, op, "k1")).hasSize(1);
            assertThat(probeBufferForKey(h, op, "k2")).hasSize(2);
            assertThat(probeBufferForKey(h, op, "k3")).hasSize(1);
            // idle-timeout timer is armed while in LOAD
            assertThat(op.isIdleFlipTimerActive()).isTrue();

            // trigger flip from LOAD to JOIN
            if (wmFlip) {
                // build WM crosses loadCompletedTime.
                addBuildWm(h, 100L);
            } else {
                // proc-time exceeds idle timeout
                h.setProcessingTime(200);
            }
            assertPhase(op, Phase.JOIN);
            // idle-timeout timer is removed on flip (canceled by a WM flip, fired by an idle flip)
            assertThat(op.isIdleFlipTimerActive()).isFalse();

            // probe k1 joins k1's multi-set (count-respecting: 2x v1 + 1x v2 = three rows);
            // probe k2 (2 rows) joins a single k2 build row;
            // probe k3 doesn't have a matching build row. INNER: no output, LEFT OUTER: null-padded
            assertWatermarkForwardedAfterRecords(h.getOutput(), 80L);
            stripWatermarksAndStatusesFromOutput(h);
            if (leftOuter) {
                JOINED_ASSERTOR.shouldEmitAll(
                        h,
                        row(1L, "k1", "probe-1", "k1", "build-k1-v1", 1L),
                        row(1L, "k1", "probe-1", "k1", "build-k1-v1", 1L),
                        row(1L, "k1", "probe-1", "k1", "build-k1-v2", 1L),
                        row(2L, "k2", "probe-2", "k2", "build-k2", 1L),
                        row(3L, "k2", "probe-3", "k2", "build-k2", 1L),
                        row(4L, "k3", "probe-no-match", null, null, null));
            } else {
                JOINED_ASSERTOR.shouldEmitAll(
                        h,
                        row(1L, "k1", "probe-1", "k1", "build-k1-v1", 1L),
                        row(1L, "k1", "probe-1", "k1", "build-k1-v1", 1L),
                        row(1L, "k1", "probe-1", "k1", "build-k1-v2", 1L),
                        row(2L, "k2", "probe-2", "k2", "build-k2", 1L),
                        row(3L, "k2", "probe-3", "k2", "build-k2", 1L));
            }
            // Probe buffer drained on flip; build table preserved (k1 keeps multi-set counts).
            assertThat(probeBufferKeys(h)).isEmpty();
            assertThat(buildTableForKey(h, op, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(
                            Map.of("build-k1-v1", 2L, "build-k1-v2", 1L));
            assertThat(buildTableForKey(h, op, "k2"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("build-k2", 1L));
        }
    }

    @Test
    void idleTimerRearmsOnBuildWatermark() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 1000L, 100L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(10);
            h.open();
            h.setProcessingTime(60);
            // Build WM advances → re-arm.
            addBuildWm(h, 10L);
            // Original idle deadline was 10+100=110. Re-armed to 60+100=160.
            h.setProcessingTime(159);
            assertPhase(op, Phase.LOAD);
            h.setProcessingTime(160);
            assertPhase(op, Phase.JOIN);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void flipJoiningInvokesCodeGeneratedJoinCondition(boolean leftOuter) throws Exception {
        LateralSnapshotJoinOperator op =
                newOperator(
                        leftOuter, newMatchValCondition(), new boolean[] {true}, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addProbeRecord(h, 1L, "k1", "match");
            addProbeRecord(h, 2L, "k1", "skip");
            addProbeWm(h, 120L);
            addBuildWm(h, 100L);

            assertWatermarkForwardedAfterRecords(h.getOutput(), 120L);
            stripWatermarksAndStatusesFromOutput(h);
            if (leftOuter) {
                JOINED_ASSERTOR.shouldEmitAll(
                        h,
                        row(1L, "k1", "match", "k1", "v1", 1L),
                        row(2L, "k1", "skip", null, null, null));
            } else {
                JOINED_ASSERTOR.shouldEmitAll(h, row(1L, "k1", "match", "k1", "v1", 1L));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void flipJoiningCompositeEquiKeys(boolean leftOuter) throws Exception {
        // Probe schema (kA VARCHAR, kB VARCHAR, val VARCHAR); build schema additionally carries a
        // row-time attribute (kA VARCHAR, kB VARCHAR, val VARCHAR, rt BIGINT) at index 3.
        InternalTypeInfo<RowData> probeType =
                InternalTypeInfo.ofFields(
                        VarCharType.STRING_TYPE, VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);
        InternalTypeInfo<RowData> buildType =
                InternalTypeInfo.ofFields(
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE,
                        VarCharType.STRING_TYPE,
                        new BigIntType());
        final int buildRowtimeIndex = 3;
        // Compose the composite key as a BinaryRowData with both key fields.
        KeySelector<RowData, RowData> selector =
                value -> {
                    BinaryRowData ret = new BinaryRowData(2);
                    BinaryRowWriter writer = new BinaryRowWriter(ret);
                    if (value.isNullAt(0)) {
                        writer.setNullAt(0);
                    } else {
                        writer.writeString(0, value.getString(0));
                    }
                    if (value.isNullAt(1)) {
                        writer.setNullAt(1);
                    } else {
                        writer.writeString(1, value.getString(1));
                    }
                    writer.complete();
                    return ret;
                };
        InternalTypeInfo<RowData> compositeKeyType =
                InternalTypeInfo.ofFields(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE);

        LateralSnapshotJoinOperator op =
                new LateralSnapshotJoinOperator(
                        leftOuter,
                        probeType,
                        buildType,
                        buildRowtimeIndex,
                        newTrueCondition(),
                        new boolean[] {true, true},
                        100L,
                        null,
                        null);

        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        op, selector, selector, compositeKeyType)) {
            h.open();
            // build rows: two identical rows for key a-1, a different row for key a-1, one row for
            // key a-2.
            addBuildChange(h, insertRecord("a", "1", "b-a-1-1", 1L));
            addBuildChange(h, insertRecord("a", "1", "b-a-1-1", 1L));
            addBuildChange(h, insertRecord("a", "1", "b-a-1-2", 1L));
            addBuildChange(h, insertRecord("a", "2", "b-a-2-1", 1L));
            // probes: matching composite, non-matching composite.
            h.processElement1(insertRecord("a", "1", "p-a-1"));
            h.processElement1(insertRecord("a", "9", "p-a-9"));
            h.processElement1(insertRecord("b", "1", "p-b-1"));
            h.processElement1(insertRecord("b", "9", "p-b-9"));
            // flip to JOIN phase
            addBuildWm(h, 100L);

            LogicalType[] outTypes = {
                VarCharType.STRING_TYPE,
                VarCharType.STRING_TYPE,
                VarCharType.STRING_TYPE,
                VarCharType.STRING_TYPE,
                VarCharType.STRING_TYPE,
                VarCharType.STRING_TYPE,
                new BigIntType()
            };
            RowDataHarnessAssertor compositeAssertor = new RowDataHarnessAssertor(outTypes);

            stripWatermarksAndStatusesFromOutput(h);
            if (leftOuter) {
                compositeAssertor.shouldEmitAll(
                        h,
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-1", 1L),
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-1", 1L),
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-2", 1L),
                        compKeyRow("a", "9", "p-a-9", null, null, null, null),
                        compKeyRow("b", "1", "p-b-1", null, null, null, null),
                        compKeyRow("b", "9", "p-b-9", null, null, null, null));
            } else {
                compositeAssertor.shouldEmitAll(
                        h,
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-1", 1L),
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-1", 1L),
                        compKeyRow("a", "1", "p-a-1", "a", "1", "b-a-1-2", 1L));
            }
        }
    }

    // ----------------------------------------------------------------- JOIN phase

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void joinPhaseImmediateInnerJoin(boolean leftOuter) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(leftOuter, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // First probe WM
            addProbeWm(h, 10L);
            assertWatermarkForwardedAfterRecords(h.getOutput(), 10L);

            // First probe — joined immediately.
            addProbeRecord(h, 1L, "k1", "probe-immediate-1");
            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(h, row(1L, "k1", "probe-immediate-1", "k1", "v1", 1L));

            // Another probe WM
            addProbeWm(h, 20L);
            assertWatermarkForwardedAfterRecords(h.getOutput(), 20L);

            // Second probe for same key — joined immediately.
            addProbeRecord(h, 2L, "k1", "probe-immediate-2");
            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(h, row(2L, "k1", "probe-immediate-2", "k1", "v1", 1L));

            // Probe for non-existent key — no output (INNER).
            addProbeRecord(h, 3L, "k2", "probe-no-match");
            stripWatermarksAndStatusesFromOutput(h);
            if (leftOuter) {
                JOINED_ASSERTOR.shouldEmitAll(h, row(3L, "k2", "probe-no-match", null, null, null));
            } else {
                assertThat(h.extractOutputStreamRecords()).isEmpty();
            }

            // one more probe WM
            addProbeWm(h, 30L);
            assertWatermarkForwardedAfterRecords(h.getOutput(), 30L);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void joinPhaseBuildSideChangeApplication(boolean appliedByBuild) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Two identical records for k1 (count 2) at row-time 1, buffered during LOAD.
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // Buffer four changes for k1
            addBuildChange(h, deleteRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v2", 102L));
            addBuildChange(h, updateBeforeRecord("k1", "v1", 1L));
            addBuildChange(h, updateAfterRecord("k1", "v3", 103L));
            // Buffer one change for k2.
            addBuildChange(h, insertRecord("k2", "v1", 101L));

            // assert number of buffered changes
            assertThat(op.getCurrentBuildSideWm()).isEqualTo(100L);
            assertThat(bufferedAtWmFor(h, op, "k1")).isEqualTo(100L);
            assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(4);
            assertThat(bufferedAtWmFor(h, op, "k2")).isEqualTo(100L);
            assertThat(bufferedChangesForKey(h, op, "k2")).hasSize(1);
            assertThat(buildTableForKey(h, op, "k1")).isEqualTo(Map.of("v1", 2L));

            // Probe record with no build WM advance - changes are not applied yet
            addProbeRecord(h, 1L, "k1", "p-1");
            JOINED_ASSERTOR.shouldEmitAll(
                    h, row(1L, "k1", "p-1", "k1", "v1", 1L), row(1L, "k1", "p-1", "k1", "v1", 1L));

            // increment build-side WM
            addBuildWm(h, 110L);

            // assert that all changes are still buffered
            assertThat(op.getCurrentBuildSideWm()).isEqualTo(110L);
            assertThat(bufferedAtWmFor(h, op, "k1")).isEqualTo(100L);
            assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(4);
            assertThat(bufferedAtWmFor(h, op, "k2")).isEqualTo(100L);
            assertThat(bufferedChangesForKey(h, op, "k2")).hasSize(1);

            // trigger application of k1 changes by build or probe-side input
            if (!appliedByBuild) {
                addBuildChange(h, insertRecord("k1", "v4", 111L));
                // assert that changes have been applied and removed from buffer
                // the triggering change is appended to the buffer
                assertThat(bufferedAtWmFor(h, op, "k1")).isEqualTo(110L);
                assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(1);
                assertThat(buildTableForKey(h, op, "k1")).isEqualTo(Map.of("v2", 1L, "v3", 1L));
                // assert empty output
                assertThat(h.getOutput()).isEmpty();
            } else {
                addProbeRecord(h, 2L, "k1", "p-2");
                // assert that changes have been applied and removed from buffer
                assertThat(bufferedAtWmFor(h, op, "k1")).isNull();
                assertThat(bufferedChangesForKey(h, op, "k1")).isEmpty();
                assertThat(buildTableForKey(h, op, "k1"))
                        .isEqualTo(Map.of("v2", 1L, "v3", 1L)); // /
                // assert join results (v2 carries row-time 2, v3 carries row-time 1)
                JOINED_ASSERTOR.shouldEmitAll(
                        h,
                        row(2L, "k1", "p-2", "k1", "v2", 102L),
                        row(2L, "k1", "p-2", "k1", "v3", 103L));
            }

            // assert that k2 change is still buffered
            assertThat(bufferedAtWmFor(h, op, "k2")).isEqualTo(100L);
            assertThat(bufferedChangesForKey(h, op, "k2")).hasSize(1);
            // apply k2 change and join
            addProbeRecord(h, 3L, "k2", "p-3");
            assertThat(bufferedAtWmFor(h, op, "k2")).isNull();
            assertThat(bufferedChangesForKey(h, op, "k2")).isEmpty();
            assertThat(buildTableForKey(h, op, "k2")).isEqualTo(Map.of("v1", 1L));
            // assert join results
            JOINED_ASSERTOR.shouldEmitAll(h, row(3L, "k2", "p-3", "k2", "v1", 101L));
        }
    }

    @Test
    void joinPhaseWmForwardingLogic() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L); // flip
            assertPhase(op, Phase.JOIN);

            // Build-side WMs after flip are not forwarded.
            addBuildWm(h, 200L);
            assertThat(extractWatermarks(h.getOutput())).isEmpty();

            // Probe-side WMs in JOIN are forwarded.
            addProbeWm(h, 150L);
            assertThat(extractWatermarks(h.getOutput())).containsExactly(new Watermark(150));
            h.getOutput().clear();

            // another build-side WM
            addBuildWm(h, 300L);
            assertThat(extractWatermarks(h.getOutput())).isEmpty();

            addProbeWm(h, 250L);
            assertThat(extractWatermarks(h.getOutput())).containsExactly(new Watermark(250));
        }
    }

    /**
     * For buffered build-side changes sharing the same row-time, retractions are applied before
     * accumulations.
     */
    @Test
    void joinPhaseAppliesRetractionsBeforeAccumulationsAtEqualRowTime() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Drive into JOIN with empty build state for k1/k2.
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // Buffer accumulation-first at row-time 105 on absent rows:
            //   k1: +I then -D ; k2: +U then -U ; k3: +U then -U
            addBuildChange(h, insertRecord("k1", "v1", 105L));
            addBuildChange(h, deleteRecord("k1", "v1", 105L));
            addBuildChange(h, updateAfterRecord("k2", "v1", 105L));
            addBuildChange(h, updateBeforeRecord("k2", "v1", 105L));

            // Advance build WM, then access each key to drain its buffer in event-time order.
            addBuildWm(h, 200L);
            addProbeRecord(h, 1L, "k1", "p1");
            addProbeRecord(h, 2L, "k2", "p2");

            assertThat(buildTableForKey(h, op, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 1L));
            assertThat(buildTableForKey(h, op, "k2"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 1L));
            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(1L, "k1", "p1", "k1", "v1", 105L),
                    row(2L, "k2", "p2", "k2", "v1", 105L));
        }
    }

    // ----------------------------------------------------------------- NULL keys

    @ParameterizedTest
    @CsvSource({"true, true", "true, false", "false, true", "false, false"})
    void joinRespectsNullKeysFilter(boolean leftOuter, boolean filterNullKey) throws Exception {
        LateralSnapshotJoinOperator op =
                newOperator(
                        leftOuter,
                        newTrueCondition(),
                        new boolean[] {filterNullKey},
                        100L,
                        null,
                        null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            addBuildChange(h, insertRecord(null, "v_null", 1L));
            addProbeRecord(h, 1L, null, "p_null");
            addBuildWm(h, 100L);

            // test joining during LOAD -> JOIN transition
            if (filterNullKey) {
                if (leftOuter) {
                    JOINED_ASSERTOR.shouldEmitAll(h, row(1L, null, "p_null", null, null, null));
                } else {
                    assertThat(h.getOutput()).isEmpty();
                }
            } else {
                JOINED_ASSERTOR.shouldEmitAll(h, row(1L, null, "p_null", null, "v_null", 1L));
            }

            // test joining in JOIN phase
            addProbeRecord(h, 2L, null, "p_null");
            if (filterNullKey) {
                if (leftOuter) {
                    JOINED_ASSERTOR.shouldEmitAll(h, row(2L, null, "p_null", null, null, null));
                } else {
                    assertThat(h.getOutput()).isEmpty();
                }
            } else {
                JOINED_ASSERTOR.shouldEmitAll(h, row(2L, null, "p_null", null, "v_null", 1L));
            }
        }
    }

    // ----------------------------------------------------------------- Watermark status

    @Test
    void buildSideWmAndWmStatusForwarding() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // probe-side WM
            addProbeWm(h, 70L);

            // LOAD phase: build-side becomes idle then active again. WM is advanced.
            h.processWatermarkStatus2(WatermarkStatus.IDLE);
            h.processWatermarkStatus2(WatermarkStatus.ACTIVE);
            addBuildWm(h, 50L);
            // assert that no WMs or WM statuses are emitted in LOAD.
            assertThat(h.getOutput()).isEmpty();

            // Drive into JOIN
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // assert that only the probe-side WM was forwarded after the transition
            assertThat(h.getOutput()).containsExactly(new Watermark(70L));
            h.getOutput().clear();

            // JOIN phase: build-side becomes idle then active again. WM is advanced
            h.processWatermarkStatus2(WatermarkStatus.IDLE);
            h.processWatermarkStatus2(WatermarkStatus.ACTIVE);
            addBuildWm(h, 200L);

            // No records, WMs, or WM statuses emitted in JOIN.
            assertThat(h.getOutput()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void probeSideWmAndWmStatusForwarding(boolean probeIdleDuringLoad) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            addProbeWm(h, 25L);
            addProbeWm(h, 50L);
            // Probe-side WM statuses received during LOAD.
            h.processWatermarkStatus1(WatermarkStatus.IDLE);
            if (!probeIdleDuringLoad) {
                h.processWatermarkStatus1(WatermarkStatus.ACTIVE);
            }
            // Absorbed during LOAD — nothing emitted.
            assertThat(extractWatermarks(h.getOutput())).isEmpty();
            assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();

            // Flip to JOIN phase — last probe WM emitted.
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            if (probeIdleDuringLoad) {
                assertThat(extractWatermarkStatuses(h.getOutput()))
                        .containsExactly(WatermarkStatus.IDLE);
            } else {
                assertThat(extractWatermarkStatuses(h.getOutput())).isEmpty();
            }
            assertThat(extractWatermarks(h.getOutput())).containsExactly(new Watermark(50));
            h.getOutput().clear();

            // WMs and WM status updates received during JOIN are forwarded.
            addProbeWm(h, 150L);
            assertThat(h.getOutput()).containsExactly(new Watermark(150));
            h.getOutput().clear();
            // set probe-side to idle
            h.processWatermarkStatus1(WatermarkStatus.IDLE);
            assertThat(h.getOutput()).containsExactly(WatermarkStatus.IDLE);
            h.getOutput().clear();
            // set probe-side to active
            h.processWatermarkStatus1(WatermarkStatus.ACTIVE);
            assertThat(h.getOutput()).containsExactly(WatermarkStatus.ACTIVE);
            h.getOutput().clear();
            // emit another watermark
            addProbeWm(h, 200L);
            assertThat(h.getOutput()).containsExactly(new Watermark(200));
            h.getOutput().clear();
        }
    }

    // ----------------------------------------------------------------- State TTL

    @Test
    void stateTtlRefreshesOnAccessAndEvictsInactiveKeys() throws Exception {
        // stateTtlMs = 100. Timers are registered at 1.5 × stateTtlMs, so the deadline for access
        // at t=0 is 150.
        LateralSnapshotJoinOperator op = newOperator(false, 50L, null, 100L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k2", "v1", 1L));
            addBuildChange(h, insertRecord("k3", "v1", 1L));
            addBuildChange(h, insertRecord("k4", "v1", 1L));

            // State is NOT evicted during LOAD even after the deadline passes (TTL fires are
            // rescheduled past the LOAD phase).
            h.setProcessingTime(200);
            assertThat(buildStateKeys(h)).containsExactly("k1", "k2", "k3", "k4");

            // flip to JOIN
            addBuildWm(h, 50L);
            assertPhase(op, Phase.JOIN);

            // Touch k1, k2, k3 to reset their TTL in JOIN; leave k4 alone.
            h.setProcessingTime(275);
            assertThat(buildStateKeys(h)).containsExactly("k1", "k2", "k3", "k4");
            addBuildChange(h, insertRecord("k1", "v2", 2L));
            addBuildChange(h, insertRecord("k2", "v2", 2L));
            addBuildChange(h, insertRecord("k3", "v2", 2L));

            // k4 evicted: it wasn't accessed since proc-time (0) and we flipped to JOIN at (200)
            h.setProcessingTime(350);
            assertThat(buildStateKeys(h)).containsExactly("k1", "k2", "k3");

            // Update build state for k1 and k2; leave k3 alone
            addBuildWm(h, 60L);
            h.setProcessingTime(400);
            assertThat(buildStateKeys(h)).containsExactly("k1", "k2", "k3");
            addBuildChange(h, insertRecord("k1", "v3", 3L));
            addBuildChange(h, insertRecord("k2", "v3", 3L));

            // k3 evicted, not accessed since (275)
            addBuildWm(h, 70L);
            h.setProcessingTime(475);
            assertThat(buildStateKeys(h)).containsExactly("k1", "k2");
            // Access k1 from probe side to reset its TTL again; leave k2 alone
            addProbeRecord(h, 1L, "k1", "p1");

            // k2 evicted, not accessed since (400)
            h.setProcessingTime(550);
            assertThat(buildStateKeys(h)).containsExactly("k1");

            // k1 finally evicted.
            h.setProcessingTime(700);
            assertThat(buildStateKeys(h)).isEmpty();

            // The probe joined against the (k1) multi-set state at proc-time 475 — entries v1,
            // v2, v3.
            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(1L, "k1", "p1", "k1", "v1", 1L),
                    row(1L, "k1", "p1", "k1", "v2", 2L),
                    row(1L, "k1", "p1", "k1", "v3", 3L));
        }
    }

    @Test
    void stateTtlClearsAllPerKeyState() throws Exception {
        // stateTtlMs = 100. Timers are registered at 1.5 × stateTtlMs.
        LateralSnapshotJoinOperator op = newOperator(false, 50L, null, 100L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            // Buffered during LOAD
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 50L); // flip to JOIN
            assertPhase(op, Phase.JOIN);

            // Buffer a change in JOIN that does NOT drain (no further WM advance / access). This
            // populates the change buffer and the buffered-at tag while the build table keeps {v1}.
            addBuildChange(h, insertRecord("k1", "v2"));
            assertThat(buildTableForKey(h, op, "k1")).isEqualTo(Map.of("v1", 1L));
            assertThat(bufferedChangesForKey(h, op, "k1")).hasSize(1);
            assertThat(bufferedAtWmFor(h, op, "k1")).isEqualTo(50L);
            assertThat(ttlExpiryFor(h, op, "k1")).isEqualTo(150L);

            // Fire the TTL timer (well past the eviction deadline).
            h.setProcessingTime(1000);

            // Every per-key state object is cleared, not just the build table.
            assertThat(buildTableForKey(h, op, "k1")).isEmpty();
            assertThat(bufferedChangesForKey(h, op, "k1")).isEmpty();
            assertThat(bufferedAtWmFor(h, op, "k1")).isNull();
            assertThat(probeBufferForKey(h, op, "k1")).isEmpty();
            assertThat(ttlExpiryFor(h, op, "k1")).isNull();
        }
    }

    @Test
    void stateTtlRestoreResetsFlipProcTime() throws Exception {
        // stateTtlMs = 50. Build write at t=0 arms TTL at 75. Flip at t=0 → flipProcTime=0.
        // Original grace ends at 0+50=50.
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, 50L);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(0);
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertThat(op1.getFlipProcTime()).isEqualTo(0L);
            state = h.snapshot(0L, 0L);
        }

        // Restart at t=30 — flipProcTime is re-anchored to 30; new grace ends at 30+50=80.
        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, 50L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setProcessingTime(30);
            h.initializeState(state);
            h.open();
            assertThat(op2.getFlipProcTime()).isEqualTo(30L);

            // At t=75 the recovered TTL timer fires. Grace check: now=75 < flipProcTime+stateTtlMs
            // = 80 → reschedule rather than evict.
            h.setProcessingTime(75);

            // k1 still present because the grace window was re-anchored.
            assertThat(buildStateKeys(h)).containsExactly("k1");

            // advance proc-time to 105 to evict state
            h.setProcessingTime(105);
            assertThat(buildStateKeys(h)).isEmpty();
        }
    }

    // ----------------------------------------------------------------- Snapshot / restore

    @Test
    void restoreFromLoadPhaseSnapshot() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.open();
            // Build-side multi-set with a duplicate count, plus a buffered
            // probe.
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v2", 2L));
            addBuildWm(h, 10L);
            addBuildChange(h, insertRecord("k1", "v3", 3L));
            addProbeRecord(h, 1L, "k1", "p1");
            assertPhase(op1, Phase.LOAD);
            assertThat(probeBufferKeys(h)).containsExactly("k1");
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertPhase(op2, Phase.LOAD);
            // Buffered probe and build-table multi-set (with counts) preserved across restore.
            assertThat(probeBufferKeys(h)).containsExactly("k1");
            assertThat(buildTableForKey(h, op2, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 2L, "v2", 1L));
            assertThat(bufferedChangesForKey(h, op2, "k1")).hasSize(1);
            // Gauge is rebuilt from restored keyed state: one probe buffered for k1.
            assertThat(op2.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(1L);

            // Trigger flip; the buffered probe is joined post-restore, count-respecting against the
            // restored multi-set (2× v1 + 1× v2 = three rows).
            addProbeWm(h, 50L);
            addBuildWm(h, 100L);
            assertPhase(op2, Phase.JOIN);
            assertThat(probeBufferKeys(h)).isEmpty();
            // Draining the restored probe and build buffers brings both gauges back to 0.
            assertThat(op2.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
            assertThat(op2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
            assertThat(buildTableForKey(h, op2, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 2L, "v2", 1L, "v3", 1L));

            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(1L, "k1", "p1", "k1", "v1", 1L),
                    row(1L, "k1", "p1", "k1", "v1", 1L),
                    row(1L, "k1", "p1", "k1", "v2", 2L),
                    row(1L, "k1", "p1", "k1", "v3", 3L));
        }
    }

    @Test
    void restoreFromMixedPhaseSnapshot() throws Exception {
        // Subtask A: drive into JOIN with a buffered change for k1.
        LateralSnapshotJoinOperator opA = newOperator(false, 100L, null, null);
        OperatorSubtaskState stateA;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opA)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(opA, Phase.JOIN);
            // Buffer a build-side change
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            stateA = h.snapshot(0L, 0L);
        }

        // Subtask B: stay in LOAD (no flip-triggering build WM).
        LateralSnapshotJoinOperator opB = newOperator(false, 100L, null, null);
        OperatorSubtaskState stateB;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opB)) {
            h.open();
            addProbeRecord(h, 1L, "k2", "p1");
            assertPhase(opB, Phase.LOAD);
            stateB = h.snapshot(0L, 0L);
        }

        OperatorSubtaskState combined =
                AbstractStreamOperatorTestHarness.repackageState(stateA, stateB);

        // Restore the combined state — phase must be LOAD because some subtask was LOAD.
        LateralSnapshotJoinOperator opC = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(opC)) {
            h.initializeState(combined);
            h.open();
            assertPhase(opC, Phase.LOAD);

            // assert state: A's build table ({v1:1}) plus A's buffered change and B's probe.
            assertThat(buildTableKeys(h)).containsExactly("k1");
            assertThat(buildTableForKey(h, opC, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 1L));
            assertThat(bufferedChangesForKey(h, opC, "k1")).hasSize(1);
            assertThat(probeBufferKeys(h)).containsExactly("k2");

            // During LOAD the current build WM is still MIN_VALUE, so the recovered buffer (tagged
            // at the pre-restore WM) is not drained; the new change is appended to it.
            addBuildChange(h, insertRecord("k1", "v2", 2L));
            assertThat(bufferedChangesForKey(h, opC, "k1")).hasSize(2);
            assertThat(buildTableForKey(h, opC, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 1L));

            // Trigger flip
            addBuildWm(h, 100L);
            assertPhase(opC, Phase.JOIN);

            // Access k1: the buffered changes drain in event-time order, then the probe joins.
            addProbeRecord(h, 1L, "k1", "p1");
            assertThat(bufferedChangesForKey(h, opC, "k1")).isEmpty();
            assertThat(buildTableForKey(h, opC, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v1", 2L, "v2", 1L));
            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(1L, "k1", "p1", "k1", "v1", 1L),
                    row(1L, "k1", "p1", "k1", "v1", 1L),
                    row(1L, "k1", "p1", "k1", "v2", 2L));
        }
    }

    @Test
    void restoreFromJoinPhaseSnapshot() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 100L, null, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k2", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(op1, Phase.JOIN);
            // Buffer a -U/+U pair for each key (tagged at bufferedAt = 100).
            addBuildChange(h, updateBeforeRecord("k1", "v1", 1L));
            addBuildChange(h, updateAfterRecord("k1", "v2", 101L));
            addBuildChange(h, updateBeforeRecord("k2", "v1", 1L));
            addBuildChange(h, updateAfterRecord("k2", "v2", 101L));
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.initializeState(state);
            h.open();
            assertPhase(op2, Phase.JOIN);
            assertThat(op2.getCurrentBuildSideWm()).isEqualTo(Long.MIN_VALUE);
            assertThat(bufferedChangesForKey(h, op2, "k1")).hasSize(2);
            assertThat(bufferedChangesForKey(h, op2, "k2")).hasSize(2);
            // Gauge is rebuilt from restored keyed state: four build-side changes buffered
            // (two for k1, two for k2).
            assertThat(op2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(4L);

            // k2: accessed while no build WM has arrived since restore → eager drain.
            addProbeRecord(h, 1L, "k2", "p-k2");
            assertThat(bufferedChangesForKey(h, op2, "k2")).isEmpty();
            // Draining k2's two restored changes leaves k1's two still buffered.
            assertThat(op2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(2L);
            assertThat(buildTableForKey(h, op2, "k2"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v2", 1L));

            // k1: still buffered (eager drain of k2 left latestBuildSideWm at MIN_VALUE); advance
            // the build WM, then access → normal WM-gated drain.
            assertThat(bufferedChangesForKey(h, op2, "k1")).hasSize(2);
            addBuildWm(h, 200L);
            addProbeRecord(h, 2L, "k1", "p-k1");
            assertThat(bufferedChangesForKey(h, op2, "k1")).isEmpty();
            // All restored changes drained; gauge back to 0.
            assertThat(op2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
            assertThat(buildTableForKey(h, op2, "k1"))
                    .containsExactlyInAnyOrderEntriesOf(Map.of("v2", 1L));

            stripWatermarksAndStatusesFromOutput(h);
            JOINED_ASSERTOR.shouldEmitAll(
                    h,
                    row(1L, "k2", "p-k2", "k2", "v2", 101L),
                    row(2L, "k1", "p-k1", "k1", "v2", 101L));
        }
    }

    @Test
    void restoreRearmsIdleFlipTimer() throws Exception {
        LateralSnapshotJoinOperator op1 = newOperator(false, 1000L, 100L, null);
        OperatorSubtaskState state;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op1)) {
            h.setProcessingTime(50);
            h.open();
            assertPhase(op1, Phase.LOAD);
            state = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator op2 = newOperator(false, 1000L, 100L, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op2)) {
            h.setProcessingTime(150);
            h.initializeState(state);
            h.open();
            assertPhase(op2, Phase.LOAD);
            // Re-armed at open()'s proc-time + idleTimeout = 150 + 100 = 250.
            h.setProcessingTime(249);
            assertPhase(op2, Phase.LOAD);
            h.setProcessingTime(250);
            assertPhase(op2, Phase.JOIN);
        }
    }

    // ----------------------------------------------------------------- Metrics

    @Test
    void currentPhaseGaugeReflectsPhase() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // LOAD = ordinal 0.
            assertThat(op.getCurrentPhaseGauge().getValue()).isEqualTo(0);

            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);
            // JOIN = ordinal 1.
            assertThat(op.getCurrentPhaseGauge().getValue()).isEqualTo(1);
        }
    }

    @Test
    void probeBufferedGaugeTracksLoadBufferingAndDrainsOnFlip() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            assertThat(op.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(0L);

            addProbeRecord(h, 1L, "k1", "p1");
            assertThat(op.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(1L);
            addProbeRecord(h, 2L, "k2", "p2");
            assertThat(op.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(2L);
            addProbeRecord(h, 3L, "k1", "p3");
            assertThat(op.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(3L);

            // Flip drains all buffered probes across keys.
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);
            assertThat(op.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
        }
    }

    @Test
    void buildBufferedGaugeTracksJoinBufferingAndDrains() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // Flip to JOIN with an empty build state (no build records during LOAD).
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);
            assertThat(op.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(0L);

            // JOIN-phase build changes are buffered (their tag equals the current build WM).
            addBuildChange(h, insertRecord("k1", "v2", 101L));
            addBuildChange(h, insertRecord("k1", "v3", 101L));
            assertThat(op.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(2L);
            addBuildChange(h, insertRecord("k2", "v1", 102L));
            assertThat(op.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(3L);

            // Advance build WM, then drain each key by accessing it from the probe side.
            addBuildWm(h, 110L);
            addProbeRecord(h, 1L, "k1", "p1");
            assertThat(op.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(1L);
            addProbeRecord(h, 2L, "k2", "p2");
            assertThat(op.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
        }
    }

    @Test
    void watermarkGaugesTrackBuildAndProbeWatermarks() throws Exception {
        // High loadCompletedTime so the operator stays in LOAD for the first build WMs.
        LateralSnapshotJoinOperator op = newOperator(false, 1000L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            assertThat(op.getCurrentBuildSideWatermarkGauge().getValue()).isEqualTo(Long.MIN_VALUE);
            assertThat(op.getCurrentProbeSideWatermarkGauge().getValue()).isEqualTo(Long.MIN_VALUE);

            // LOAD phase: both watermarks are tracked even though nothing is forwarded.
            addBuildWm(h, 50L);
            assertThat(op.getCurrentBuildSideWatermarkGauge().getValue()).isEqualTo(50L);
            addProbeWm(h, 70L);
            assertThat(op.getCurrentProbeSideWatermarkGauge().getValue()).isEqualTo(70L);
            assertPhase(op, Phase.LOAD);

            // Flip to JOIN.
            addBuildWm(h, 1000L);
            assertPhase(op, Phase.JOIN);
            assertThat(op.getCurrentBuildSideWatermarkGauge().getValue()).isEqualTo(1000L);

            // JOIN phase: probe WM gauge keeps tracking (guards the dedicated currentProbeWm
            // field).
            addProbeWm(h, 1500L);
            assertThat(op.getCurrentProbeSideWatermarkGauge().getValue()).isEqualTo(1500L);
            addBuildWm(h, 1100L);
            assertThat(op.getCurrentBuildSideWatermarkGauge().getValue()).isEqualTo(1100L);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fanOutGaugesTrackMaxAndAverage(boolean leftOuter) throws Exception {
        LateralSnapshotJoinOperator op = newOperator(leftOuter, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // k1 multi-set: v1 x2, v2 x1 → fan-out 3; k2 single row → fan-out 1.
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k1", "v2", 1L));
            addBuildChange(h, insertRecord("k2", "v1", 1L));
            // a probe record during load
            addProbeRecord(h, 1L, "k2", "p1"); // fan-out 1
            // a probe record that does not match
            addProbeRecord(h, 2L, "k3", "p2"); // fan-out INNER: 0, LEFT OUTER: 1
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // check merge stats after transition
            assertThat(op.getMaxJoinFanOutGauge().getValue()).isEqualTo(1L);
            if (leftOuter) {
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isEqualTo(2d / 2);
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(0L);
            } else {
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isCloseTo(1d / 2, within(1e-9));
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(1L);
            }

            // join probe in JOIN phase
            addProbeRecord(h, 3L, "k1", "p3"); // fan-out 3
            assertThat(op.getMaxJoinFanOutGauge().getValue()).isEqualTo(3L);
            if (leftOuter) {
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isEqualTo(5d / 3, within(1e-9));
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(0L);
            } else {
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isCloseTo(4d / 3, within(1e-9));
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(1L);
            }

            addProbeRecord(h, 3L, "k3", "no-match");
            assertThat(op.getMaxJoinFanOutGauge().getValue()).isEqualTo(3L);
            if (leftOuter) {
                // fan-out 1 (LEFT OUTER, null-padded)
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isCloseTo(6.0d / 4, within(1e-9));
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(0L);
            } else {
                // fan-out 0 (INNER, unmatched)
                assertThat(op.getAvgJoinFanOutGauge().getValue()).isCloseTo(4.0d / 4, within(1e-9));
                assertThat(op.getNumUnmatchedProbeRecords().getCount()).isEqualTo(2L);
            }
        }
    }

    @Test
    void numUnmatchedBuildRetractionsCountsAbsentRowRetractions() throws Exception {
        LateralSnapshotJoinOperator op = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.open();
            // LOAD: a -D for a never-inserted row is counted.
            addBuildChange(h, deleteRecord("k1", "ghost", 1L));
            addBuildChange(h, insertRecord("k1", "v1", 2L));
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(0L);

            // Add a real row for k1
            addBuildChange(h, insertRecord("k1", "v1", 3L));
            // Flip to JOIN
            addBuildWm(h, 100L);
            assertPhase(op, Phase.JOIN);

            // Access k1 → drains [-D(ghost)@1, +I(v1)@2]. The -D hits an absent row and is counted.
            addBuildChange(h, deleteRecord("k1", "ghost2", 101L));
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(1L);

            // Advance WM + access → drains -D(ghost2)@3 on an absent row.
            addBuildWm(h, 110L);
            addProbeRecord(h, 1L, "k1", "p1");
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(2L);

            // Deleting an existing row (matching row-time) does not change the metric.
            addBuildChange(h, deleteRecord("k1", "v1", 2L));
            addBuildWm(h, 120L);
            addProbeRecord(h, 2L, "k1", "p2");
            assertThat(op.getNumUnmatchedBuildRetractions().getCount()).isEqualTo(2L);
        }
    }

    @Test
    void numStateTtlEvictionsCountsEvictedKeys() throws Exception {
        // stateTtlMs = 100 → timers registered at 1.5 × = 150.
        LateralSnapshotJoinOperator op = newOperator(false, 50L, null, 100L);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(op)) {
            h.setProcessingTime(0);
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildChange(h, insertRecord("k2", "v1", 1L));
            addBuildWm(h, 50L); // flip to JOIN at proc-time 0
            assertPhase(op, Phase.JOIN);
            assertThat(op.getNumStateTtlEvictions().getCount()).isEqualTo(0L);

            // Neither key is touched again; both evict once their TTL timer (150) fires.
            h.setProcessingTime(150);
            assertThat(buildTableKeys(h)).isEmpty();
            assertThat(op.getNumStateTtlEvictions().getCount()).isEqualTo(2L);
        }
    }

    @Test
    void bufferGaugesAreRebuiltFromStateOnRestore() throws Exception {
        // Snapshot in LOAD with probes buffered across multiple keys, and in JOIN with build-side
        // changes buffered across multiple keys, then assert both gauges are rebuilt from the
        // restored keyed state (the in-memory tallies start at 0 in the restored operator).

        // --- LOAD-phase snapshot: three buffered probes spread over two keys. ---
        LateralSnapshotJoinOperator loadOp = newOperator(false, 100L, null, null);
        OperatorSubtaskState loadState;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(loadOp)) {
            h.open();
            addProbeRecord(h, 1L, "k1", "p1");
            addProbeRecord(h, 2L, "k2", "p2");
            addProbeRecord(h, 3L, "k1", "p3");
            assertPhase(loadOp, Phase.LOAD);
            assertThat(loadOp.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(3L);
            loadState = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator loadOp2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(loadOp2)) {
            h.initializeState(loadState);
            h.open();
            // Rebuilt from restored state: all three probes, across both keys.
            assertThat(loadOp2.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(3L);
            // The build gauge stays 0 — nothing was buffered on the build side.
            assertThat(loadOp2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
        }

        // --- JOIN-phase snapshot: three buffered build-side changes spread over two keys. ---
        LateralSnapshotJoinOperator joinOp = newOperator(false, 100L, null, null);
        OperatorSubtaskState joinState;
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(joinOp)) {
            h.open();
            addBuildChange(h, insertRecord("k1", "v1", 1L));
            addBuildWm(h, 100L);
            assertPhase(joinOp, Phase.JOIN);
            // The v2 access drains the LOAD insert (v1); v2, v3 and k2/v1 remain buffered.
            addBuildChange(h, insertRecord("k1", "v2", 101L));
            addBuildChange(h, insertRecord("k1", "v3", 101L));
            addBuildChange(h, insertRecord("k2", "v1", 101L));
            assertThat(joinOp.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(3L);
            joinState = h.snapshot(0L, 0L);
        }

        LateralSnapshotJoinOperator joinOp2 = newOperator(false, 100L, null, null);
        try (KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h =
                newHarness(joinOp2)) {
            h.initializeState(joinState);
            h.open();
            assertPhase(joinOp2, Phase.JOIN);
            // Rebuilt from restored state: all three buffered changes, across both keys.
            assertThat(joinOp2.getNumBuildSideRecordsBufferedGauge().getValue()).isEqualTo(3L);
            // The probe gauge stays 0 — nothing was buffered on the probe side.
            assertThat(joinOp2.getNumProbeSideRecordsBufferedGauge().getValue()).isEqualTo(0L);
        }
    }

    // ----------------------------------------------------------------- Helpers

    /** Sends a build-side change record (any {@link RowKind}) to the harness. */
    private static void addBuildChange(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            StreamRecord<RowData> record)
            throws Exception {
        h.processElement2(record);
    }

    /** Sends a probe-side {@link RowKind#INSERT} record with the default probe schema. */
    private static void addProbeRecord(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            Long id,
            String key,
            String val)
            throws Exception {
        h.processElement1(insertRecord(id, key, val));
    }

    /** Sends a build-side watermark to the harness (input 2). */
    private static void addBuildWm(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h, long ts)
            throws Exception {
        h.processWatermark2(new Watermark(ts));
    }

    /** Sends a probe-side watermark to the harness (input 1). */
    private static void addProbeWm(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h, long ts)
            throws Exception {
        h.processWatermark1(new Watermark(ts));
    }

    /**
     * Builds an expected joined row. The operator only ever emits {@link RowKind#INSERT}, so we
     * don't accept a rowkind parameter.
     */
    private static GenericRowData row(
            Long id, String pKey, String pVal, String bKey, String bVal, Long bRt) {
        GenericRowData r = new GenericRowData(OUTPUT_TYPES.length);
        r.setField(0, id);
        r.setField(1, pKey == null ? null : StringData.fromString(pKey));
        r.setField(2, pVal == null ? null : StringData.fromString(pVal));
        r.setField(3, bKey == null ? null : StringData.fromString(bKey));
        r.setField(4, bVal == null ? null : StringData.fromString(bVal));
        r.setField(5, bRt);
        r.setRowKind(RowKind.INSERT);
        return r;
    }

    /**
     * Builds an expected joined row for a composite (two-field) equi-key on both sides: probe
     * {@code (kA, kB, val)} concatenated with build {@code (kA, kB, val)}.
     */
    private static GenericRowData compKeyRow(
            String pKa, String pKb, String pVal, String bKa, String bKb, String bVal, Long bRt) {
        GenericRowData r = new GenericRowData(7);
        r.setField(0, pKa == null ? null : StringData.fromString(pKa));
        r.setField(1, pKb == null ? null : StringData.fromString(pKb));
        r.setField(2, pVal == null ? null : StringData.fromString(pVal));
        r.setField(3, bKa == null ? null : StringData.fromString(bKa));
        r.setField(4, bKb == null ? null : StringData.fromString(bKb));
        r.setField(5, bVal == null ? null : StringData.fromString(bVal));
        r.setField(6, bRt);
        r.setRowKind(RowKind.INSERT);
        return r;
    }

    private static void assertPhase(LateralSnapshotJoinOperator op, Phase expected) {
        assertThat(op.getPhase()).isEqualTo(expected);
    }

    private static BinaryRowData stringKey(String key) {
        BinaryRowData k = new BinaryRowData(1);
        BinaryRowWriter w = new BinaryRowWriter(k);
        if (key == null) {
            w.setNullAt(0);
        } else {
            w.writeString(0, StringData.fromString(key));
        }
        w.complete();
        return k;
    }

    private static List<String> buildTableKeys(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
            throws Exception {
        return h.getOperator()
                .getKeyedStateBackend()
                .getKeys(BUILD_TABLE_STATE_NAME, VoidNamespace.INSTANCE)
                .map(r -> ((BinaryRowData) r).getString(0).toString())
                .sorted()
                .toList();
    }

    private static List<String> probeBufferKeys(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
            throws Exception {
        return h.getOperator()
                .getKeyedStateBackend()
                .getKeys(PROBE_BUFFER_STATE_NAME, VoidNamespace.INSTANCE)
                .map(r -> ((BinaryRowData) r).getString(0).toString())
                .sorted()
                .toList();
    }

    /**
     * Returns the keys that currently hold any build-side state — either a materialized build-table
     * entry or a buffered (not-yet-applied) build-side change. TTL eviction clears both, so this
     * reflects whether a key is still "alive", regardless of whether its buffered changes have been
     * drained into the build table yet.
     */
    private static List<String> buildStateKeys(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h)
            throws Exception {
        java.util.SortedSet<String> keys = new java.util.TreeSet<>();
        h.getOperator()
                .getKeyedStateBackend()
                .getKeys(BUILD_TABLE_STATE_NAME, VoidNamespace.INSTANCE)
                .forEach(r -> keys.add(((BinaryRowData) r).getString(0).toString()));
        h.getOperator()
                .getKeyedStateBackend()
                .getKeys(BUILD_CHANGE_BUFFER_STATE_NAME, VoidNamespace.INSTANCE)
                .forEach(r -> keys.add(((BinaryRowData) r).getString(0).toString()));
        return new ArrayList<>(keys);
    }

    /**
     * Returns the build-table multi-set for the given key as {@code build-val → count}. Assumes the
     * schema's value field is at index 1.
     */
    private static Map<String, Long> buildTableForKey(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            LateralSnapshotJoinOperator op,
            String key)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(key));
        Map<String, Long> result = new LinkedHashMap<>();
        for (Map.Entry<RowData, Long> e : op.getBuildTableState().entries()) {
            result.put(e.getKey().getString(1).toString(), e.getValue());
        }
        return result;
    }

    private static List<RowData> probeBufferForKey(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            LateralSnapshotJoinOperator op,
            String key)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(key));
        List<RowData> result = new ArrayList<>();
        for (RowData r : op.getProbeBuffer().get()) {
            result.add(r);
        }
        return result;
    }

    private static List<RowData> bufferedChangesForKey(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            LateralSnapshotJoinOperator op,
            String key)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(key));
        List<RowData> result = new ArrayList<>();
        for (RowData r : op.getBuildChangeBuffer().get()) {
            result.add(r);
        }
        return result;
    }

    private static Long bufferedAtWmFor(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            LateralSnapshotJoinOperator op,
            String key)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(key));
        return op.getBufferedAtWmState().value();
    }

    private static Long ttlExpiryFor(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h,
            LateralSnapshotJoinOperator op,
            String key)
            throws Exception {
        h.getOperator().setCurrentKey(stringKey(key));
        return op.getTtlExpiryState().value();
    }

    /** Drops watermarks and watermark statuses from the harness output queue, in place. */
    private static void stripWatermarksAndStatusesFromOutput(
            KeyedTwoInputStreamOperatorTestHarness<RowData, RowData, RowData, RowData> h) {
        h.getOutput().removeIf(o -> o instanceof Watermark || o instanceof WatermarkStatus);
    }

    private static List<Object> stripWatermarks(ConcurrentLinkedQueue<Object> output) {
        return output.stream().filter(o -> !(o instanceof Watermark)).toList();
    }

    private static List<Watermark> extractWatermarks(ConcurrentLinkedQueue<Object> output) {
        return output.stream().filter(o -> o instanceof Watermark).map(w -> (Watermark) w).toList();
    }

    /**
     * Asserts that exactly one watermark equal to {@code expected} was emitted and that no {@link
     * StreamRecord} follows it. A forwarded watermark must be released only after the records that
     * logically precede it (e.g. probes drained on flip) have been emitted.
     */
    private static void assertWatermarkForwardedAfterRecords(
            ConcurrentLinkedQueue<Object> output, long expectedTs) {
        Watermark expectedWatermark = new Watermark(expectedTs);
        List<Object> elements = List.copyOf(output);
        assertThat(extractWatermarks(output)).containsExactly(expectedWatermark);
        int wmIndex = elements.indexOf(expectedWatermark);
        assertThat(elements.subList(wmIndex + 1, elements.size()))
                .as("no records may be emitted after watermark %s", expectedWatermark)
                .noneMatch(o -> o instanceof StreamRecord);
    }

    private static List<WatermarkStatus> extractWatermarkStatuses(
            ConcurrentLinkedQueue<Object> output) {
        return output.stream()
                .filter(o -> o instanceof WatermarkStatus)
                .map(w -> (WatermarkStatus) w)
                .toList();
    }
}
