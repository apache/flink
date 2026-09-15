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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness.TableArgument;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.runtime.functions.PtfTestFunctions.KEY_VALUE_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.TIMED_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.VALUE_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.inputArg;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.onInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Changelog-mode behavior of {@link ProcessTableFunctionTestHarness}: what a configured mode makes
 * visible to the function, and which {@link RowKind}s are accepted on the way in and out. Rejection
 * of illegal changelog configuration at {@code build()} time lives in {@link
 * PtfChangelogModeValidatorTest}; the PTFs under test live in {@link PtfTestFunctions}.
 */
class ProcessTableFunctionTestHarnessChangelogModeTest {

    // -------------------------------------------------------------------------
    // Table argument (input) changelog mode
    // -------------------------------------------------------------------------

    @Test
    void testTableArgumentChangelogModeIsPerArgument() throws Exception {
        // A configured updating mode must not leak into a sibling argument.
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.TwoTableChangelogModePTF.class)
                        .withTableArgument(
                                TableArgument.forName("retracting")
                                        .type(DataTypes.of(VALUE_ROW))
                                        .changelogMode(ChangelogMode.all())
                                        .build())
                        .withTableArgument(
                                TableArgument.forName("plain")
                                        .type(DataTypes.of(VALUE_ROW))
                                        .build())
                        .build()) {
            h.processElementForTable("retracting", Row.of(1));

            Row row = h.getOutput().get(0);
            assertThat(row.<String>getFieldAs(0)).isEqualTo(ChangelogMode.all().toString());
            // "plain" is unconfigured and lacks SUPPORT_UPDATES, so it reports the default.
            assertThat(row.<String>getFieldAs(1)).isEqualTo(ChangelogMode.insertOnly().toString());
        }
    }

    @Test
    void testConfiguredUpsertKeysReachEval() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.TableSemanticsProbePTF.class)
                        .withTableArgument(
                                inputArg("ROW<a INT, b INT, c INT>")
                                        .partitionBy("a", "b")
                                        .upsertKey("a", "b")
                                        .changelogMode(ChangelogMode.upsert(true))
                                        .build())
                        .build()) {
            h.processElement(RowKind.UPDATE_AFTER, 1, 2, 99);

            // Output layout is [partition_a, partition_b, mode, upsertKeys]. Upsert-key column
            // names must arrive as resolved field indices, grouped as a single key.
            assertThat(h.getOutput().get(0).<String>getFieldAs(3)).isEqualTo("[[0, 1]]");
        }
    }

    /**
     * Each {@code upsertKey(...)} call declares one more candidate identity key, mirroring the
     * planner's {@code TableSemantics#upsertKeyColumns()} which reports a list of candidates. A
     * retracting input is used so the candidate set is observed for its own sake, independent of
     * the partition-vs-upsert-key check that applies only to upsert-style input.
     */
    @Test
    void testMultipleUpsertKeyCallsDeclareSeparateCandidates() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.TableSemanticsProbePTF.class)
                        .withTableArgument(
                                inputArg("ROW<a INT, b INT, c INT>")
                                        .partitionBy("a")
                                        .upsertKey("a")
                                        .upsertKey("b")
                                        .changelogMode(ChangelogMode.all())
                                        .build())
                        .build()) {
            h.processElement(RowKind.UPDATE_AFTER, 1, 2, 99);

            // Output layout is [partition_a, mode, upsertKeys]. Both candidates must reach eval()
            // as resolved field indices: {a} -> [0] and {b} -> [1].
            assertThat(h.getOutput().get(0).<String>getFieldAs(2)).isEqualTo("[[0], [1]]");
        }
    }

    /**
     * The planner delivers an upsert (no UPDATE_BEFORE) stream whenever the partition columns equal
     * ANY of the input's candidate identity keys, not only the one declared last. So partitioning
     * by "key" is accepted even though a further candidate (value) is declared.
     */
    @Test
    void testUpsertInputAcceptsPartitionMatchingAnyCandidate() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.TableSemanticsProbePTF.class)
                        .withTableArgument(
                                inputArg(KEY_VALUE_ROW)
                                        .partitionBy("key")
                                        .upsertKey("key")
                                        .upsertKey("value")
                                        .changelogMode(ChangelogMode.upsert(true))
                                        .build())
                        .build()) {
            h.processElement(RowKind.UPDATE_AFTER, 1, 100);

            // Output layout is [partition_key, mode, upsertKeys]; both candidates are surfaced.
            assertThat(h.getOutput().get(0).<String>getFieldAs(2)).isEqualTo("[[0], [1]]");
        }
    }

    @Test
    void testProcessElementRejectsRowKindNotInConfiguredInputMode() throws Exception {
        // insertOnly() is trait-consistent for SUPPORT_UPDATES, so the rejection happens at
        // processElement() time rather than at build() time.
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                        .withTableArgument(
                                inputArg(VALUE_ROW)
                                        .changelogMode(ChangelogMode.insertOnly())
                                        .build())
                        .build()) {
            assertThatThrownBy(() -> h.processElement(RowKind.UPDATE_AFTER, 10))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("UPDATE_AFTER");
        }
    }

    @Test
    void testUpsertDeleteNullsNonKeyFieldsWithoutMutatingCaller() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                        .withTableArgument(
                                inputArg(KEY_VALUE_ROW)
                                        .partitionBy("key")
                                        .upsertKey("key")
                                        .changelogMode(ChangelogMode.upsert(true))
                                        .build())
                        .build()) {
            h.processElement(Row.of(1, 100));
            Row deleteRow = Row.ofKind(RowKind.DELETE, 1, 999);
            h.processElement(deleteRow);

            // Output layout is [partition_key, key, value]; non-key fields of a DELETE are nulled
            // before the PTF sees them.
            assertThat(h.getOutput()).hasSize(2);
            Row deleteOutput = h.getOutput().get(1);
            assertThat(deleteOutput.getKind()).isEqualTo(RowKind.DELETE);
            assertThat(deleteOutput.<Integer>getFieldAs(2)).isNull();

            // Nulling must not write through to the caller's Row.
            assertThat(deleteRow.<Integer>getFieldAs(0)).isEqualTo(1);
            assertThat(deleteRow.<Integer>getFieldAs(1)).isEqualTo(999);
        }
    }

    @Test
    void testChangelogModeEmptyDuringTypeInference() throws Exception {
        // TableSemantics#changelogMode() must return Optional.empty() during type inference,
        // matching the contract of the real Flink planner's CallBindingCallContext.
        PtfTestFunctions.TypeInferenceChangelogModeProbePTF.capturedChangelogMode = null;
        try (ProcessTableFunctionTestHarness<Row> h =
                onInput(PtfTestFunctions.TypeInferenceChangelogModeProbePTF.class, VALUE_ROW)
                        .build()) {
            assertThat(PtfTestFunctions.TypeInferenceChangelogModeProbePTF.capturedChangelogMode)
                    .isEqualTo(Optional.empty());
            h.processElement(Row.of(1));
        }
    }

    @Test
    void testProcessElementPreservesRowKindOnUpdatingArgument() throws Exception {
        // RowKind is preserved through processing on an argument with SUPPORT_UPDATES.
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.UpdatingPassthroughPTF.class)
                        .withTableArgument(
                                inputArg(VALUE_ROW).changelogMode(ChangelogMode.all()).build())
                        .build()) {
            h.processElement(RowKind.INSERT, 10);
            h.processElement(RowKind.UPDATE_BEFORE, 15);
            h.processElement(RowKind.UPDATE_AFTER, 20);
            h.processElement(RowKind.DELETE, 30);

            List<Row> output = h.getOutput();
            assertThat(output).hasSize(4);
            assertThat(output.get(0).getKind()).isEqualTo(RowKind.INSERT);
            assertThat(output.get(0).getField("value")).isEqualTo(10);
            assertThat(output.get(1).getKind()).isEqualTo(RowKind.UPDATE_BEFORE);
            assertThat(output.get(1).getField("value")).isEqualTo(15);
            assertThat(output.get(2).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(output.get(2).getField("value")).isEqualTo(20);
            assertThat(output.get(3).getKind()).isEqualTo(RowKind.DELETE);
            assertThat(output.get(3).getField("value")).isEqualTo(30);
        }
    }

    @Test
    void testProcessElementRejectsUpdateOnNonUpdatingArgument() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(PtfTestFunctions.PassthroughPTF.class)
                        .withTableArgument(inputArg(VALUE_ROW).build())
                        .build()) {
            assertThatThrownBy(() -> h.processElement(RowKind.UPDATE_AFTER, 42))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("SUPPORT_UPDATES");
        }
    }

    // -------------------------------------------------------------------------
    // Output changelog mode
    // -------------------------------------------------------------------------

    @Test
    void testDerivedOutputChangelogModeTracksInputMode() throws Exception {
        // With no explicit output mode, build() must run the resolver. This PTF's
        // getChangelogMode() echoes its input mode, so the echoed value proves both that the
        // resolver ran and that configured input modes reached it.
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.InputDrivenChangelogModePTF.class)
                        .withTableArgument(
                                inputArg(KEY_VALUE_ROW)
                                        .partitionBy("key")
                                        .upsertKey("key")
                                        .changelogMode(ChangelogMode.upsert(true))
                                        .build())
                        .build()) {
            h.processElement(RowKind.UPDATE_AFTER, 1, 42);
            assertThat(h.getOutput().get(0).<String>getFieldAs(1))
                    .isEqualTo(ChangelogMode.upsert(true).toString());
        }
    }

    @Test
    void testUpsertOutputMapsNullValueRowToDelete() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(PtfTestFunctions.UpsertSetSemanticPTF.class)
                        .withTableArgument(inputArg(KEY_VALUE_ROW).partitionBy("key").build())
                        .build()) {
            h.processElement(Row.of(1, 100));
            h.processElement(Row.of(1, null));

            assertThat(h.getOutput()).hasSize(2);
            assertThat(h.getOutput().get(0).getKind()).isEqualTo(RowKind.UPDATE_AFTER);
            assertThat(h.getOutput().get(1).getKind()).isEqualTo(RowKind.DELETE);
        }
    }

    @Test
    void testCollectRejectsRowKindNotInDefaultOutputMode() throws Exception {
        // A PTF that is not a ChangelogFunction falls back to insertOnly(), and the rejection must
        // name the kinds that were permitted.
        try (ProcessTableFunctionTestHarness<Row> h =
                onInput(PtfTestFunctions.InvalidRowKindPTF.class, VALUE_ROW).build()) {
            assertThatThrownBy(() -> h.processElement(Row.of(1)))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContainingAll("Invalid row kind received", "DELETE", "[INSERT]");
        }
    }

    @Test
    void testOnTimerRejectsRowKindNotInConfiguredOutputMode() throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h =
                ProcessTableFunctionTestHarness.ofClass(
                                PtfTestFunctions.TimerEmitsInvalidRowKindPTF.class)
                        .withTableArgument(inputArg(TIMED_ROW).partitionBy("partition").build())
                        .withOnTimeColumn("ts")
                        .build()) {
            h.processElement(Row.of("P1", LocalDateTime.of(2025, 1, 1, 0, 0, 1)));
            assertThatThrownBy(() -> h.setWatermark(LocalDateTime.of(2025, 1, 1, 0, 0, 2)))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContainingAll("Invalid row kind received", "DELETE");
        }
    }
}
