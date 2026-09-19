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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.runtime.functions.ProcessTableFunctionTestHarness.TableArgument;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.functions.PtfTestFunctions.KEY_VALUE_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.TIMED_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.VALUE_ROW;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.inputArg;
import static org.apache.flink.table.runtime.functions.PtfTestFunctions.onInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the changelog-mode rules enforced by {@link PtfChangelogModeValidator}, driven through
 * {@link ProcessTableFunctionTestHarness.Builder#build()} so that the wiring between builder and
 * validator is covered too. The trait combinations each case relies on are declared by the PTFs in
 * {@link PtfTestFunctions}.
 */
class PtfChangelogModeValidatorTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("validConfigurations")
    void testValidConfigurationBuildsAndAcceptsInsert(
            String name, ProcessTableFunctionTestHarness.Builder<Row> builder, Row input)
            throws Exception {
        try (ProcessTableFunctionTestHarness<Row> h = builder.build()) {
            h.processElement(input);
            assertThat(h.getOutput()).hasSize(1);
            assertThat(h.getOutput().get(0).getKind()).isEqualTo(RowKind.INSERT);
        }
    }

    private static Stream<Arguments> validConfigurations() {
        return Stream.of(
                Arguments.of(
                        // Upsert key comparison is set-based: partition (a, b) matches key (b, a).
                        "upsert key ordering independent of partition column order",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg("ROW<a INT, b INT>")
                                                .partitionBy("a", "b")
                                                .upsertKey("b", "a")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        Row.of(1, 2)),
                Arguments.of(
                        // Insert-only mode has no updates, so it trivially satisfies the trait.
                        "REQUIRE_UPDATE_BEFORE with insertOnly()",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .changelogMode(ChangelogMode.insertOnly())
                                                .build()),
                        Row.of(1)),
                Arguments.of(
                        // upsert(false) has keyOnlyDeletes=false, satisfying REQUIRE_FULL_DELETE.
                        "REQUIRE_FULL_DELETE with upsert(false)",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.RequireFullDeletePassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .partitionBy("value")
                                                .upsertKey("value")
                                                .changelogMode(ChangelogMode.upsert(false))
                                                .build()),
                        Row.of(1)),
                Arguments.of(
                        // Name validation is deferred to build(), so order must not matter.
                        "upsert key configured before the table argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .upsertKey("key")
                                                .type(DataTypes.of(KEY_VALUE_ROW))
                                                .partitionBy("key")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        Row.of(1, 42)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidConfigurations")
    void testInvalidConfigurationRejectedAtBuild(
            String name,
            ThrowingCallable build,
            Class<? extends Throwable> expectedType,
            String expectedMessage) {
        assertThatThrownBy(build).isInstanceOf(expectedType).hasMessageContaining(expectedMessage);
    }

    private static Stream<Arguments> invalidConfigurations() {
        return Stream.of(
                invalid(
                        "REQUIRE_UPDATE_BEFORE with a mode lacking UPDATE_BEFORE",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "declares REQUIRE_UPDATE_BEFORE"),
                invalid(
                        "REQUIRE_FULL_DELETE with keyOnlyDeletes=true",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.RequireFullDeletePassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "declares REQUIRE_FULL_DELETE"),
                invalid(
                        "updating mode on an argument without SUPPORT_UPDATES",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .changelogMode(ChangelogMode.all())
                                                .build()),
                        IllegalStateException.class,
                        "does not declare SUPPORT_UPDATES"),
                invalid(
                        "upsert input mode on a non-partitioned argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .upsertKey("value")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "only possible for SET_SEMANTIC_TABLE arguments"),
                invalid(
                        // {INSERT, DELETE} with keyOnlyDeletes(true) has no UPDATE_BEFORE, so it
                        // is upsert-style and must still require partitioning + a matching upsert
                        // key.
                        "upsert-style mode without UPDATE_AFTER on a non-partitioned argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(VALUE_ROW)
                                                .changelogMode(
                                                        keyOnlyDeleteMode(
                                                                RowKind.INSERT, RowKind.DELETE))
                                                .build()),
                        IllegalStateException.class,
                        "only possible for SET_SEMANTIC_TABLE arguments"),
                invalid(
                        "upsert input mode without a configured upsert key",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(KEY_VALUE_ROW)
                                                .partitionBy("key")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "no upsert key was configured"),
                invalid(
                        "upsert key columns not matching partition columns",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(KEY_VALUE_ROW)
                                                .partitionBy("key")
                                                .upsertKey("value")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "do not match any configured upsert-key candidate"),
                invalid(
                        // Upsert-key column validation runs unconditionally, so a non-upsert mode
                        // must not skip it.
                        "upsert key naming a column absent from the schema",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        inputArg(KEY_VALUE_ROW)
                                                .partitionBy("key")
                                                .upsertKey("nonexistentColumn")
                                                .changelogMode(ChangelogMode.all())
                                                .build()),
                        IllegalArgumentException.class,
                        "Upsert key column 'nonexistentColumn' not found"),
                invalid(
                        "SUPPORT_UPDATES declared without a configured changelog mode",
                        onInput(PtfTestFunctions.PlainUpdatingPassthroughPTF.class, VALUE_ROW),
                        IllegalStateException.class,
                        "declares SUPPORT_UPDATES but no changelog mode"),
                invalid(
                        "upsert output with row-semantic table arguments",
                        onInput(PtfTestFunctions.UpdatingOutputPTF.class, VALUE_ROW),
                        IllegalStateException.class,
                        "don't support upsert output"),
                invalid(
                        "on_time with updating output",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.OnTimeUpdatingOutputPTF.class)
                                .withTableArgument(
                                        inputArg(TIMED_ROW).partitionBy("partition").build())
                                .withOnTimeColumn("ts"),
                        IllegalStateException.class,
                        "not supported for PTFs that consume or produce updates"),
                invalid(
                        // The updating input alone triggers the rejection; the PTF declares
                        // insert-only output so the violation is unambiguously input-side.
                        "on_time with updating input",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.TimerWithUpdatingInputPTF.class)
                                .withTableArgument(
                                        inputArg(TIMED_ROW)
                                                .partitionBy("partition")
                                                .upsertKey("partition")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build())
                                .withOnTimeColumn("ts"),
                        IllegalStateException.class,
                        "not supported for PTFs that consume or produce updates"),
                invalid(
                        "on_time column absent from every table argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.TimerEmitsInvalidRowKindPTF.class)
                                .withTableArgument(
                                        inputArg(TIMED_ROW).partitionBy("partition").build())
                                .withOnTimeColumn("missing"),
                        IllegalArgumentException.class,
                        "does not exist in any"),
                invalid(
                        "pass-through columns with updating output",
                        onInput(PtfTestFunctions.PassThroughUpdatingOutputPTF.class, VALUE_ROW),
                        IllegalStateException.class,
                        "Pass-through columns are not supported"));
    }

    private static Arguments invalid(
            String name,
            ProcessTableFunctionTestHarness.Builder<Row> builder,
            Class<? extends Throwable> expectedType,
            String expectedMessage) {
        return Arguments.of(name, (ThrowingCallable) builder::build, expectedType, expectedMessage);
    }

    private static ChangelogMode keyOnlyDeleteMode(RowKind... kinds) {
        return modeBuilder(kinds).keyOnlyDeletes(true).build();
    }

    private static ChangelogMode.Builder modeBuilder(RowKind... kinds) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        Arrays.stream(kinds).forEach(builder::addContainedKind);
        return builder;
    }
}
