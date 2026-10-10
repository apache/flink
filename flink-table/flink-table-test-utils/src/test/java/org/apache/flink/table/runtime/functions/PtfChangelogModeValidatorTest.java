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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PtfChangelogModeValidator}, driven through {@code build()}. */
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
                                        TableArgument.forName("input")
                                                .type(DataTypes.of("ROW<a INT, b INT>"))
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
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(ChangelogMode.insertOnly())
                                                .build()),
                        Row.of(1)),
                Arguments.of(
                        // upsert(false) has keyOnlyDeletes=false, satisfying REQUIRE_FULL_DELETE.
                        "REQUIRE_FULL_DELETE with upsert(false)",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.RequireFullDeletePassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .partitionBy("value")
                                                .upsertKey("value")
                                                .changelogMode(ChangelogMode.upsert(false))
                                                .build()),
                        Row.of(1)));
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
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "declares REQUIRE_UPDATE_BEFORE"),
                invalid(
                        "REQUIRE_FULL_DELETE with keyOnlyDeletes=true",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.RequireFullDeletePassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "declares REQUIRE_FULL_DELETE"),
                invalid(
                        "updating mode on an argument without SUPPORT_UPDATES",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(ChangelogMode.all())
                                                .build()),
                        IllegalStateException.class,
                        "does not declare SUPPORT_UPDATES"),
                invalid(
                        "upsert input mode on a non-partitioned argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .upsertKey("value")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build()),
                        IllegalStateException.class,
                        "only possible for SET_SEMANTIC_TABLE arguments"),
                invalid(
                        // {INSERT, DELETE} without UPDATE_BEFORE is still upsert-style.
                        "upsert-style mode without UPDATE_AFTER on a non-partitioned argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(
                                                        keyOnlyDeleteMode(
                                                                RowKind.INSERT, RowKind.DELETE))
                                                .build()),
                        IllegalStateException.class,
                        "only possible for SET_SEMANTIC_TABLE arguments"),
                invalid(
                        "key-only deletes in a retract mode",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(
                                                        keyOnlyDeleteMode(
                                                                RowKind.INSERT,
                                                                RowKind.UPDATE_BEFORE,
                                                                RowKind.UPDATE_AFTER,
                                                                RowKind.DELETE))
                                                .build()),
                        IllegalStateException.class,
                        "Retract changelogs always carry full deletes"),
                invalid(
                        "upsert input mode without a configured upsert key",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingTwoColumnPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(KEY_VALUE_ROW))
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
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(KEY_VALUE_ROW))
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
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(KEY_VALUE_ROW))
                                                .partitionBy("key")
                                                .upsertKey("nonexistentColumn")
                                                .changelogMode(ChangelogMode.all())
                                                .build()),
                        IllegalArgumentException.class,
                        "Upsert key column 'nonexistentColumn' not found"),
                invalid(
                        "SUPPORT_UPDATES declared without a configured changelog mode",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PlainUpdatingPassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .build()),
                        IllegalStateException.class,
                        "no changelog mode was configured. Use .changelogMode(...)"),
                invalid(
                        "upsert output with row-semantic table arguments",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.UpdatingOutputPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .build()),
                        IllegalStateException.class,
                        "don't support upsert output"),
                invalid(
                        "on_time with updating output",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.OnTimeUpdatingOutputPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(TIMED_ROW))
                                                .partitionBy("partition")
                                                .build())
                                .withOnTimeColumn("ts"),
                        IllegalStateException.class,
                        "not supported for PTFs that consume or produce updates"),
                invalid(
                        "on_time with updating input",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.TimerWithUpdatingInputPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(TIMED_ROW))
                                                .partitionBy("partition")
                                                .upsertKey("partition")
                                                .changelogMode(ChangelogMode.upsert(true))
                                                .build())
                                .withOnTimeColumn("ts"),
                        IllegalStateException.class,
                        "not supported for PTFs that consume or produce updates"),
                invalid(
                        "pass-through columns with updating output",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PassThroughUpdatingOutputPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .build()),
                        IllegalStateException.class,
                        "Pass-through columns are not supported"),
                invalid(
                        "changelog mode on an unknown table argument",
                        ProcessTableFunctionTestHarness.ofClass(
                                        PtfTestFunctions.PassthroughPTF.class)
                                .withTableArgument(
                                        TableArgument.forName("input")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .build())
                                .withTableArgument(
                                        TableArgument.forName("missing")
                                                .type(DataTypes.of(VALUE_ROW))
                                                .changelogMode(ChangelogMode.all())
                                                .build()),
                        IllegalStateException.class,
                        "Unknown table argument: 'missing'"));
    }

    private static Arguments invalid(
            String name,
            ProcessTableFunctionTestHarness.Builder<Row> builder,
            Class<? extends Throwable> expectedType,
            String expectedMessage) {
        return Arguments.of(name, (ThrowingCallable) builder::build, expectedType, expectedMessage);
    }

    private static ChangelogMode keyOnlyDeleteMode(RowKind... kinds) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        Arrays.stream(kinds).forEach(builder::addContainedKind);
        return builder.keyOnlyDeletes(true).build();
    }
}
