/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates the changelog-mode configuration of the given {@link ProcessTableFunction} currently
 * being tested.
 *
 * <p>It validates per-argument input modes and upsert keys against the arguments' declared traits.
 * It also validates the resolved output mode against on-time compatibility, the set-semantics
 * requirement for upsert output, and the pass-through-columns restriction.
 */
@Internal
final class PtfChangelogModeValidator {

    private final List<ProcessTableFunctionTestHarness.TableArgumentInfo> tableArguments;
    private final Map<String, ChangelogMode> tableArgumentChangelogModes;
    private final Map<String, List<String[]>> tableArgumentUpsertKeys;
    @Nullable private final String onTimeColumnName;

    PtfChangelogModeValidator(
            List<ProcessTableFunctionTestHarness.TableArgumentInfo> tableArguments,
            Map<String, ChangelogMode> tableArgumentChangelogModes,
            Map<String, List<String[]>> tableArgumentUpsertKeys,
            @Nullable String onTimeColumnName) {
        this.tableArguments = tableArguments;
        this.tableArgumentChangelogModes = tableArgumentChangelogModes;
        this.tableArgumentUpsertKeys = tableArgumentUpsertKeys;
        this.onTimeColumnName = onTimeColumnName;
    }

    /**
     * Validates the per-argument changelog configuration: unknown argument names, SUPPORT_UPDATES
     * requiring an explicit mode, REQUIRE_UPDATE_BEFORE/REQUIRE_FULL_DELETE trait compatibility,
     * upsert mode requiring set semantics with matching partition columns, upsert-key column
     * validity, and on-time column existence.
     *
     * @throws IllegalArgumentException if configuration references unknown arguments
     * @throws IllegalStateException if configuration violates documented constraints
     */
    void validateConfiguration() {
        validateChangelogModeArgsAreKnown();
        validateUpsertKeyColumnsExist();
        // Validated here at config time so a missing column reports "does not exist" instead of the
        // later, more confusing on-time/changelog compatibility error.
        validateOnTimeColumnExists();

        for (ProcessTableFunctionTestHarness.TableArgumentInfo tableArg : tableArguments) {
            if (tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)) {
                validateUpdatingInput(tableArg);
            } else {
                validateNonUpdatingInputIsInsertOnly(tableArg);
            }
        }
    }

    private Set<String> tableArgNames() {
        return tableArguments.stream().map(t -> t.name).collect(Collectors.toSet());
    }

    private void validateChangelogModeArgsAreKnown() {
        Set<String> validTableArgNames = tableArgNames();
        for (String argName : tableArgumentChangelogModes.keySet()) {
            if (!validTableArgNames.contains(argName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unknown table argument: '%s'. Available table arguments: %s",
                                argName, validTableArgNames));
            }
        }
    }

    private void validateUpsertKeyColumnsExist() {
        for (Map.Entry<String, List<String[]>> entry : tableArgumentUpsertKeys.entrySet()) {
            String argName = entry.getKey();
            List<String[]> candidateKeys = entry.getValue();

            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg =
                    tableArguments.stream()
                            .filter(t -> t.name.equals(argName))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    String.format(
                                                            "Unknown table argument for upsert key: '%s'. "
                                                                    + "Available table arguments: %s",
                                                            argName, tableArgNames())));

            for (String[] candidateKey : candidateKeys) {
                validateUpsertKeyColumnNames(tableArg, candidateKey);
            }
        }
    }

    private void validateOnTimeColumnExists() {
        if (onTimeColumnName == null) {
            return;
        }
        boolean foundInAnyTable =
                tableArguments.stream()
                        .anyMatch(
                                t ->
                                        ProcessTableFunctionTestHarness.getFieldNames(t.dataType)
                                                .contains(onTimeColumnName));
        if (!foundInAnyTable) {
            throw new IllegalArgumentException(
                    String.format(
                            "withOnTimeColumn references column '%s' which does not exist in any "
                                    + "table argument. Available table arguments and their columns: %s",
                            onTimeColumnName,
                            tableArguments.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    t -> t.name,
                                                    t ->
                                                            ProcessTableFunctionTestHarness
                                                                    .getFieldNames(t.dataType)))));
        }
    }

    private static void validateUpdatingInput(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg) {
        ChangelogMode mode = requireExplicitChangelogMode(tableArg);
        requireUpdateBeforeIfDeclared(tableArg, mode);
        requireFullDeleteIfDeclared(tableArg, mode);
        if (isUpsertStyleInput(mode)) {
            validateUpsertStyleInput(tableArg, mode);
        }
    }

    private static ChangelogMode requireExplicitChangelogMode(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg) {
        if (tableArg.changelogMode == null) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' declares SUPPORT_UPDATES but no changelog mode "
                                    + "was configured. Use .withTableArgumentChangelogMode(\"%s\", ...) "
                                    + "to specify what changelog mode this argument receives.",
                            tableArg.name, tableArg.name));
        }
        return tableArg.changelogMode;
    }

    private static void requireUpdateBeforeIfDeclared(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, ChangelogMode mode) {
        // Insert-only mode is legal even with REQUIRE_UPDATE_BEFORE since it has no updates to
        // encode.
        if (tableArg.is(StaticArgumentTrait.REQUIRE_UPDATE_BEFORE)
                && !mode.containsOnly(RowKind.INSERT)
                && !mode.contains(RowKind.UPDATE_BEFORE)) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' declares REQUIRE_UPDATE_BEFORE but "
                                    + "configured mode %s does not include UPDATE_BEFORE.",
                            tableArg.name, mode));
        }
    }

    private static void requireFullDeleteIfDeclared(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, ChangelogMode mode) {
        if (tableArg.is(StaticArgumentTrait.REQUIRE_FULL_DELETE) && mode.keyOnlyDeletes()) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' declares REQUIRE_FULL_DELETE but "
                                    + "configured mode %s has keyOnlyDeletes=true.",
                            tableArg.name, mode));
        }
    }

    /**
     * An update without UPDATE_BEFORE is upsert-style and needs a co-located key; a retract (with
     * UPDATE_BEFORE) carries its own before-image and needs none.
     */
    private static boolean isUpsertStyleInput(ChangelogMode mode) {
        return !mode.containsOnly(RowKind.INSERT) && !mode.contains(RowKind.UPDATE_BEFORE);
    }

    private static void validateUpsertStyleInput(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, ChangelogMode mode) {
        if (!tableArg.isPartitioned()) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' is configured with upsert mode %s, "
                                    + "but this is only possible for SET_SEMANTIC_TABLE arguments "
                                    + "with non-empty PARTITION BY columns. "
                                    + "ROW_SEMANTIC_TABLE arguments or arguments with no "
                                    + "partitioning can only use insertOnly() or all() modes.",
                            tableArg.name, mode));
        }
        List<String[]> candidateKeys = tableArg.upsertKeys;
        if (candidateKeys.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' is configured with upsert mode %s, "
                                    + "but no upsert key was configured. "
                                    + "Use .upsertKey(...) to specify "
                                    + "the upsert key columns.",
                            tableArg.name, mode));
        }

        // The planner delivers an upsert (no UPDATE_BEFORE) stream to a PTF only when the partition
        // columns coincide with one of the input's candidate identity keys; otherwise it must
        // retract. Mirror that: partition columns must equal one candidate (order-independent).
        Set<String> partitionCols = new HashSet<>(Arrays.asList(tableArg.partitionColumnNames));
        boolean partitionMatchesCandidate =
                candidateKeys.stream()
                        .anyMatch(key -> new HashSet<>(Arrays.asList(key)).equals(partitionCols));
        if (!partitionMatchesCandidate) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' partition columns %s do not match any "
                                    + "configured upsert-key candidate %s. "
                                    + "For upsert input modes, partition columns must contain "
                                    + "the exact same set of columns as one candidate upsert "
                                    + "key (order-independent).",
                            tableArg.name,
                            Arrays.toString(tableArg.partitionColumnNames),
                            candidateKeys.stream()
                                    .map(Arrays::toString)
                                    .collect(Collectors.toList())));
        }
    }

    private static void validateNonUpdatingInputIsInsertOnly(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg) {
        // A non-SUPPORT_UPDATES argument is contractually insert-only, so any other configured mode
        // describes a stream the planner could never deliver here.
        ChangelogMode configured = tableArg.changelogMode;
        if (configured != null && !configured.equals(ChangelogMode.insertOnly())) {
            throw new IllegalStateException(
                    String.format(
                            "Table argument '%s' does not declare SUPPORT_UPDATES, so "
                                    + "its changelog mode must be insertOnly(). "
                                    + "Configured mode: %s",
                            tableArg.name, configured));
        }
    }

    /**
     * Validates the resolved output changelog mode against on-time compatibility, the set-semantics
     * requirement for upsert output, and the pass-through-columns restriction.
     *
     * <p>Deliverability and the "non-ChangelogFunction is insert-only" rule are not checked here:
     * the mode always originates from {@link PtfChangelogModeResolver} (which only assembles
     * deliverable modes) or the insert-only default for a non-{@link
     * org.apache.flink.table.functions.ChangelogFunction}, so both hold by construction.
     *
     * @param outputMode the resolved output changelog mode
     * @throws IllegalStateException if the mode violates on-time, set-semantics, or pass-through
     *     constraints
     */
    void validateResolvedOutputMode(ChangelogMode outputMode) {
        validateOnTimeCompatibleWithChangelogModes(outputMode);
        validateUpsertOutputRequiresSetSemantics(outputMode);
        validatePassThroughColumnsCompatibleWithOutputMode(outputMode);
    }

    private void validateOnTimeCompatibleWithChangelogModes(ChangelogMode outputMode) {
        if (onTimeColumnName == null) {
            return;
        }
        boolean anyUpdatingInput =
                tableArguments.stream()
                        .anyMatch(t -> !t.effectiveChangelogMode().containsOnly(RowKind.INSERT));
        boolean updatingOutput = !outputMode.containsOnly(RowKind.INSERT);
        if (anyUpdatingInput || updatingOutput) {
            throw new IllegalStateException(
                    "Time operations using the `on_time` argument are currently not "
                            + "supported for PTFs that consume or produce updates.");
        }
    }

    private void validateUpsertOutputRequiresSetSemantics(ChangelogMode outputMode) {
        if (outputMode.containsOnly(RowKind.INSERT) || outputMode.contains(RowKind.UPDATE_BEFORE)) {
            return;
        }
        for (ProcessTableFunctionTestHarness.TableArgumentInfo tableArg : tableArguments) {
            if (!tableArg.isSetSemantic()) {
                throw new IllegalStateException(
                        String.format(
                                "PTFs that take table arguments with row semantics don't "
                                        + "support upsert output. Table argument '%s' must "
                                        + "use set semantics.",
                                tableArg.name));
            }
        }
    }

    private void validatePassThroughColumnsCompatibleWithOutputMode(ChangelogMode outputMode) {
        if (outputMode.containsOnly(RowKind.INSERT)) {
            return;
        }
        for (ProcessTableFunctionTestHarness.TableArgumentInfo tableArg : tableArguments) {
            if (tableArg.is(StaticArgumentTrait.PASS_COLUMNS_THROUGH)) {
                throw new IllegalStateException(
                        "Pass-through columns are not supported for PTFs that produce updates.");
            }
        }
    }

    private void validateUpsertKeyColumnNames(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, String[] upsertKeyColumns) {
        List<String> fieldNames = ProcessTableFunctionTestHarness.getFieldNames(tableArg.dataType);
        for (String columnName : upsertKeyColumns) {
            if (!fieldNames.contains(columnName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Upsert key column '%s' not found in table argument '%s'. "
                                        + "Available fields: %s",
                                columnName, tableArg.name, fieldNames));
            }
        }
    }
}
