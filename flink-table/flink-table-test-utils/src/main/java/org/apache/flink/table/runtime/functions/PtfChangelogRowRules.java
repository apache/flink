/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Changelog-mode rules applied to each row entering or leaving the {@link ProcessTableFunction}
 * under test.
 */
@Internal
final class PtfChangelogRowRules {

    private PtfChangelogRowRules() {}

    /** Validates the row kind, then reduces key-only deletes to their key columns. */
    static Row prepareInputRow(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, Row row) {
        validateInputRowKind(tableArg, row.getKind());
        return stripNonKeyFieldsForKeyOnlyDelete(tableArg, row);
    }

    static void validateOutputRowKind(ChangelogMode outputMode, RowKind rowKind) {
        if (!outputMode.contains(rowKind)) {
            throw new TableRuntimeException(
                    String.format(
                            "Invalid row kind received: %s. Expected produced changelog mode: %s",
                            rowKind, outputMode));
        }
    }

    private static void validateInputRowKind(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, RowKind rowKind) {
        ChangelogMode mode = tableArg.effectiveChangelogMode();
        if (mode.contains(rowKind)) {
            return;
        }

        if (!tableArg.is(StaticArgumentTrait.SUPPORT_UPDATES)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Row kind %s is not permitted on table argument '%s'. "
                                    + "This argument does not declare SUPPORT_UPDATES.",
                            rowKind, tableArg.name));
        }

        throw new IllegalArgumentException(
                String.format(
                        "Row kind %s is not permitted on table argument '%s'. "
                                + "Expected consumed changelog mode: %s",
                        rowKind, tableArg.name, mode));
    }

    private static Row stripNonKeyFieldsForKeyOnlyDelete(
            ProcessTableFunctionTestHarness.TableArgumentInfo tableArg, Row row) {
        if (row.getKind() != RowKind.DELETE
                || !tableArg.effectiveChangelogMode().keyOnlyDeletes()) {
            return row;
        }

        // A key-only delete carries only the columns the stream is co-partitioned by.
        int[] keyIndices = ProcessTableFunctionTestHarness.getPartitionColumnIndices(tableArg);
        if (keyIndices.length == 0) {
            return row;
        }

        Row result = new Row(row.getKind(), row.getArity());
        for (int i : keyIndices) {
            result.setField(i, row.getField(i));
        }
        return result;
    }
}
