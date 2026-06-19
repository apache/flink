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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.flink.table.catalog.Column.computed;
import static org.apache.flink.table.catalog.Column.physical;
import static org.apache.flink.table.operations.materializedtable.AlterMaterializedTableChangeOperationValidationTest.resolvedTable;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for validation of {@link AlterMaterializedTableAsQueryOperation} (ALTER MATERIALIZED TABLE
 * ... AS &lt;query&gt;). CREATE OR ALTER goes through {@link FullAlterMaterializedTableOperation}
 * and is exercised separately by the planner integration tests.
 */
class AlterMaterializedTableAsQueryOperationValidationTest {

    private static final TableChange QUERY_CHANGE =
            TableChange.modifyDefinitionQuery(
                    "SELECT a, b FROM src", "SELECT `src`.`a`, `src`.`b` FROM `src`");

    @Test
    void rejectDropPersistedColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyDefinitionQuery(
                                        "SELECT b FROM src", "SELECT `src`.`b` FROM `src`"),
                                TableChange.dropColumn("a")));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Dropping of persisted column `a` is not supported.");
    }

    @Test
    void acceptDropNonPersistedColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()),
                                computed(
                                        "comp",
                                        new ResolvedExpressionMock(
                                                DataTypes.STRING(), () -> "UPPER(a)"))));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyDefinitionQuery(
                                        "SELECT a FROM src2", "SELECT `src2`.`a` FROM `src2`"),
                                TableChange.dropColumn("comp")));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void rejectReorderColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()),
                                physical("b", DataTypes.STRING()),
                                physical("c", DataTypes.BIGINT())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyDefinitionQuery(
                                        "SELECT a, c, b FROM src",
                                        "SELECT `src`.`a`, `src`.`c`, `src`.`b` FROM `src`"),
                                TableChange.modifyColumnPosition(
                                        physical("c", DataTypes.BIGINT()),
                                        ColumnPosition.after("a"))));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column mismatch at position 2: Original column is [`b` STRING], but new column is [`c` BIGINT].");
    }

    @Test
    void rejectReorderColumnToFirst() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyDefinitionQuery(
                                        "SELECT b, a FROM src",
                                        "SELECT `src`.`b`, `src`.`a` FROM `src`"),
                                TableChange.modifyColumnPosition(
                                        physical("b", DataTypes.STRING()),
                                        ColumnPosition.first())));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column mismatch at position 1: Original column is [`a` INT], but new column is [`b` STRING].");
    }

    @Test
    void rejectTypeChange() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyDefinitionQuery(
                                        "SELECT CAST(a AS BIGINT), b FROM src",
                                        "SELECT CAST(`src`.`a` AS BIGINT), `src`.`b` FROM `src`"),
                                TableChange.modifyPhysicalColumnType(
                                        physical("a", DataTypes.INT()), DataTypes.BIGINT())));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Column mismatch at position 1: Original column is [`a` INT], but new column is [`a` BIGINT].");
    }

    @Test
    void acceptAppendColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(
                        oldTable,
                        List.of(QUERY_CHANGE, TableChange.add(physical("c", DataTypes.BIGINT()))));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void acceptQueryOnlyChange() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableAsQueryOperation op =
                operation(oldTable, List.of(QUERY_CHANGE));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    private static AlterMaterializedTableAsQueryOperation operation(
            ResolvedCatalogMaterializedTable oldTable, List<TableChange> changes) {
        return new AlterMaterializedTableAsQueryOperation(
                ObjectIdentifier.of("cat", "db", "mt"), ignored -> changes, oldTable);
    }
}
