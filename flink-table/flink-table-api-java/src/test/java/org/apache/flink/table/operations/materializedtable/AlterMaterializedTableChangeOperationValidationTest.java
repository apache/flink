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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.catalog.Column.computed;
import static org.apache.flink.table.catalog.Column.metadata;
import static org.apache.flink.table.catalog.Column.physical;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AlterMaterializedTableChangeOperation#validateChanges()} when the operation
 * carries explicit DDL changes (no query change). Query-change validation is exercised by {@link
 * AlterMaterializedTableAsQueryOperationValidationTest}.
 */
class AlterMaterializedTableChangeOperationValidationTest {

    @Test
    void rejectDropPersistedColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.dropColumn("a")));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Dropping of persisted column `a` is not supported.");
    }

    @Test
    void rejectDropPersistedMetadataColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()),
                                metadata("m", DataTypes.STRING(), null, false)));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.dropColumn("m")));

        assertThatThrownBy(op::validateChanges)
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Dropping of persisted column `m` is not supported.");
    }

    @Test
    void acceptDropComputedColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()),
                                computed(
                                        "c",
                                        new ResolvedExpressionMock(
                                                DataTypes.STRING(), () -> "UPPER(a)"))));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.dropColumn("c")));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void acceptDropVirtualMetadataColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()),
                                metadata("v", DataTypes.STRING(), null, true)));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.dropColumn("v")));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void acceptDropNonExistentColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.dropColumn("does-not-exist")));

        // No column to validate against; planner-level checks handle this elsewhere.
        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void acceptAddColumn() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableChangeOperation op =
                operation(oldTable, List.of(TableChange.add(physical("c", DataTypes.BIGINT()))));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    @Test
    void acceptCommentChange() {
        final ResolvedCatalogMaterializedTable oldTable =
                resolvedTable(
                        ResolvedSchema.of(
                                physical("a", DataTypes.INT()), physical("b", DataTypes.STRING())));

        final AlterMaterializedTableChangeOperation op =
                operation(
                        oldTable,
                        List.of(
                                TableChange.modifyColumnComment(
                                        physical("a", DataTypes.INT()), "new comment")));

        assertThatNoException().isThrownBy(op::validateChanges);
    }

    static ResolvedCatalogMaterializedTable resolvedTable(ResolvedSchema resolvedSchema) {
        final CatalogMaterializedTable origin =
                CatalogMaterializedTable.newBuilder()
                        .schema(Schema.newBuilder().fromResolvedSchema(resolvedSchema).build())
                        .comment("")
                        .partitionKeys(List.of())
                        .options(Map.of())
                        .originalQuery("SELECT a FROM src")
                        .expandedQuery("SELECT `src`.`a` FROM `src`")
                        .freshness(IntervalFreshness.ofSecond(60))
                        .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                        .refreshMode(RefreshMode.CONTINUOUS)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .startMode(StartMode.of(StartModeKind.FROM_BEGINNING))
                        .build();
        return new ResolvedCatalogMaterializedTable(
                origin,
                resolvedSchema,
                RefreshMode.CONTINUOUS,
                IntervalFreshness.ofSecond(60),
                StartMode.of(StartModeKind.FROM_BEGINNING));
    }

    static AlterMaterializedTableChangeOperation operation(
            ResolvedCatalogMaterializedTable oldTable, List<TableChange> changes) {
        return new AlterMaterializedTableChangeOperation(
                ObjectIdentifier.of("cat", "db", "mt"), ignored -> changes, oldTable);
    }
}
