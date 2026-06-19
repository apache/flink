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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.expressions.utils.ResolvedExpressionMock;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/**
 * Tests for {@link MaterializedTableChangeHandler} — pure applier behavior exercised without the
 * planner stack. Validation lives in {@link
 * AlterMaterializedTableChangeOperation#validateChanges()}; tests there assert what is rejected.
 */
class MaterializedTableChangeHandlerTest {

    /** Default schema used by tests that don't refine it: [a INT, b STRING]. */
    private static final Schema DEFAULT_SCHEMA =
            Schema.newBuilder()
                    .column("a", DataTypes.INT())
                    .column("b", DataTypes.STRING())
                    .build();

    private static CatalogMaterializedTable.Builder defaultBuilder() {
        return CatalogMaterializedTable.newBuilder()
                .schema(DEFAULT_SCHEMA)
                .comment("")
                .partitionKeys(List.of())
                .options(Map.of())
                .originalQuery("dummy")
                .expandedQuery("dummy")
                .freshness(IntervalFreshness.ofSecond(60))
                .logicalRefreshMode(LogicalRefreshMode.AUTOMATIC)
                .refreshMode(RefreshMode.CONTINUOUS)
                .refreshStatus(RefreshStatus.INITIALIZING)
                .startMode(StartMode.of(StartModeKind.FROM_BEGINNING));
    }

    @Test
    void shouldDropPersistedColumn() {
        // old schema: [a INT, b STRING]
        final CatalogMaterializedTable newTable =
                applyChanges(defaultBuilder().build(), List.of(TableChange.dropColumn("a")));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("b");
    }

    @Test
    void shouldDropComputedColumn() {
        // old schema: [a INT, comp AS UPPER(a)]
        final CatalogMaterializedTable tableWithComputed =
                defaultBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .columnByExpression("comp", "UPPER(a)")
                                        .build())
                        .build();

        final CatalogMaterializedTable newTable =
                applyChanges(tableWithComputed, List.of(TableChange.dropColumn("comp")));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("a");
    }

    @Test
    void shouldDropVirtualMetadataColumn() {
        // old schema: [a INT, v STRING METADATA VIRTUAL]
        final CatalogMaterializedTable tableWithMetadata =
                defaultBuilder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .columnByMetadata("v", DataTypes.STRING(), null, true)
                                        .build())
                        .build();

        final CatalogMaterializedTable newTable =
                applyChanges(tableWithMetadata, List.of(TableChange.dropColumn("v")));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("a");
    }

    @Test
    void shouldApplyAddColumnAfter() {
        // old schema: [a INT, b STRING]
        final CatalogMaterializedTable newTable =
                applyChanges(
                        defaultBuilder().build(),
                        List.of(
                                TableChange.add(
                                        Column.physical("x", DataTypes.BIGINT()),
                                        ColumnPosition.after("a"))));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("a", "x", "b");
        assertThat(newTable.getUnresolvedSchema().getColumns())
                .element(1)
                .asInstanceOf(type(Schema.UnresolvedPhysicalColumn.class))
                .returns(DataTypes.BIGINT(), Schema.UnresolvedPhysicalColumn::getDataType);
    }

    @Test
    void shouldApplyAddComputedColumnFirst() {
        // old schema: [a INT, b STRING]
        final CatalogMaterializedTable newTable =
                applyChanges(
                        defaultBuilder().build(),
                        List.of(
                                TableChange.add(
                                        Column.computed(
                                                "comp",
                                                new ResolvedExpressionMock(
                                                        DataTypes.INT(), () -> "1")),
                                        ColumnPosition.first())));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("comp", "a", "b");
    }

    @Test
    void shouldApplyAddVirtualMetadataColumn() {
        // old schema: [a INT, b STRING]
        final CatalogMaterializedTable newTable =
                applyChanges(
                        defaultBuilder().build(),
                        List.of(
                                TableChange.add(
                                        Column.metadata("m", DataTypes.STRING(), null, true))));

        assertThat(newTable.getUnresolvedSchema().getColumns())
                .extracting(Schema.UnresolvedColumn::getName)
                .containsExactly("a", "b", "m");
    }

    @Test
    void shouldRejectUnsupportedTableChange() {
        // old schema: [a INT, b STRING]
        final TableChange unsupported = new TableChange() {};

        assertThatThrownBy(() -> applyChanges(defaultBuilder().build(), List.of(unsupported)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Unsupported table change");
    }

    private static CatalogMaterializedTable applyChanges(
            CatalogMaterializedTable oldTable, List<TableChange> changes) {
        final MaterializedTableChangeHandler handler =
                MaterializedTableChangeHandler.getHandlerWithChanges(oldTable, changes);
        return MaterializedTableChangeHandler.buildNewMaterializedTable(handler);
    }
}
