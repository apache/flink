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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils for materialized table schema. */
@Internal
public final class MaterializedTableSchemaUtils {

    private MaterializedTableSchemaUtils() {}

    /**
     * Computes the column-level {@link TableChange}s between a materialized table's current schema
     * and its new schema (query-derived, or declared, e.g. by a DDL column list, when {@code
     * schemaDeclared}). The result feeds the append-only enforcement in {@code
     * AlterMaterializedTableChangeOperation#validateChanges}. Per column it emits:
     *
     * <ul>
     *   <li>{@code add} — a column absent from the old schema (nullable when query-derived);
     *   <li>{@code modifyColumnPosition} — an existing column the query moved; the query order is
     *       authoritative, so a reorder surfaces here (and is later rejected). A DDL column list
     *       keeps its own order and emits no reposition;
     *   <li>{@code modifyPhysicalColumnType} — a physical-type change. For a query-derived schema a
     *       tightening nullability flip (nullable to NOT NULL) is tolerated as an inference
     *       artifact, while a loosening flip is surfaced; a DDL-defined schema surfaces any
     *       difference;
     *   <li>{@code modifyColumn} / {@code modifyColumnComment} — changed computed/metadata
     *       definitions or comments;
     *   <li>{@code dropColumn} — an old column absent from the new schema; old non-persisted
     *       columns are retained when the schema is query-derived.
     * </ul>
     *
     * <p>Existing columns are ranked among the columns that survive into the new schema, so
     * retained non-persisted columns do not skew the position diff.
     */
    public static List<TableChange> validateAndExtractColumnChanges(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean schemaDeclared) {
        final List<Column> oldColumns = oldSchema.getColumns();
        final List<Column> newColumns = newSchema.getColumns();
        final Set<String> newColumnNames =
                newColumns.stream().map(Column::getName).collect(Collectors.toSet());
        // Position each old column among the columns that survive into the new schema, so retained
        // non-persisted columns (absent from the query projection) do not skew the position diff.
        final Map<String, Tuple2<Column, Integer>> oldByName = new HashMap<>();
        int nextPosition = 0;
        for (final Column oldColumn : oldColumns) {
            final Integer position =
                    newColumnNames.contains(oldColumn.getName()) ? nextPosition++ : null;
            oldByName.put(oldColumn.getName(), Tuple2.of(oldColumn, position));
        }
        final Set<String> seen = new HashSet<>();
        final List<TableChange> changes = new ArrayList<>();
        for (int newIndex = 0; newIndex < newColumns.size(); newIndex++) {
            final Column newColumn = newColumns.get(newIndex);
            seen.add(newColumn.getName());
            final Tuple2<Column, Integer> oldEntry = oldByName.get(newColumn.getName());
            if (oldEntry == null) {
                changes.add(addChange(newColumn, schemaDeclared));
                continue;
            }
            final Column oldColumn = oldEntry.f0;
            // The query order is authoritative, so reposition a column the query moved; a
            // DDL-defined schema keeps the arbitrary DDL order.
            if (!schemaDeclared) {
                applyPositionChanges(newColumns, oldEntry, newIndex, changes);
            }
            if (oldColumn.isPhysical()
                    && newColumn.isPhysical()
                    && typeChanged(oldColumn, newColumn, schemaDeclared)) {
                final DataType newType =
                        schemaDeclared
                                ? newColumn.getDataType()
                                : newColumn.getDataType().nullable();
                changes.add(TableChange.modifyPhysicalColumnType(oldColumn, newType));
                // Type changed; still check whether the comment also changed.
                final String oldComment = oldColumn.getComment().orElse(null);
                final String newComment = newColumn.getComment().orElse(null);
                if (!Objects.equals(oldComment, newComment)) {
                    changes.add(TableChange.modifyColumnComment(oldColumn, newComment));
                }
                continue;
            }
            if (oldColumn.getClass() != newColumn.getClass()
                    || !definitionEquals(oldColumn, newColumn)) {
                changes.add(
                        new TableChange.ModifyColumn(
                                oldColumn, normalizedColumn(newColumn, schemaDeclared), null));
                continue;
            }
            final String oldComment = oldColumn.getComment().orElse(null);
            final String newComment = newColumn.getComment().orElse(null);
            if (!Objects.equals(oldComment, newComment)) {
                changes.add(TableChange.modifyColumnComment(oldColumn, newComment));
            }
        }

        for (Map.Entry<String, Tuple2<Column, Integer>> entry : oldByName.entrySet()) {
            if (seen.contains(entry.getKey())) {
                continue;
            }
            // Without an explicit DDL column list the new schema only reflects the query
            // projection, so old non-persisted columns are retained, not dropped.
            if (!schemaDeclared && !entry.getValue().f0.isPersisted()) {
                continue;
            }
            changes.add(TableChange.dropColumn(entry.getKey()));
        }
        return changes;
    }

    private static void applyPositionChanges(
            List<Column> newColumns,
            Tuple2<Column, Integer> oldColumnToPosition,
            int currentPosition,
            List<TableChange> changes) {
        Column oldColumn = oldColumnToPosition.f0;
        int oldPosition = oldColumnToPosition.f1;
        if (oldPosition != currentPosition) {
            ColumnPosition position =
                    currentPosition == 0
                            ? ColumnPosition.first()
                            : ColumnPosition.after(newColumns.get(currentPosition - 1).getName());
            changes.add(TableChange.modifyColumnPosition(oldColumn, position));
        }
    }

    private static TableChange.AddColumn addChange(Column column, boolean schemaDeclared) {
        return TableChange.add(normalizedColumn(column, schemaDeclared));
    }

    private static Column normalizedColumn(Column column, boolean schemaDeclared) {
        return schemaDeclared ? column : column.copy(column.getDataType().nullable());
    }

    private static boolean definitionEquals(Column oldColumn, Column newColumn) {
        if (oldColumn instanceof MetadataColumn && newColumn instanceof MetadataColumn) {
            final MetadataColumn oldMeta = (MetadataColumn) oldColumn;
            final MetadataColumn newMeta = (MetadataColumn) newColumn;
            return oldMeta.isVirtual() == newMeta.isVirtual()
                    && Objects.equals(
                            oldMeta.getMetadataKey().orElse(null),
                            newMeta.getMetadataKey().orElse(null))
                    && oldMeta.getDataType().equals(newMeta.getDataType());
        }
        if (oldColumn instanceof ComputedColumn && newColumn instanceof ComputedColumn) {
            return Objects.equals(
                    ((ComputedColumn) oldColumn).getExpression(),
                    ((ComputedColumn) newColumn).getExpression());
        }
        return true;
    }

    private static boolean typeChanged(Column oldColumn, Column newColumn, boolean schemaDeclared) {
        final DataType oldType = oldColumn.getDataType();
        final DataType newType = newColumn.getDataType();
        if (schemaDeclared) {
            return !oldType.equals(newType);
        }
        // Query-inferred nullability is a real change only when it loosens (NOT NULL -> nullable):
        // the stored column can no longer hold the query's possible nulls. A tightening is
        // tolerated.
        final boolean baseTypeChanged = !oldType.nullable().equals(newType.nullable());
        final boolean loosened =
                !oldType.getLogicalType().isNullable() && newType.getLogicalType().isNullable();
        return baseTypeChanged || loosened;
    }
}
