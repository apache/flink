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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

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

    /** The column kind of a physical column. */
    public static final String PHYSICAL_COLUMN_KIND = "physical";

    /** The column kind of a persisted (non-virtual) metadata column. */
    public static final String METADATA_PERSISTED_COLUMN_KIND = "metadata persisted";

    private static final String PERSISTED_COLUMN_NOT_USED_IN_QUERY =
            "Failed to execute %s statement.\n"
                    + "Invalid schema. All persisted (physical and metadata) columns "
                    + "in the schema part need to be present in the query part.\n"
                    + "However, %s column `%s` could not be found in the query.";

    private MaterializedTableSchemaUtils() {}

    /**
     * Rejects a declared persisted column that the query does not produce.
     *
     * @param columnKind the column kind, {@link #PHYSICAL_COLUMN_KIND} or {@link
     *     #METADATA_PERSISTED_COLUMN_KIND}
     * @param operationName the SQL statement name, e.g. {@code "CREATE OR ALTER MATERIALIZED
     *     TABLE"}
     */
    public static void checkPersistedColumnProducedByQuery(
            String columnName, String columnKind, Set<String> queryColumns, String operationName) {
        if (!queryColumns.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            PERSISTED_COLUMN_NOT_USED_IN_QUERY,
                            operationName,
                            columnKind,
                            columnName));
        }
    }

    /**
     * Computes the column-level {@link TableChange}s between a materialized table's current schema
     * and its new schema (query-derived, or declared, e.g. by a DDL column list, when {@code
     * schemaDeclared}). The result feeds the append-only enforcement in {@code
     * AlterMaterializedTableChangeOperation#validateChanges}. Per column it emits:
     *
     * <ul>
     *   <li>{@code add} — a column absent from the old schema (nullable when query-derived);
     *   <li>{@code modifyColumnPosition} — an existing persisted column that is out of place, so
     *       that applying the changes reproduces the new schema's persisted column order, in both
     *       modes;
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
     * <p>Positions are compared among persisted columns only, so retained or reordered
     * non-persisted columns do not skew the position diff. Added columns keep a {@code null}
     * position (appended); the existing columns that follow them are moved instead.
     */
    public static List<TableChange> validateAndExtractColumnChanges(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean schemaDeclared) {
        final List<Column> oldColumns = oldSchema.getColumns();
        final List<Column> newColumns = newSchema.getColumns();
        final Map<String, Column> oldByName = new HashMap<>();
        oldColumns.forEach(column -> oldByName.put(column.getName(), column));
        // The table writes query values by position into the persisted columns, so the changes
        // must reproduce the new persisted order exactly. Simulate that order while emitting.
        final List<String> targetOrder = persistedColumnNames(newColumns);
        final List<String> currentOrder =
                oldColumns.stream()
                        .map(Column::getName)
                        .filter(targetOrder::contains)
                        .collect(Collectors.toList());
        final Set<String> seen = new HashSet<>();
        final List<TableChange> changes = new ArrayList<>();
        for (final Column newColumn : newColumns) {
            seen.add(newColumn.getName());
            final Column oldColumn = oldByName.get(newColumn.getName());
            if (oldColumn == null) {
                changes.add(addChange(newColumn, schemaDeclared));
                if (newColumn.isPersisted()) {
                    currentOrder.add(newColumn.getName());
                }
                continue;
            }
            if (newColumn.isPersisted()) {
                addPositionChangeIfMisplaced(oldColumn, targetOrder, currentOrder, changes);
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

        for (final Column oldColumn : oldColumns) {
            if (seen.contains(oldColumn.getName())) {
                continue;
            }
            // Without an explicit DDL column list the new schema only reflects the query
            // projection, so old non-persisted columns are retained, not dropped.
            if (!schemaDeclared && !oldColumn.isPersisted()) {
                continue;
            }
            changes.add(TableChange.dropColumn(oldColumn.getName()));
        }
        return changes;
    }

    private static List<String> persistedColumnNames(List<Column> columns) {
        return columns.stream()
                .filter(Column::isPersisted)
                .map(Column::getName)
                .collect(Collectors.toList());
    }

    private static void addPositionChangeIfMisplaced(
            Column oldColumn,
            List<String> targetOrder,
            List<String> currentOrder,
            List<TableChange> changes) {
        final String name = oldColumn.getName();
        final String expectedPredecessor = predecessor(targetOrder, name);
        if (Objects.equals(expectedPredecessor, predecessor(currentOrder, name))) {
            return;
        }
        currentOrder.remove(name);
        if (expectedPredecessor == null) {
            currentOrder.add(0, name);
            changes.add(TableChange.modifyColumnPosition(oldColumn, ColumnPosition.first()));
            return;
        }
        currentOrder.add(currentOrder.indexOf(expectedPredecessor) + 1, name);
        changes.add(
                TableChange.modifyColumnPosition(
                        oldColumn, ColumnPosition.after(expectedPredecessor)));
    }

    private static @Nullable String predecessor(List<String> order, String name) {
        final int index = order.indexOf(name);
        return index > 0 ? order.get(index - 1) : null;
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
