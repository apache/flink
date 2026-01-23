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

package org.apache.flink.table.planner.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** The utils for materialized table. */
@Internal
public class MaterializedTableUtils {

    private static final String PERSISTED_COLUMN_NOT_USED_IN_QUERY =
            "Failed to execute ALTER MATERIALIZED TABLE statement.\n"
                    + "Invalid schema change. All persisted (physical and metadata) columns "
                    + "in the schema part need to be present in the query part.\n"
                    + "However, %s column `%s` could not be found in the query.";

    public static IntervalFreshness getMaterializedTableFreshness(
            SqlIntervalLiteral sqlIntervalLiteral) {
        if (sqlIntervalLiteral.signum() < 0) {
            throw new ValidationException(
                    "Materialized table freshness doesn't support negative value.");
        }
        if (sqlIntervalLiteral.getTypeName().getFamily() != SqlTypeFamily.INTERVAL_DAY_TIME) {
            throw new ValidationException(
                    "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
        }

        SqlIntervalLiteral.IntervalValue intervalValue =
                sqlIntervalLiteral.getValueAs(SqlIntervalLiteral.IntervalValue.class);
        String interval = intervalValue.getIntervalLiteral();
        switch (intervalValue.getIntervalQualifier().typeName()) {
            case INTERVAL_DAY:
                return IntervalFreshness.ofDay(interval);
            case INTERVAL_HOUR:
                return IntervalFreshness.ofHour(interval);
            case INTERVAL_MINUTE:
                return IntervalFreshness.ofMinute(interval);
            case INTERVAL_SECOND:
                return IntervalFreshness.ofSecond(interval);
            default:
                throw new ValidationException(
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
        }
    }

    public static LogicalRefreshMode deriveLogicalRefreshMode(SqlRefreshMode sqlRefreshMode) {
        if (sqlRefreshMode == null) {
            return LogicalRefreshMode.AUTOMATIC;
        }

        switch (sqlRefreshMode) {
            case FULL:
                return LogicalRefreshMode.FULL;
            case CONTINUOUS:
                return LogicalRefreshMode.CONTINUOUS;
            default:
                throw new ValidationException(
                        String.format("Unsupported logical refresh mode: %s.", sqlRefreshMode));
        }
    }

    public static RefreshMode fromLogicalRefreshModeToRefreshMode(
            LogicalRefreshMode logicalRefreshMode) {
        switch (logicalRefreshMode) {
            case AUTOMATIC:
                return null;
            case FULL:
                return RefreshMode.FULL;
            case CONTINUOUS:
                return RefreshMode.CONTINUOUS;
            default:
                throw new IllegalArgumentException(
                        "Unknown logical refresh mode: " + logicalRefreshMode);
        }
    }

    // Used to build changes introduced by changed query like
    // ALTER MATERIALIZED TABLE ... AS ...
    public static List<TableChange> buildSchemaTableChanges(
            ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        final List<Column> oldColumns = oldSchema.getColumns();
        final List<Column> newColumns = newSchema.getColumns();
        int persistedColumnOffset = 0;
        final Map<String, Tuple2<Column, Integer>> oldColumnSet = new HashMap<>();
        for (int i = 0; i < oldColumns.size(); i++) {
            Column column = oldColumns.get(i);
            if (!column.isPersisted()) {
                persistedColumnOffset++;
            }
            oldColumnSet.put(column.getName(), Tuple2.of(oldColumns.get(i), i));
        }

        List<TableChange> changes = new ArrayList<>();
        for (int i = 0; i < newColumns.size(); i++) {
            Column newColumn = newColumns.get(i);
            Tuple2<Column, Integer> oldColumnToPosition = oldColumnSet.get(newColumn.getName());

            if (oldColumnToPosition == null) {
                changes.add(TableChange.add(newColumn.copy(newColumn.getDataType().nullable())));
                continue;
            }

            // Check if position changed
            applyPositionChanges(
                    newColumns, oldColumnToPosition, i + persistedColumnOffset, changes);

            Column oldColumn = oldColumnToPosition.f0;
            // Check if column changed
            // Note: it could be unchanged while the position is changed
            if (oldColumn.equals(newColumn)) {
                // no changes
                continue;
            }

            // Check if kind changed
            if (oldColumn.getClass() != newColumn.getClass()) {
                changes.add(TableChange.dropColumn(oldColumn.getName()));
                changes.add(TableChange.add(newColumn.copy(newColumn.getDataType().nullable())));
                continue;
            }

            // Check if comment is changed
            if (!Objects.equals(
                    oldColumn.getComment().orElse(null), newColumn.getComment().orElse(null))) {
                changes.add(
                        TableChange.modifyColumnComment(
                                oldColumn, newColumn.getComment().orElse(null)));
            }

            // Check if physical column type changed
            if (oldColumn.isPhysical()
                    && newColumn.isPhysical()
                    && !oldColumn.getDataType().equals(newColumn.getDataType())) {
                changes.add(
                        TableChange.modifyPhysicalColumnType(oldColumn, newColumn.getDataType()));
            }

            // Check if metadata fields changed
            if (oldColumn instanceof MetadataColumn) {
                applyMetadataColumnChanges(
                        (MetadataColumn) oldColumn, (MetadataColumn) newColumn, changes);
            }

            // Check if computed expression changed
            if (oldColumn instanceof ComputedColumn) {
                applyComputedColumnChanges(
                        (ComputedColumn) oldColumn, (ComputedColumn) newColumn, changes);
            }
        }

        for (Column newColumn : newColumns) {
            oldColumnSet.remove(newColumn.getName());
        }

        for (Map.Entry<String, Tuple2<Column, Integer>> entry : oldColumnSet.entrySet()) {
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

    private static void applyComputedColumnChanges(
            ComputedColumn oldColumn, ComputedColumn newColumn, List<TableChange> changes) {
        if (!oldColumn
                        .getExpression()
                        .asSerializableString()
                        .equals(newColumn.getExpression().asSerializableString())
                && !Objects.equals(
                        oldColumn.explainExtras().orElse(null),
                        newColumn.explainExtras().orElse(null))) {
            // for now there is no dedicated table change
            changes.add(TableChange.dropColumn(oldColumn.getName()));
            changes.add(TableChange.add(newColumn.copy(newColumn.getDataType().nullable())));
        }
    }

    private static void applyMetadataColumnChanges(
            MetadataColumn oldColumn, MetadataColumn newColumn, List<TableChange> changes) {
        if (oldColumn.isVirtual() != newColumn.isVirtual()
                || !Objects.equals(
                        oldColumn.getMetadataKey().orElse(null),
                        newColumn.getMetadataKey().orElse(null))) {
            // for now there is no dedicated table change
            changes.add(TableChange.dropColumn(oldColumn.getName()));
            changes.add(TableChange.add(newColumn.copy(newColumn.getDataType().nullable())));
        }
    }

    public static List<Column> validateAndExtractNewColumns(
            ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        final List<Column> newColumns = getPersistedColumns(newSchema);
        final List<Column> oldColumns = getPersistedColumns(oldSchema);
        final int originalColumnSize = oldColumns.size();
        final int newColumnSize = newColumns.size();

        if (originalColumnSize > newColumnSize) {
            throw new ValidationException(
                    String.format(
                            "Failed to modify query because drop column is unsupported. "
                                    + "When modifying a query, you can only append new columns at the end of original schema. "
                                    + "The original schema has %d columns, but the newly derived schema from the query has %d columns.",
                            originalColumnSize, newColumnSize));
        }

        for (int i = 0; i < oldColumns.size(); i++) {
            Column oldColumn = oldColumns.get(i);
            Column newColumn = newColumns.get(i);
            if (!oldColumn.equals(newColumn)) {
                throw new ValidationException(
                        String.format(
                                "When modifying the query of a materialized table, "
                                        + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                        + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                                i, oldColumn, newColumn));
            }
        }

        final List<Column> newAddedColumns = new ArrayList<>();
        for (int i = oldColumns.size(); i < newColumns.size(); i++) {
            Column newColumn = newColumns.get(i);
            newAddedColumns.add(newColumn.copy(newColumn.getDataType().nullable()));
        }

        return newAddedColumns;
    }

    public static ResolvedSchema getQueryOperationResolvedSchema(
            ResolvedCatalogMaterializedTable oldTable, ConvertContext context) {
        final SqlNode originalQuery =
                context.getFlinkPlanner().parser().parse(oldTable.getOriginalQuery());
        final SqlNode validateQuery = context.getSqlValidator().validate(originalQuery);
        final PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));
        return queryOperation.getResolvedSchema();
    }

    public static void validatePersistedColumnsUsedByQuery(
            ResolvedCatalogMaterializedTable oldTable,
            SqlAlterMaterializedTableSchema alterTableSchema,
            ConvertContext context) {
        final SqlNodeList sqlNodeList = alterTableSchema.getColumnPositions();
        if (sqlNodeList.isEmpty()) {
            return;
        }

        final ResolvedSchema querySchema = getQueryOperationResolvedSchema(oldTable, context);
        validatePersistedColumnsUsedByQuery(sqlNodeList, querySchema);
    }

    public static void validatePersistedColumnsUsedByQuery(
            SqlNodeList columnPositions, ResolvedSchema querySchema) {
        final Set<String> querySchemaColumnNames = new HashSet<>(querySchema.getColumnNames());
        for (SqlNode column : columnPositions) {
            throwIfPersistedColumnNotUsedByQuery(column, querySchemaColumnNames);
        }
    }

    private static void throwIfPersistedColumnNotUsedByQuery(
            SqlNode column, Set<String> querySchemaColumnNames) {
        if (column instanceof SqlRegularColumn) {
            String columnName = ((SqlRegularColumn) column).getName().getSimple();
            if (!querySchemaColumnNames.contains(columnName)) {
                throwPersistedColumnNotUsedException("physical", columnName);
            }
        } else if (column instanceof SqlMetadataColumn) {
            SqlMetadataColumn metadataColumn = (SqlMetadataColumn) column;
            String columnName = metadataColumn.getName().getSimple();
            if (!metadataColumn.isVirtual() && !querySchemaColumnNames.contains(columnName)) {
                throwPersistedColumnNotUsedException("metadata persisted", columnName);
            }
        } else if (column instanceof SqlTableColumnPosition) {
            throwIfPersistedColumnNotUsedByQuery(
                    ((SqlTableColumnPosition) column).getColumn(), querySchemaColumnNames);
        }
    }

    private static List<Column> getPersistedColumns(ResolvedSchema schema) {
        return schema.getColumns().stream()
                .filter(Column::isPersisted)
                .collect(Collectors.toList());
    }

    private static void throwPersistedColumnNotUsedException(String type, String columnName) {
        throw new ValidationException(
                String.format(PERSISTED_COLUMN_NOT_USED_IN_QUERY, type, columnName));
    }
}
