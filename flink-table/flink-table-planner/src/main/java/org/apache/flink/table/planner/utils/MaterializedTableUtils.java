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
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlAlterMaterializedTableSchema;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

    public static List<Column> validateAndExtractNewColumns(
            ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        List<Column> newAddedColumns = new ArrayList<>();
        int originalColumnSize = oldSchema.getColumns().size();
        int newColumnSize = newSchema.getColumns().size();

        if (originalColumnSize > newColumnSize) {
            throw new ValidationException(
                    String.format(
                            "Failed to modify query because drop column is unsupported. "
                                    + "When modifying a query, you can only append new columns at the end of original schema. "
                                    + "The original schema has %d columns, but the newly derived schema from the query has %d columns.",
                            originalColumnSize, newColumnSize));
        }

        for (int i = 0; i < oldSchema.getColumns().size(); i++) {
            Column oldColumn = oldSchema.getColumns().get(i);
            Column newColumn = newSchema.getColumns().get(i);
            if (!oldColumn.equals(newColumn)) {
                throw new ValidationException(
                        String.format(
                                "When modifying the query of a materialized table, "
                                        + "currently only support appending columns at the end of original schema, dropping, renaming, and reordering columns are not supported.\n"
                                        + "Column mismatch at position %d: Original column is [%s], but new column is [%s].",
                                i, oldColumn, newColumn));
            }
        }

        for (int i = oldSchema.getColumns().size(); i < newSchema.getColumns().size(); i++) {
            Column newColumn = newSchema.getColumns().get(i);
            newAddedColumns.add(newColumn.copy(newColumn.getDataType().nullable()));
        }

        return newAddedColumns;
    }

    public static void validatePersistedColumnsUsedByQuery(
            ResolvedCatalogMaterializedTable oldTable,
            SqlAlterMaterializedTableSchema alterTableSchema,
            ConvertContext context) {
        final SqlNodeList sqlNodeList = alterTableSchema.getColumnPositions();
        if (sqlNodeList.isEmpty()) {
            return;
        }

        final SqlNode originalQuery =
                context.getFlinkPlanner().parser().parse(oldTable.getOriginalQuery());
        final SqlNode validateQuery = context.getSqlValidator().validate(originalQuery);
        final PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));

        validatePersistedColumnsUsedByQuery(sqlNodeList, queryOperation.getResolvedSchema());
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
            SqlRegularColumn physicalColumn = (SqlRegularColumn) column;
            String columnName = physicalColumn.getName().getSimple();
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

    private static void throwPersistedColumnNotUsedException(String type, String columnName) {
        throw new ValidationException(
                String.format(PERSISTED_COLUMN_NOT_USED_IN_QUERY, type, columnName));
    }
}
