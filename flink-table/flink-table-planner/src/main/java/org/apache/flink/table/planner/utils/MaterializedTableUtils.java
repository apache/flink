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
import org.apache.flink.sql.parser.ddl.materializedtable.SqlStartMode;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlStartMode.SqlStartModeKind;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.Interval.TimeUnit;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.ColumnPosition;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;

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
        return IntervalFreshness.of(getFreshnessInterval(sqlIntervalLiteral));
    }

    public static StartMode getStartMode(SqlStartMode sqlStartMode) {
        if (sqlStartMode == null) {
            return null;
        }

        SqlStartModeKind sqlStartModeKind = sqlStartMode.getKind();
        StartModeKind startModeKind = deriveStartModeKind(sqlStartModeKind);
        switch (sqlStartModeKind) {
            case FROM_NOW:
            case RESUME_OR_FROM_NOW:
                SqlIntervalLiteral intervalLiteral = sqlStartMode.getIntervalLiteral();
                if (intervalLiteral == null) {
                    return StartMode.of(startModeKind, null, false);
                }

                Interval interval = intervalFrom(intervalLiteral, "start mode");
                validateIntervalValuePositive(interval.getInterval(), "start mode");
                return StartMode.of(startModeKind, interval);

            case RESUME_OR_FROM_BEGINNING:
            case FROM_BEGINNING:
                return StartMode.of(startModeKind);
            case RESUME_OR_FROM_TIMESTAMP:
            case FROM_TIMESTAMP:
                SqlTimestampLiteral timestampLiteral = sqlStartMode.getTimestampLiteral();
                if (timestampLiteral == null) {
                    return StartMode.of(startModeKind, null, false);
                }

                TimestampString timestampString =
                        timestampLiteral.getValueAs(TimestampString.class);
                SqlTypeName timestampTypeName = timestampLiteral.getTypeName();
                long millis = timestampString.getMillisSinceEpoch();
                Instant timestamp = Instant.ofEpochMilli(millis);
                return StartMode.of(
                        startModeKind,
                        timestamp,
                        timestampTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

            default:
                throw new ValidationException(
                        String.format("Unsupported start mode: %s.", sqlStartModeKind));
        }
    }

    private static Interval getFreshnessInterval(SqlIntervalLiteral sqlIntervalLiteral) {
        final IntervalValue intervalValue = sqlIntervalLiteral.getValueAs(IntervalValue.class);
        final SqlTypeName typeName = intervalValue.getIntervalQualifier().typeName();

        if (isDateTimeInterval(typeName)) {
            final Interval freshnessInterval =
                    getDayTimeInterval(
                            intervalValue,
                            typeName,
                            sqlIntervalLiteral.getValueAs(BigDecimal.class),
                            "freshness");
            final int interval = freshnessInterval.getInterval();
            // Freshness interval might be only positive
            validateIntervalValuePositive(interval, "freshness");
            return freshnessInterval;
        }

        throw new ValidationException(
                "Materialized table freshness only supports SECOND, MINUTE, HOUR, DAY, WEEK as the time unit.");
    }

    private static Interval intervalFrom(
            SqlIntervalLiteral sqlIntervalLiteral, String intervalDescription) {

        final IntervalValue intervalValue = sqlIntervalLiteral.getValueAs(IntervalValue.class);
        final SqlTypeName typeName = intervalValue.getIntervalQualifier().typeName();
        if (intervalValue.getIntervalQualifier().isYearMonth()) {
            return getYearMonthInterval(
                    intervalValue,
                    typeName,
                    sqlIntervalLiteral.getValueAs(BigDecimal.class),
                    intervalDescription);
        }

        if (isDateTimeInterval(typeName)) {
            return getDayTimeInterval(
                    intervalValue,
                    typeName,
                    sqlIntervalLiteral.getValueAs(BigDecimal.class),
                    intervalDescription);
        }

        throw new ValidationException(
                String.format(
                        "Materialized table %s only supports SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, YEAR as the time unit.",
                        intervalDescription));
    }

    private static Interval getYearMonthInterval(
            final IntervalValue intervalValue,
            final SqlTypeName typeName,
            final BigDecimal interval,
            final String intervalDescription) {
        final int intervalInt = interval.intValue();
        switch (typeName) {
            case INTERVAL_MONTH:
                if (intervalValue.getIntervalQualifier().timeUnitRange.startUnit
                        == org.apache.calcite.avatica.util.TimeUnit.QUARTER) {
                    return Interval.of(
                            intervalInt / DateTimeUtils.MONTHS_PER_QUARTER, TimeUnit.QUARTER);
                }
                return Interval.of(intervalInt, TimeUnit.MONTH);
            case INTERVAL_YEAR:
                return Interval.of(
                        (int) (intervalInt / MONTH_OF_YEAR.range().getMaximum()), TimeUnit.YEAR);
            default:
                throw new ValidationException(
                        String.format(
                                "Materialized table %s only supports MONTH, QUARTER, YEAR as the time unit.",
                                intervalDescription));
        }
    }

    private static Interval getDayTimeInterval(
            final IntervalValue intervalValue,
            final SqlTypeName typeName,
            final BigDecimal interval,
            final String intervalDescription) {
        final long millis = interval.longValue();
        switch (typeName) {
            case INTERVAL_DAY:
                final int amountOfDays = (int) (millis / DateTimeUtils.MILLIS_PER_DAY);
                if (intervalValue.getIntervalQualifier().timeUnitRange.startUnit
                        == org.apache.calcite.avatica.util.TimeUnit.WEEK) {
                    return Interval.of(amountOfDays / DateTimeUtils.DAYS_PER_WEEK, TimeUnit.WEEK);
                }
                return Interval.of(amountOfDays, TimeUnit.DAY);
            case INTERVAL_HOUR:
                return Interval.of((int) (millis / DateTimeUtils.MILLIS_PER_HOUR), TimeUnit.HOUR);
            case INTERVAL_MINUTE:
                return Interval.of(
                        (int) (millis / DateTimeUtils.MILLIS_PER_MINUTE), TimeUnit.MINUTE);
            case INTERVAL_SECOND:
                return Interval.of(
                        (int) (millis / DateTimeUtils.MILLIS_PER_SECOND), TimeUnit.SECOND);
            default:
                throw new ValidationException(
                        String.format(
                                "Materialized table %s only supports SECOND, MINUTE, HOUR, DAY, WEEK as the time unit.",
                                intervalDescription));
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
        if (!isSchemaChanged(oldSchema, newSchema)) {
            return List.of();
        }

        final List<Column> oldColumns = oldSchema.getColumns();
        final Map<String, Tuple2<Column, Integer>> oldColumnSet = new HashMap<>();
        for (int i = 0; i < oldColumns.size(); i++) {
            Column column = oldColumns.get(i);
            oldColumnSet.put(column.getName(), Tuple2.of(oldColumns.get(i), i));
        }
        // Schema retrieved from query doesn't count existing non persisted columns
        final List<Column> newColumns = newSchema.getColumns();

        List<TableChange> changes = new ArrayList<>();
        for (int i = 0; i < newColumns.size(); i++) {
            Column newColumn = newColumns.get(i);
            Tuple2<Column, Integer> oldColumnToPosition = oldColumnSet.get(newColumn.getName());

            if (oldColumnToPosition == null) {
                changes.add(TableChange.add(newColumn.copy(newColumn.getDataType().nullable())));
                continue;
            }

            // Check if position changed
            applyPositionChanges(newColumns, oldColumnToPosition, i, changes);

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

    private static boolean isDateTimeInterval(SqlTypeName typeName) {
        return typeName == SqlTypeName.INTERVAL_DAY
                || typeName == SqlTypeName.INTERVAL_HOUR
                || typeName == SqlTypeName.INTERVAL_MINUTE
                || typeName == SqlTypeName.INTERVAL_SECOND;
    }

    private static StartModeKind deriveStartModeKind(SqlStartModeKind sqlStartModeKind) {
        switch (sqlStartModeKind) {
            case FROM_NOW:
                return StartModeKind.FROM_NOW;
            case RESUME_OR_FROM_NOW:
                return StartModeKind.RESUME_OR_FROM_NOW;
            case RESUME_OR_FROM_BEGINNING:
                return StartModeKind.RESUME_OR_FROM_BEGINNING;
            case FROM_BEGINNING:
                return StartModeKind.FROM_BEGINNING;
            case RESUME_OR_FROM_TIMESTAMP:
                return StartModeKind.RESUME_OR_FROM_TIMESTAMP;
            case FROM_TIMESTAMP:
                return StartModeKind.FROM_TIMESTAMP;
            default:
                throw new ValidationException(
                        String.format("Unsupported start mode: %s.", sqlStartModeKind));
        }
    }

    // Since it is only for query change, then check only persisted columns which could be
    // changed/added/dropped with such change
    private static boolean isSchemaChanged(ResolvedSchema oldSchema, ResolvedSchema newSchema) {
        List<Column> oldPersistedColumns =
                oldSchema.getColumns().stream()
                        .filter(Column::isPersisted)
                        .collect(Collectors.toList());
        if (oldPersistedColumns.size() != newSchema.getColumnCount()) {
            return true;
        }
        for (int i = 0; i < oldPersistedColumns.size(); i++) {
            Column oldColumn = oldPersistedColumns.get(i);
            Column newColumn = newSchema.getColumn(i).get();
            if (!oldColumn.getName().equals(newColumn.getName())) {
                return true;
            }
            if (!newColumn
                    .getDataType()
                    .getLogicalType()
                    .equals(oldColumn.getDataType().getLogicalType())) {
                return true;
            }
        }

        return false;
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

    public static List<TableChange> validateAndExtractColumnChanges(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean schemaDefinedInQuery) {
        final List<Column> oldColumns = oldSchema.getColumns();
        final Map<String, Tuple2<Column, Integer>> oldByName = new HashMap<>();
        for (int i = 0; i < oldColumns.size(); i++) {
            oldByName.put(oldColumns.get(i).getName(), Tuple2.of(oldColumns.get(i), i));
        }
        final Set<String> seen = new HashSet<>();
        final List<Column> newColumns = newSchema.getColumns();
        final List<TableChange> changes = new ArrayList<>();
        for (int newIndex = 0; newIndex < newColumns.size(); newIndex++) {
            final Column newColumn = newColumns.get(newIndex);
            seen.add(newColumn.getName());
            final Tuple2<Column, Integer> oldEntry = oldByName.get(newColumn.getName());
            if (oldEntry == null) {
                changes.add(addChange(newColumn, schemaDefinedInQuery));
                continue;
            }
            final Column oldColumn = oldEntry.f0;
            // No position diff: DDL order is arbitrary; query-driven reorders are caught by
            // buildSchemaTableChanges on the ALTER MT AS path.
            if (oldColumn.isPhysical()
                    && newColumn.isPhysical()
                    && typeChanged(oldColumn, newColumn, schemaDefinedInQuery)) {
                final DataType newType =
                        schemaDefinedInQuery
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
                                oldColumn,
                                normalizedColumn(newColumn, schemaDefinedInQuery),
                                null));
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
            if (!schemaDefinedInQuery && !entry.getValue().f0.isPersisted()) {
                continue;
            }
            changes.add(TableChange.dropColumn(entry.getKey()));
        }
        return changes;
    }

    private static TableChange.AddColumn addChange(Column column, boolean schemaDefinedInQuery) {
        return TableChange.add(normalizedColumn(column, schemaDefinedInQuery));
    }

    private static Column normalizedColumn(Column column, boolean schemaDefinedInQuery) {
        return schemaDefinedInQuery ? column : column.copy(column.getDataType().nullable());
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

    private static boolean typeChanged(
            Column oldColumn, Column newColumn, boolean schemaDefinedInQuery) {
        final DataType oldType = oldColumn.getDataType();
        final DataType newType = newColumn.getDataType();
        // schemaDefinedInQuery=false: schema is inferred from the query, which may flip
        // nullability without intent — only the base type difference is a real change.
        return schemaDefinedInQuery
                ? !oldType.equals(newType)
                : !oldType.nullable().equals(newType.nullable());
    }

    public static ResolvedSchema getQueryOperationResolvedSchema(
            CatalogMaterializedTable oldTable, ConvertContext context) {
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
            CatalogMaterializedTable oldTable,
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

    private static void validateIntervalValuePositive(
            final int interval, final String description) {
        if (interval <= 0) {
            throw new ValidationException(
                    String.format(
                            "The %s interval currently only supports positive integer type values. But was: %d",
                            description, interval));
        }
    }
}
