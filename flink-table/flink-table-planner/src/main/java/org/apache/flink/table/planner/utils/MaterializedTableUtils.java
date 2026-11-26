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
import org.apache.flink.sql.parser.ddl.materializedtable.SqlStartMode;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlStartMode.SqlStartModeKind;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.Interval;
import org.apache.flink.table.catalog.Interval.TimeUnit;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.StartMode.StartModeKind;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTimestampLiteral;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.METADATA_PERSISTED_COLUMN_KIND;
import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.PHYSICAL_COLUMN_KIND;
import static org.apache.flink.table.operations.materializedtable.MaterializedTableSchemaUtils.checkPersistedColumnProducedByQuery;

/** The utils for materialized table. */
@Internal
public class MaterializedTableUtils {

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
            ConvertContext context,
            String operationName) {
        final SqlNodeList sqlNodeList = alterTableSchema.getColumnPositions();
        if (sqlNodeList.isEmpty()) {
            return;
        }

        final ResolvedSchema querySchema = getQueryOperationResolvedSchema(oldTable, context);
        validatePersistedColumnsUsedByQuery(sqlNodeList, querySchema, operationName);
    }

    public static void validatePersistedColumnsUsedByQuery(
            SqlNodeList columnPositions, ResolvedSchema querySchema, String operationName) {
        final Set<String> querySchemaColumnNames = new HashSet<>(querySchema.getColumnNames());
        for (SqlNode column : columnPositions) {
            throwIfPersistedColumnNotUsedByQuery(column, querySchemaColumnNames, operationName);
        }
    }

    private static void throwIfPersistedColumnNotUsedByQuery(
            SqlNode column, Set<String> querySchemaColumnNames, String operationName) {
        if (column instanceof SqlRegularColumn) {
            checkPersistedColumnProducedByQuery(
                    ((SqlRegularColumn) column).getName().getSimple(),
                    PHYSICAL_COLUMN_KIND,
                    querySchemaColumnNames,
                    operationName);
        } else if (column instanceof SqlMetadataColumn) {
            SqlMetadataColumn metadataColumn = (SqlMetadataColumn) column;
            if (!metadataColumn.isVirtual()) {
                checkPersistedColumnProducedByQuery(
                        metadataColumn.getName().getSimple(),
                        METADATA_PERSISTED_COLUMN_KIND,
                        querySchemaColumnNames,
                        operationName);
            }
        } else if (column instanceof SqlTableColumnPosition) {
            throwIfPersistedColumnNotUsedByQuery(
                    ((SqlTableColumnPosition) column).getColumn(),
                    querySchemaColumnNames,
                    operationName);
        }
    }

    /** Returns a copy of the validated query whose output columns follow {@code targetColumns}. */
    public static SqlNode reorderQueryColumns(
            SqlNode validatedQuery, List<String> queryColumns, List<String> targetColumns) {
        final Set<String> targetSet = new HashSet<>(targetColumns);
        Preconditions.checkArgument(
                targetSet.size() == targetColumns.size()
                        && targetColumns.size() == queryColumns.size()
                        && targetSet.equals(new HashSet<>(queryColumns)),
                "Target columns %s are not a permutation of query columns %s.",
                targetColumns,
                queryColumns);
        return reorderQuery(validatedQuery, queryColumns, targetColumns, true);
    }

    /**
     * Only the leftmost branch of a set operation names its columns. {@code namesColumns} is false
     * for the other branches, which keep their own aliases because their ORDER BY may refer to
     * them.
     */
    private static SqlNode reorderQuery(
            SqlNode query,
            List<String> queryColumns,
            List<String> targetColumns,
            boolean namesColumns) {
        if (query instanceof SqlSelect) {
            return reorderSelect((SqlSelect) query, queryColumns, targetColumns, namesColumns);
        }
        if (query instanceof SqlWith) {
            return reorderWith((SqlWith) query, queryColumns, targetColumns, namesColumns);
        }
        if (query.getKind().belongsTo(SqlKind.SET_QUERY)) {
            return reorderSetOperation((SqlCall) query, queryColumns, targetColumns, namesColumns);
        }
        throw unsupportedQueryReorder();
    }

    private static SqlSelect reorderSelect(
            SqlSelect select,
            List<String> queryColumns,
            List<String> targetColumns,
            boolean namesColumns) {
        final SqlNodeList selectList = select.getSelectList();
        if (selectList.size() != queryColumns.size()) {
            throw unsupportedQueryReorder();
        }
        final List<SqlNode> reorderedItems = new ArrayList<>(selectList.size());
        for (int targetPosition = 0; targetPosition < targetColumns.size(); targetPosition++) {
            final int sourceIndex = queryColumns.indexOf(targetColumns.get(targetPosition));
            final SqlNode item = selectList.get(sourceIndex);
            reorderedItems.add(
                    namesColumns ? withOutputName(item, queryColumns.get(sourceIndex)) : item);
        }
        final SqlSelect copy = (SqlSelect) select.clone(select.getParserPosition());
        copy.setSelectList(new SqlNodeList(reorderedItems, selectList.getParserPosition()));
        copy.setOrderBy(remapOrdinals(select.getOrderList(), queryColumns, targetColumns));
        return copy;
    }

    private static SqlNode withOutputName(SqlNode item, String outputName) {
        final SqlNode expression =
                item.getKind() == SqlKind.AS ? ((SqlCall) item).operand(0) : item;
        return SqlStdOperatorTable.AS.createCall(
                item.getParserPosition(),
                expression,
                new SqlIdentifier(outputName, item.getParserPosition()));
    }

    private static SqlCall reorderSetOperation(
            SqlCall setOperation,
            List<String> queryColumns,
            List<String> targetColumns,
            boolean namesColumns) {
        final List<SqlNode> operands = setOperation.getOperandList();
        final List<SqlNode> reorderedOperands = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            reorderedOperands.add(
                    reorderQuery(
                            operands.get(i), queryColumns, targetColumns, namesColumns && i == 0));
        }
        return setOperation
                .getOperator()
                .createCall(
                        setOperation.getFunctionQuantifier(),
                        setOperation.getParserPosition(),
                        reorderedOperands.toArray(new SqlNode[0]));
    }

    private static SqlWith reorderWith(
            SqlWith with,
            List<String> queryColumns,
            List<String> targetColumns,
            boolean namesColumns) {
        return new SqlWith(
                with.getParserPosition(),
                with.withList,
                reorderQuery(with.body, queryColumns, targetColumns, namesColumns));
    }

    private static @Nullable SqlNodeList remapOrdinals(
            @Nullable SqlNodeList orderList,
            List<String> queryColumns,
            List<String> targetColumns) {
        if (orderList == null) {
            return null;
        }
        final List<SqlNode> remappedItems = new ArrayList<>(orderList.size());
        for (SqlNode orderItem : orderList) {
            remappedItems.add(remapOrdinal(orderItem, queryColumns, targetColumns));
        }
        return new SqlNodeList(remappedItems, orderList.getParserPosition());
    }

    private static SqlNode remapOrdinal(
            SqlNode orderItem, List<String> queryColumns, List<String> targetColumns) {
        if (orderItem instanceof SqlNumericLiteral) {
            final int ordinal = ((SqlNumericLiteral) orderItem).intValue(false);
            if (ordinal < 1) {
                return orderItem;
            }
            final int newOrdinal = targetColumns.indexOf(queryColumns.get(ordinal - 1)) + 1;
            return SqlLiteral.createExactNumeric(
                    String.valueOf(newOrdinal), orderItem.getParserPosition());
        }
        if (orderItem.getKind() == SqlKind.DESCENDING
                || orderItem.getKind() == SqlKind.NULLS_FIRST
                || orderItem.getKind() == SqlKind.NULLS_LAST) {
            final SqlCall modifier = (SqlCall) orderItem;
            return modifier.getOperator()
                    .createCall(
                            modifier.getParserPosition(),
                            remapOrdinal(modifier.operand(0), queryColumns, targetColumns));
        }
        return orderItem;
    }

    private static ValidationException unsupportedQueryReorder() {
        return new ValidationException(
                "The column list of a materialized table reorders the query's columns, which"
                        + " is not supported for this query. Wrap the query in a SELECT that lists"
                        + " the columns in the column list's order instead.");
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
