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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.canBeTimeAttributeType;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isTimeAttribute;

/**
 * Relational operation that assigns rows to windows using a windowing table-valued function
 * (TUMBLE/HOP/CUMULATE/SESSION). Appends {@code window_start}, {@code window_end} and {@code
 * window_time} to the input and returns the enriched relation.
 */
@Internal
public class WindowTableFunctionQueryOperation implements QueryOperation {

    private static final String INPUT_ALIAS = "$$T_WIN";

    /** Window kind; the name maps to the SQL window operator. */
    @Internal
    public enum WindowKind {
        TUMBLE(1),
        HOP(2),
        CUMULATE(2),
        SESSION(1);

        private final int expectedIntervalCount;

        WindowKind(int expectedIntervalCount) {
            this.expectedIntervalCount = expectedIntervalCount;
        }

        /** Number of interval operands this window kind requires. */
        public int expectedIntervalCount() {
            return expectedIntervalCount;
        }
    }

    private final WindowKind windowKind;
    private final String timeColumn;
    private final List<Duration> intervals;
    private final QueryOperation child;
    private final ResolvedSchema resolvedSchema;

    public WindowTableFunctionQueryOperation(
            WindowKind windowKind,
            String timeColumn,
            List<Duration> intervals,
            QueryOperation child) {
        if (intervals.size() != windowKind.expectedIntervalCount()) {
            throw new ValidationException(
                    String.format(
                            "Window kind %s requires %d interval(s), but got %d.",
                            windowKind, windowKind.expectedIntervalCount(), intervals.size()));
        }
        final ResolvedSchema inputSchema = child.getResolvedSchema();
        final int timeIndex = inputSchema.getColumnNames().indexOf(timeColumn);
        if (timeIndex < 0) {
            throw new ValidationException(
                    String.format(
                            "Window time column '%s' does not exist. Available columns: %s",
                            timeColumn, inputSchema.getColumnNames()));
        }
        final DataType timeType = inputSchema.getColumnDataTypes().get(timeIndex);
        if (!canBeTimeAttributeType(timeType.getLogicalType())) {
            throw new ValidationException(
                    String.format(
                            "Window time column '%s' must be a TIMESTAMP or TIMESTAMP_LTZ column, "
                                    + "but was %s.",
                            timeColumn, timeType.getLogicalType()));
        }
        if (!isTimeAttribute(timeType.getLogicalType())) {
            throw new ValidationException(
                    String.format(
                            "Window time column '%s' must be a time attribute (rowtime or "
                                    + "proctime), but was %s.",
                            timeColumn, timeType.getLogicalType()));
        }
        this.windowKind = windowKind;
        this.timeColumn = timeColumn;
        this.intervals = intervals;
        this.child = child;
        this.resolvedSchema = deriveResolvedSchema(inputSchema, timeType);
    }

    private static ResolvedSchema deriveResolvedSchema(
            ResolvedSchema inputSchema, DataType timeColumnType) {
        final List<Column> columns = new ArrayList<>(inputSchema.getColumns());
        columns.add(Column.physical("window_start", DataTypes.TIMESTAMP(3).notNull()));
        columns.add(Column.physical("window_end", DataTypes.TIMESTAMP(3).notNull()));
        columns.add(Column.physical("window_time", timeColumnType.notNull()));
        return ResolvedSchema.of(columns);
    }

    public WindowKind getWindowKind() {
        return windowKind;
    }

    public String getTimeColumn() {
        return timeColumn;
    }

    public List<Duration> getIntervals() {
        return intervals;
    }

    public QueryOperation getChild() {
        return child;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public String asSummaryString() {
        final Map<String, Object> args = new LinkedHashMap<>();
        args.put("window", windowKind);
        args.put("timeCol", timeColumn);
        args.put("intervals", intervals);
        return OperationUtils.formatWithChildren(
                "WindowTableFunction", args, getChildren(), Operation::asSummaryString);
    }

    @Override
    public String asSerializableString(SqlFactory sqlFactory) {
        return String.format(
                "SELECT %s FROM TABLE(%s\n) %s",
                OperationUtils.formatSelectColumns(getResolvedSchema(), INPUT_ALIAS),
                OperationUtils.indent(serializeWindowCall(sqlFactory)),
                INPUT_ALIAS);
    }

    private String serializeWindowCall(SqlFactory sqlFactory) {
        final String childSql = OperationUtils.indent(child.asSerializableString(sqlFactory));
        final String descriptor = "DESCRIPTOR(" + EncodingUtils.escapeIdentifier(timeColumn) + ")";
        final String serializedIntervals =
                intervals.stream()
                        .map(
                                d ->
                                        ApiExpressionUtils.intervalOfMillis(d.toMillis())
                                                .asSerializableString(sqlFactory))
                        .collect(Collectors.joining(", "));
        return String.format(
                "%s((%s\n), %s, %s)", windowKind, childSql, descriptor, serializedIntervals);
    }

    @Override
    public List<QueryOperation> getChildren() {
        return List.of(child);
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
