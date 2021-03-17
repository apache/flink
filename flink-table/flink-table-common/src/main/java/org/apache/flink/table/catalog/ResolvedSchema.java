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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.utils.DataTypeUtils.removeTimeAttribute;

/**
 * Schema of a table or view consisting of columns, constraints, and watermark specifications.
 *
 * <p>This class is the result of resolving a {@link Schema} into a final validated representation.
 *
 * <ul>
 *   <li>Data types and functions have been expanded to fully qualified identifiers.
 *   <li>Time attributes are represented in the column's data type.
 *   <li>{@link Expression}s have been translated to {@link ResolvedExpression}.
 *   <li>{@link AbstractDataType}s have been translated to {@link DataType}.
 * </ul>
 */
@PublicEvolving
public final class ResolvedSchema {

    private final List<Column> columns;
    private final List<WatermarkSpec> watermarkSpecs;
    private final @Nullable UniqueConstraint primaryKey;

    public ResolvedSchema(
            List<Column> columns,
            List<WatermarkSpec> watermarkSpecs,
            @Nullable UniqueConstraint primaryKey) {
        this.columns = Preconditions.checkNotNull(columns, "Columns must not be null.");
        this.watermarkSpecs =
                Preconditions.checkNotNull(watermarkSpecs, "Watermark specs must not be null.");
        this.primaryKey = primaryKey;
    }

    /** Shortcut for a resolved schema of only columns. */
    public static ResolvedSchema of(List<Column> columns) {
        return new ResolvedSchema(columns, Collections.emptyList(), null);
    }

    /** Shortcut for a resolved schema of only columns. */
    public static ResolvedSchema of(Column... columns) {
        return ResolvedSchema.of(Arrays.asList(columns));
    }

    /** Shortcut for a resolved schema of only physical columns. */
    public static ResolvedSchema physical(
            List<String> columnNames, List<DataType> columnDataTypes) {
        Preconditions.checkArgument(
                columnNames.size() == columnDataTypes.size(),
                "Mismatch between number of columns names and data types.");
        final List<Column> columns =
                IntStream.range(0, columnNames.size())
                        .mapToObj(i -> Column.physical(columnNames.get(i), columnDataTypes.get(i)))
                        .collect(Collectors.toList());
        return new ResolvedSchema(columns, Collections.emptyList(), null);
    }

    /** Shortcut for a resolved schema of only physical columns. */
    public static ResolvedSchema physical(String[] columnNames, DataType[] columnDataTypes) {
        return physical(Arrays.asList(columnNames), Arrays.asList(columnDataTypes));
    }

    /** Returns the number of {@link Column}s of this schema. */
    public int getColumnCount() {
        return columns.size();
    }

    /** Returns all {@link Column}s of this schema. */
    public List<Column> getColumns() {
        return columns;
    }

    /** Returns all column names. It does not distinguish between different kinds of columns. */
    public List<String> getColumnNames() {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    /**
     * Returns all column data types. It does not distinguish between different kinds of columns.
     */
    public List<DataType> getColumnDataTypes() {
        return columns.stream().map(Column::getDataType).collect(Collectors.toList());
    }

    /**
     * Returns the {@link Column} instance for the given column index.
     *
     * @param columnIndex the index of the column
     */
    public Optional<Column> getColumn(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            return Optional.empty();
        }
        return Optional.of(this.columns.get(columnIndex));
    }

    /**
     * Returns the {@link Column} instance for the given column name.
     *
     * @param columnName the name of the column
     */
    public Optional<Column> getColumn(String columnName) {
        return this.columns.stream()
                .filter(column -> column.getName().equals(columnName))
                .findFirst();
    }

    /**
     * Returns a list of watermark specifications each consisting of a rowtime attribute and
     * watermark strategy expression.
     *
     * <p>Note: Currently, there is at most one {@link WatermarkSpec} in the list, because we don't
     * support multiple watermark definitions yet.
     */
    public List<WatermarkSpec> getWatermarkSpecs() {
        return watermarkSpecs;
    }

    /** Returns the primary key if it has been defined. */
    public Optional<UniqueConstraint> getPrimaryKey() {
        return Optional.ofNullable(primaryKey);
    }

    /**
     * Converts all columns of this schema into a (possibly nested) row data type.
     *
     * <p>This method returns the <b>source-to-query schema</b>.
     *
     * <p>Note: The returned row data type contains physical, computed, and metadata columns. Be
     * careful when using this method in a table source or table sink. In many cases, {@link
     * #toPhysicalRowDataType()} might be more appropriate.
     *
     * @see DataTypes#ROW(DataTypes.Field...)
     * @see #toPhysicalRowDataType()
     * @see #toSinkRowDataType()
     */
    public DataType toSourceRowDataType() {
        final DataTypes.Field[] fields =
                columns.stream().map(ResolvedSchema::columnToField).toArray(DataTypes.Field[]::new);
        // the row should never be null
        return ROW(fields).notNull();
    }

    /**
     * Converts all physical columns of this schema into a (possibly nested) row data type.
     *
     * <p>Note: The returned row data type contains only physical columns. It does not include
     * computed or metadata columns.
     *
     * @see DataTypes#ROW(DataTypes.Field...)
     * @see #toSourceRowDataType()
     * @see #toSinkRowDataType()
     */
    public DataType toPhysicalRowDataType() {
        final DataTypes.Field[] fields =
                columns.stream()
                        .filter(Column::isPhysical)
                        .map(ResolvedSchema::columnToField)
                        .toArray(DataTypes.Field[]::new);
        // the row should never be null
        return ROW(fields).notNull();
    }

    /**
     * Converts all persisted columns of this schema into a (possibly nested) row data type.
     *
     * <p>This method returns the <b>query-to-sink schema</b>.
     *
     * <p>Note: Computed columns and virtual columns are excluded in the returned row data type. The
     * data type contains the columns of {@link #toPhysicalRowDataType()} plus persisted metadata
     * columns.
     *
     * @see DataTypes#ROW(DataTypes.Field...)
     * @see #toSourceRowDataType()
     * @see #toPhysicalRowDataType()
     */
    public DataType toSinkRowDataType() {
        final DataTypes.Field[] fields =
                columns.stream()
                        .filter(Column::isPersisted)
                        .map(ResolvedSchema::columnToField)
                        .toArray(DataTypes.Field[]::new);
        // the row should never be null
        return ROW(fields).notNull();
    }

    @Override
    public String toString() {
        final List<Object> components = new ArrayList<>();
        components.addAll(columns);
        components.addAll(watermarkSpecs);
        if (primaryKey != null) {
            components.add(primaryKey);
        }
        return components.stream()
                .map(Objects::toString)
                .map(s -> "  " + s)
                .collect(Collectors.joining(",\n", "(\n", "\n)"));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ResolvedSchema that = (ResolvedSchema) o;
        return Objects.equals(columns, that.columns)
                && Objects.equals(watermarkSpecs, that.watermarkSpecs)
                && Objects.equals(primaryKey, that.primaryKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns, watermarkSpecs, primaryKey);
    }

    // --------------------------------------------------------------------------------------------

    private static DataTypes.Field columnToField(Column column) {
        return FIELD(
                column.getName(),
                // only a column in a schema should have a time attribute,
                // a field should not propagate the attribute because it might be used in a
                // completely different context
                removeTimeAttribute(column.getDataType()));
    }
}
