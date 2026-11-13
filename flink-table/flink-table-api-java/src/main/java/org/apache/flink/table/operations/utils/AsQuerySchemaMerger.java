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

package org.apache.flink.table.operations.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/**
 * Merges the declared columns of a {@code CREATE TABLE}, {@code [CREATE OR] REPLACE TABLE} or
 * {@code CREATE [OR ALTER] MATERIALIZED TABLE} statement with an {@code AS <query>} clause onto the
 * columns of its query.
 */
@Internal
public final class AsQuerySchemaMerger {

    private final DataTypeFactory dataTypeFactory;
    private final Map<String, UnresolvedColumn> declaredOnlyColumns = new LinkedHashMap<>();
    private final Map<String, UnresolvedColumn> sourceColumns = new LinkedHashMap<>();

    /**
     * The source columns are expected to be physical only, as produced by a query.
     *
     * @throws ValidationException if a source column is not physical, or two source columns share a
     *     name
     */
    public AsQuerySchemaMerger(
            List<UnresolvedColumn> sourceColumns, DataTypeFactory dataTypeFactory) {
        this.dataTypeFactory = dataTypeFactory;
        for (final UnresolvedColumn column : sourceColumns) {
            if (!(column instanceof UnresolvedPhysicalColumn)) {
                throw new ValidationException(
                        "Computed columns and metadata columns are not expected "
                                + "in the source schema.");
            }
            if (this.sourceColumns.containsKey(column.getName())) {
                throw new ValidationException(
                        String.format(
                                "A column named '%s' already exists in the schema. ",
                                column.getName()));
            }
            this.sourceColumns.put(column.getName(), column);
        }
    }

    /** Rejects a name an earlier declared-only column already uses. */
    public void checkNotDeclared(String columnName) {
        if (declaredOnlyColumns.containsKey(columnName)) {
            throw new ValidationException(
                    String.format(
                            "A column named '%s' already exists in the schema. ", columnName));
        }
    }

    /**
     * Adds a declared column.
     *
     * @param position the column's 0-based index in the declaration, used only in error messages
     * @throws ValidationException if {@link #checkNotDeclared} rejects the name, or the column
     *     cannot replace the query column of the same name
     */
    public void addDeclaredColumn(UnresolvedColumn column, int position) {
        final String name = column.getName();
        checkNotDeclared(name);
        if (sourceColumns.containsKey(name)) {
            validateImplicitCastCompatibility(
                    dataTypeFactory, name, position, sourceColumns.get(name), column);
            // Replacing the value keeps the query column's position in the LinkedHashMap.
            sourceColumns.put(name, column);
        } else {
            declaredOnlyColumns.put(name, column);
        }
    }

    /** Returns the declared-only columns followed by the (possibly replaced) query columns. */
    public List<UnresolvedColumn> getMergedColumns() {
        final List<UnresolvedColumn> merged = new ArrayList<>(declaredOnlyColumns.values());
        merged.addAll(sourceColumns.values());
        return merged;
    }

    private static void validateImplicitCastCompatibility(
            DataTypeFactory dataTypeFactory,
            String columnName,
            int columnPos,
            UnresolvedColumn sourceColumn,
            UnresolvedColumn sinkColumn) {
        final LogicalType sinkColumnType;

        if (sinkColumn instanceof UnresolvedPhysicalColumn) {
            sinkColumnType =
                    getLogicalType(
                            dataTypeFactory, ((UnresolvedPhysicalColumn) sinkColumn).getDataType());
        } else if ((sinkColumn instanceof UnresolvedMetadataColumn)) {
            if (((UnresolvedMetadataColumn) sinkColumn).isVirtual()) {
                throw new ValidationException(
                        String.format(
                                "A column named '%s' already exists in the source schema. "
                                        + "Virtual metadata columns cannot overwrite "
                                        + "columns from source.",
                                columnName));
            }

            sinkColumnType =
                    getLogicalType(
                            dataTypeFactory, ((UnresolvedMetadataColumn) sinkColumn).getDataType());
        } else {
            throw new ValidationException(
                    String.format(
                            "A column named '%s' already exists in the source schema. "
                                    + "Computed columns cannot overwrite columns from source.",
                            columnName));
        }

        final LogicalType sourceColumnType =
                getLogicalType(
                        dataTypeFactory, ((UnresolvedPhysicalColumn) sourceColumn).getDataType());
        if (!supportsImplicitCast(sourceColumnType, sinkColumnType)) {
            throw new ValidationException(
                    String.format(
                            "Incompatible types for sink column '%s' at position %d. "
                                    + "The source column has type '%s', "
                                    + "while the target column has type '%s'.",
                            columnName, columnPos + 1, sourceColumnType, sinkColumnType));
        }
    }

    private static LogicalType getLogicalType(
            DataTypeFactory dataTypeFactory, AbstractDataType<?> dataType) {
        return dataTypeFactory.createDataType(dataType).getLogicalType();
    }
}
