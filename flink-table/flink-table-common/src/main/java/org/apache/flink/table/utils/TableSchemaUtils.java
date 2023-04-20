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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utilities to {@link TableSchema}. */
@Internal
public class TableSchemaUtils {

    /**
     * Return {@link TableSchema} which consists of all physical columns. That means, the computed
     * columns and metadata columns are filtered out.
     *
     * <p>Readers(or writers) such as {@link TableSource} and {@link TableSink} should use this
     * physical schema to generate {@link TableSource#getProducedDataType()} and {@link
     * TableSource#getTableSchema()} rather than using the raw TableSchema which may contains
     * additional columns.
     */
    public static TableSchema getPhysicalSchema(TableSchema tableSchema) {
        return getTableSchema(tableSchema, TableColumn::isPhysical);
    }

    /**
     * Return {@link TableSchema} which consists of all persisted columns. That means, the virtual
     * computed columns and metadata columns are filtered out.
     *
     * <p>Its difference from {@link TableSchemaUtils#getPhysicalSchema(TableSchema)} is that it
     * includes of all physical columns and metadata columns without virtual keyword.
     */
    public static TableSchema getPersistedSchema(TableSchema tableSchema) {
        return getTableSchema(tableSchema, TableColumn::isPersisted);
    }

    /** Build a {@link TableSchema} with columns filtered by a given columnFilter. */
    private static TableSchema getTableSchema(
            TableSchema tableSchema, Function<TableColumn, Boolean> columnFilter) {
        Preconditions.checkNotNull(tableSchema);
        TableSchema.Builder builder = new TableSchema.Builder();
        tableSchema
                .getTableColumns()
                .forEach(
                        tableColumn -> {
                            if (columnFilter.apply(tableColumn)) {
                                builder.field(tableColumn.getName(), tableColumn.getType());
                            }
                        });
        tableSchema
                .getPrimaryKey()
                .ifPresent(
                        uniqueConstraint ->
                                builder.primaryKey(
                                        uniqueConstraint.getName(),
                                        uniqueConstraint.getColumns().toArray(new String[0])));
        return builder.build();
    }

    /** Returns true if there are only physical columns in the given {@link TableSchema}. */
    public static boolean containsPhysicalColumnsOnly(TableSchema schema) {
        Preconditions.checkNotNull(schema);
        return schema.getTableColumns().stream().allMatch(TableColumn::isPhysical);
    }

    /** Throws an exception if the given {@link TableSchema} contains any non-physical columns. */
    public static TableSchema checkOnlyPhysicalColumns(TableSchema schema) {
        Preconditions.checkNotNull(schema);
        if (!containsPhysicalColumnsOnly(schema)) {
            throw new ValidationException(
                    "The given schema contains non-physical columns, schema: \n"
                            + schema.toString());
        }
        return schema;
    }

    /**
     * Returns the field indices of primary key in the physical columns of this schema (not include
     * computed columns or metadata columns).
     */
    public static int[] getPrimaryKeyIndices(TableSchema schema) {
        if (schema.getPrimaryKey().isPresent()) {
            List<String> fieldNames = DataTypeUtils.flattenToNames(schema.toPhysicalRowDataType());
            return schema.getPrimaryKey().get().getColumns().stream()
                    .mapToInt(fieldNames::indexOf)
                    .toArray();
        } else {
            return new int[0];
        }
    }

    /** Removes time attributes from the {@link ResolvedSchema}. */
    public static ResolvedSchema removeTimeAttributeFromResolvedSchema(
            ResolvedSchema resolvedSchema) {
        return new ResolvedSchema(
                resolvedSchema.getColumns().stream()
                        .map(col -> col.copy(DataTypeUtils.removeTimeAttribute(col.getDataType())))
                        .collect(Collectors.toList()),
                resolvedSchema.getWatermarkSpecs(),
                resolvedSchema.getPrimaryKey().orElse(null));
    }

    /**
     * Creates a builder with given table schema.
     *
     * @param oriSchema Original schema
     * @return the builder with all the information from the given schema
     */
    public static TableSchema.Builder builderWithGivenSchema(TableSchema oriSchema) {
        TableSchema.Builder builder = builderWithGivenColumns(oriSchema.getTableColumns());
        // Copy watermark specification.
        for (WatermarkSpec wms : oriSchema.getWatermarkSpecs()) {
            builder.watermark(
                    wms.getRowtimeAttribute(),
                    wms.getWatermarkExpr(),
                    wms.getWatermarkExprOutputType());
        }
        // Copy primary key constraint.
        oriSchema
                .getPrimaryKey()
                .map(
                        pk ->
                                builder.primaryKey(
                                        pk.getName(), pk.getColumns().toArray(new String[0])));
        return builder;
    }

    /** Creates a new schema but drop the constraint with given name. */
    public static TableSchema dropConstraint(TableSchema oriSchema, String constraintName) {
        // Validate the constraint name is valid.
        Optional<UniqueConstraint> uniqueConstraintOpt = oriSchema.getPrimaryKey();
        if (!uniqueConstraintOpt.isPresent()
                || !uniqueConstraintOpt.get().getName().equals(constraintName)) {
            throw new ValidationException(
                    String.format("Constraint %s to drop does not exist", constraintName));
        }
        TableSchema.Builder builder = builderWithGivenColumns(oriSchema.getTableColumns());
        // Copy watermark specification.
        for (WatermarkSpec wms : oriSchema.getWatermarkSpecs()) {
            builder.watermark(
                    wms.getRowtimeAttribute(),
                    wms.getWatermarkExpr(),
                    wms.getWatermarkExprOutputType());
        }
        return builder.build();
    }

    /** Returns the builder with copied columns info from the given table schema. */
    private static TableSchema.Builder builderWithGivenColumns(List<TableColumn> originalColumns) {
        final TableSchema.Builder builder = TableSchema.builder();
        for (TableColumn column : originalColumns) {
            builder.add(column);
        }
        return builder;
    }
}
