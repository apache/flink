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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** A utility class with logic for handling the {@code CREATE TABLE ... AS SELECT} clause. */
public class MergeTableAsUtil {
    private final SqlValidator validator;
    private final Function<SqlNode, String> escapeExpression;
    private final DataTypeFactory dataTypeFactory;

    MergeTableAsUtil(
            SqlValidator validator,
            Function<SqlNode, String> escapeExpression,
            DataTypeFactory dataTypeFactory) {
        this.validator = validator;
        this.escapeExpression = escapeExpression;
        this.dataTypeFactory = dataTypeFactory;
    }

    /**
     * Merges the schema part of the {@code sqlCreateTableAs} with the {@code sourceSchema}.
     *
     * <p>The schema part of the {@code CREATE TABLE} statement merged includes:
     *
     * <ul>
     *   <li>columns
     *   <li>computed columns
     *   <li>metadata columns
     *   <li>watermarks
     *   <li>primary key
     * </ul>
     *
     * <p>It is expected that the {@code sourceSchema} contains only physical/regular columns, which
     * is behavior of the CTAS statement to generate such schema.
     *
     * <p>Columns of {@code sourceSchema} are appended to the schema of {@code sqlCreateTableAs}. If
     * a column in the {@code sqlCreateTableAs} is already defined in {@code sourceSchema}, then the
     * types of the columns are implicit cast and must be compatible based on the implicit cast
     * rules. If they're compatible, then the column position in the schema stays the same as
     * defined in the appended {@code sourceSchema}.
     */
    public Schema mergeSchemas(SqlCreateTableAs sqlCreateTableAs, ResolvedSchema sourceSchema) {
        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        dataTypeFactory,
                        validator,
                        escapeExpression);

        schemaBuilder.mergeColumns(
                sqlCreateTableAs.getColumnList(),
                Schema.newBuilder().fromResolvedSchema(sourceSchema).build().getColumns());

        if (sqlCreateTableAs.getWatermark().isPresent()) {
            schemaBuilder.setWatermark(sqlCreateTableAs.getWatermark().get());
        }

        // It is assumed only a primary key constraint may be defined in the table. The
        // SqlCreateTableAs has validations to ensure this before the object is created.
        Optional<SqlTableConstraint> primaryKey =
                sqlCreateTableAs.getFullConstraints().stream()
                        .filter(SqlTableConstraint::isPrimaryKey)
                        .findAny();

        if (primaryKey.isPresent()) {
            schemaBuilder.setPrimaryKey(primaryKey.get());
        }

        return schemaBuilder.build();
    }

    /**
     * Builder class for constructing a {@link Schema} based on the rules of the {@code CREATE TABLE
     * ... AS SELECT} statement.
     */
    private static class SchemaBuilder extends SchemaBuilderUtil {
        // Mapping required when evaluating compute expressions and watermark columns.
        private Map<String, RelDataType> regularAndMetadataFieldNamesToTypes =
                new LinkedHashMap<>();
        private Map<String, RelDataType> computeFieldNamesToTypes = new LinkedHashMap<>();

        FlinkTypeFactory typeFactory;

        SchemaBuilder(
                FlinkTypeFactory typeFactory,
                DataTypeFactory dataTypeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions) {
            super(sqlValidator, escapeExpressions, dataTypeFactory);
            this.typeFactory = typeFactory;
        }

        /**
         * Merges the sink columns with the source columns. The resulted schema will contain columns
         * of the sink schema first, followed by the columns of the source schema.
         *
         * <p>If a column in the sink schema is already defined in the source schema, then the types
         * of the columns overrides the types of the columns in the source schema. The column
         * position in the schema stays the same as defined in the source schema.
         *
         * <p>Column types overridden follows the same implicit cast rules defined for INSERT INTO
         * statements.
         */
        private void mergeColumns(List<SqlNode> sinkCols, List<UnresolvedColumn> sourceCols) {
            Map<String, UnresolvedColumn> sinkSchemaCols = new LinkedHashMap<>();
            Map<String, UnresolvedColumn> sourceSchemaCols = new LinkedHashMap<>();

            populateColumnsFromSource(sourceCols, sourceSchemaCols);

            int sinkColumnPos = -1;
            for (SqlNode sinkColumn : sinkCols) {
                String name = ((SqlTableColumn) sinkColumn).getName().getSimple();
                sinkColumnPos++;

                if (sinkSchemaCols.containsKey(name)) {
                    throw new ValidationException(
                            String.format(
                                    "A column named '%s' already exists in the schema. ", name));
                }

                UnresolvedColumn unresolvedSinkColumn;

                if (sinkColumn instanceof SqlRegularColumn) {
                    unresolvedSinkColumn =
                            toUnresolvedPhysicalColumn((SqlRegularColumn) sinkColumn);

                    regularAndMetadataFieldNamesToTypes.put(
                            name, toRelDataType(((SqlRegularColumn) sinkColumn).getType()));
                } else if (sinkColumn instanceof SqlMetadataColumn) {
                    unresolvedSinkColumn =
                            toUnresolvedMetadataColumn((SqlMetadataColumn) sinkColumn);

                    regularAndMetadataFieldNamesToTypes.put(
                            name, toRelDataType(((SqlMetadataColumn) sinkColumn).getType()));
                } else if (sinkColumn instanceof SqlComputedColumn) {
                    final SqlNode validatedExpr =
                            sqlValidator.validateParameterizedExpression(
                                    ((SqlComputedColumn) sinkColumn).getExpr(),
                                    regularAndMetadataFieldNamesToTypes);

                    unresolvedSinkColumn =
                            toUnresolvedComputedColumn(
                                    (SqlComputedColumn) sinkColumn, validatedExpr);

                    computeFieldNamesToTypes.put(
                            name, sqlValidator.getValidatedNodeType(validatedExpr));
                } else {
                    throw new ValidationException("Unsupported column type: " + sinkColumn);
                }

                if (sourceSchemaCols.containsKey(name)) {
                    // If the column is already defined in the source schema, then check if
                    // the types are compatible.
                    validateImplicitCastCompatibility(
                            name, sinkColumnPos, sourceSchemaCols.get(name), unresolvedSinkColumn);

                    // Replace the source schema column with the new sink schema column, which
                    // keeps the position of the source schema column but with the data type
                    // of the sink column.
                    sourceSchemaCols.put(name, unresolvedSinkColumn);
                } else {
                    sinkSchemaCols.put(name, unresolvedSinkColumn);
                }
            }

            columns.clear();
            columns.putAll(sinkSchemaCols);
            columns.putAll(sourceSchemaCols);
        }

        /**
         * Populates the schema columns from the source schema. The source schema is expected to
         * contain only physical columns.
         */
        private void populateColumnsFromSource(
                List<UnresolvedColumn> columns, Map<String, UnresolvedColumn> schemaCols) {
            for (UnresolvedColumn column : columns) {
                if (!(column instanceof UnresolvedPhysicalColumn)) {
                    throw new ValidationException(
                            "Computed columns and metadata columns are not expected "
                                    + "in the source schema.");
                }

                if (schemaCols.containsKey(column.getName())) {
                    throw new ValidationException(
                            String.format(
                                    "A column named '%s' already exists in the schema. ",
                                    column.getName()));
                }

                String name = column.getName();
                LogicalType sourceColumnType = getLogicalType(((UnresolvedPhysicalColumn) column));

                schemaCols.put(column.getName(), column);
                regularAndMetadataFieldNamesToTypes.put(
                        name, typeFactory.createFieldTypeFromLogicalType(sourceColumnType));
            }
        }

        private void validateImplicitCastCompatibility(
                String columnName,
                int columnPos,
                UnresolvedColumn sourceColumn,
                UnresolvedColumn sinkColumn) {
            if (!(sinkColumn instanceof UnresolvedPhysicalColumn)) {
                throw new ValidationException(
                        String.format(
                                "A column named '%s' already exists in the source schema. "
                                        + "Computed and metadata columns cannot overwrite "
                                        + "regular columns.",
                                columnName));
            }

            LogicalType sourceColumnType =
                    getLogicalType(((UnresolvedPhysicalColumn) sourceColumn));
            LogicalType sinkColumnType = getLogicalType(((UnresolvedPhysicalColumn) sinkColumn));

            if (!supportsImplicitCast(sourceColumnType, sinkColumnType)) {
                throw new ValidationException(
                        String.format(
                                "Incompatible types for sink column '%s' at position %d. "
                                        + "The source column has type '%s', "
                                        + "while the target column has type '%s'.",
                                columnName, columnPos, sourceColumnType, sinkColumnType));
            }
        }

        private void setWatermark(SqlWatermark sqlWatermark) {
            Map<String, RelDataType> accessibleFieldNamesToTypes =
                    new LinkedHashMap() {
                        {
                            putAll(regularAndMetadataFieldNamesToTypes);
                            putAll(computeFieldNamesToTypes);
                        }
                    };

            addWatermarks(
                    Collections.singletonList(sqlWatermark), accessibleFieldNamesToTypes, false);
        }
    }
}
