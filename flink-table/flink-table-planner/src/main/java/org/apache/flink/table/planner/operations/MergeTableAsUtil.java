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

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedMetadataColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlRewriterUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** A utility class with logic for handling the {@code CREATE TABLE ... AS SELECT} clause. */
public class MergeTableAsUtil {
    private final SqlValidator validator;
    private final Function<SqlNode, String> escapeExpression;
    private final DataTypeFactory dataTypeFactory;

    public MergeTableAsUtil(
            SqlValidator validator,
            Function<SqlNode, String> escapeExpression,
            DataTypeFactory dataTypeFactory) {
        this.validator = validator;
        this.escapeExpression = escapeExpression;
        this.dataTypeFactory = dataTypeFactory;
    }

    /**
     * Rewrites the query operation to include only the fields that may be persisted in the sink.
     */
    public PlannerQueryOperation maybeRewriteQuery(
            CatalogManager catalogManager,
            FlinkPlannerImpl flinkPlanner,
            PlannerQueryOperation origQueryOperation,
            SqlNode origQueryNode,
            ResolvedCatalogTable sinkTable) {
        FlinkCalciteSqlValidator sqlValidator = flinkPlanner.getOrCreateSqlValidator();
        SqlRewriterUtils rewriterUtils = new SqlRewriterUtils(sqlValidator);
        FlinkTypeFactory typeFactory = (FlinkTypeFactory) sqlValidator.getTypeFactory();

        // Only fields that may be persisted will be included in the select query
        RowType sinkRowType =
                ((RowType) sinkTable.getResolvedSchema().toSinkRowDataType().getLogicalType());

        Map<String, Integer> sourceFields =
                IntStream.range(0, origQueryOperation.getResolvedSchema().getColumnNames().size())
                        .boxed()
                        .collect(
                                Collectors.toMap(
                                        origQueryOperation.getResolvedSchema().getColumnNames()
                                                ::get,
                                        Function.identity()));

        // assignedFields contains the new sink fields that are not present in the source
        // and that will be included in the select query
        LinkedHashMap<Integer, SqlNode> assignedFields = new LinkedHashMap<>();

        // targetPositions contains the positions of the source fields that will be
        // included in the select query
        List<Object> targetPositions = new ArrayList<>();

        int pos = -1;
        for (RowType.RowField targetField : sinkRowType.getFields()) {
            pos++;

            if (!sourceFields.containsKey(targetField.getName())) {
                if (!targetField.getType().isNullable()) {
                    throw new ValidationException(
                            "Column '"
                                    + targetField.getName()
                                    + "' has no default value and does not allow NULLs.");
                }

                assignedFields.put(
                        pos,
                        rewriterUtils.maybeCast(
                                SqlLiteral.createNull(SqlParserPos.ZERO),
                                typeFactory.createUnknownType(),
                                typeFactory.createFieldTypeFromLogicalType(targetField.getType()),
                                typeFactory));
            } else {
                targetPositions.add(sourceFields.get(targetField.getName()));
            }
        }

        // rewrite query
        SqlCall newSelect =
                rewriterUtils.rewriteCall(
                        rewriterUtils,
                        sqlValidator,
                        (SqlCall) origQueryNode,
                        typeFactory.buildRelNodeRowType(sinkRowType),
                        assignedFields,
                        targetPositions,
                        () -> "Unsupported node type " + origQueryNode.getKind());

        return (PlannerQueryOperation)
                SqlNodeToOperationConversion.convert(flinkPlanner, catalogManager, newSelect)
                        .orElseThrow(
                                () ->
                                        new TableException(
                                                "Unsupported node type "
                                                        + newSelect.getClass().getSimpleName()));
    }

    /**
     * Merges the specified schema with columns, watermark, and constraints with the {@code
     * sourceSchema}.
     *
     * <p>The resulted schema will contain the following elements:
     *
     * <ul>
     *   <li>columns
     *   <li>computed columns
     *   <li>metadata columns
     *   <li>watermarks
     *   <li>primary key
     * </ul>
     *
     * <p>It is expected that the {@code sourceSchema} contains only physical/regular columns.
     *
     * <p>Columns of the {@code sourceSchema} are appended to the schema columns defined in the
     * {@code sqlColumnList}. If a column in the {@code sqlColumnList} is already defined in the
     * {@code sourceSchema}, then the types of the columns are implicit cast and must be compatible
     * based on the implicit cast rules. If they're compatible, then the column position in the
     * schema stays the same as defined in the appended {@code sourceSchema}.
     */
    public Schema mergeSchemas(
            SqlNodeList sqlColumnList,
            @Nullable SqlWatermark sqlWatermark,
            List<SqlTableConstraint> sqlTableConstraints,
            ResolvedSchema sourceSchema) {
        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        dataTypeFactory,
                        validator,
                        escapeExpression);

        schemaBuilder.mergeColumns(
                sqlColumnList,
                Schema.newBuilder().fromResolvedSchema(sourceSchema).build().getColumns());

        if (sqlWatermark != null) {
            schemaBuilder.setWatermark(sqlWatermark);
        }

        // It is assumed only a primary key constraint may be defined in the table. The
        // SqlCreateTableAs has validations to ensure this before the object is created.
        Optional<SqlTableConstraint> primaryKey =
                sqlTableConstraints.stream().filter(SqlTableConstraint::isPrimaryKey).findAny();

        if (primaryKey.isPresent()) {
            schemaBuilder.setPrimaryKey(primaryKey.get());
        }

        return schemaBuilder.build();
    }

    /** Reorders the columns from the source schema based on the columns identifiers list. */
    public Schema reorderSchema(SqlNodeList sqlColumnList, ResolvedSchema sourceSchema) {
        SchemaBuilder schemaBuilder =
                new SchemaBuilder(
                        (FlinkTypeFactory) validator.getTypeFactory(),
                        dataTypeFactory,
                        validator,
                        escapeExpression);

        schemaBuilder.reorderColumns(
                sqlColumnList,
                Schema.newBuilder().fromResolvedSchema(sourceSchema).build().getColumns());

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

        /** Reorders the columns from the source schema based on the columns identifiers list. */
        private void reorderColumns(List<SqlNode> identifiers, List<UnresolvedColumn> sourceCols) {
            Map<String, UnresolvedColumn> sinkSchemaCols = new LinkedHashMap<>();
            Map<String, UnresolvedColumn> sourceSchemaCols = new LinkedHashMap<>();

            populateColumnsFromSource(sourceCols, sourceSchemaCols);

            if (identifiers.size() != sourceCols.size()) {
                throw new ValidationException(
                        "The number of columns in the column list must match the number "
                                + "of columns in the source schema.");
            }

            for (SqlNode identifier : identifiers) {
                String name = ((SqlIdentifier) identifier).getSimple();
                if (!sourceSchemaCols.containsKey(name)) {
                    throw new ValidationException(
                            String.format("Column '%s' not found in the source schema. ", name));
                }

                sinkSchemaCols.put(name, sourceSchemaCols.get(name));
            }

            columns.clear();
            columns.putAll(sinkSchemaCols);
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
            LogicalType sinkColumnType;

            if (sinkColumn instanceof UnresolvedPhysicalColumn) {
                sinkColumnType = getLogicalType(((UnresolvedPhysicalColumn) sinkColumn));
            } else if ((sinkColumn instanceof UnresolvedMetadataColumn)) {
                if (((UnresolvedMetadataColumn) sinkColumn).isVirtual()) {
                    throw new ValidationException(
                            String.format(
                                    "A column named '%s' already exists in the source schema. "
                                            + "Virtual metadata columns cannot overwrite "
                                            + "columns from source.",
                                    columnName));
                }

                sinkColumnType = getLogicalType(((UnresolvedMetadataColumn) sinkColumn));
            } else {
                throw new ValidationException(
                        String.format(
                                "A column named '%s' already exists in the source schema. "
                                        + "Computed columns cannot overwrite columns from source.",
                                columnName));
            }

            LogicalType sourceColumnType =
                    getLogicalType(((UnresolvedPhysicalColumn) sourceColumn));
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
