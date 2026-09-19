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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlComputedColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.calcite.FlinkCalciteSqlValidator;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RelBuilder;

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

/** A utility class with logic for handling the {@code CREATE TABLE ... AS SELECT} clause. */
public class MergeTableAsUtil {
    private final FlinkCalciteSqlValidator validator;
    private final Function<SqlNode, String> escapeExpression;
    private final DataTypeFactory dataTypeFactory;

    public MergeTableAsUtil(
            FlinkCalciteSqlValidator validator,
            Function<SqlNode, String> escapeExpression,
            DataTypeFactory dataTypeFactory) {
        this.validator = validator;
        this.escapeExpression = escapeExpression;
        this.dataTypeFactory = dataTypeFactory;
    }

    public MergeTableAsUtil(ConvertContext context) {
        this(
                context.getSqlValidator(),
                context::toQuotedSqlString,
                context.getCatalogManager().getDataTypeFactory());
    }

    /**
     * Reshapes the query so its output columns line up with the sink's persistable columns:
     * reordering them and filling sink columns the query does not produce with {@code NULL}.
     * Returns the query unchanged when it already matches the sink 1:1. A sink column the query
     * does not produce that is declared {@code NOT NULL} raises a {@link ValidationException}.
     */
    public PlannerQueryOperation maybeRewriteQuery(
            PlannerQueryOperation origQueryOperation,
            SqlNode origQueryNode,
            ResolvedCatalogBaseTable<?> sinkTable) {
        final RelNode queryRelNode = origQueryOperation.getCalciteTree();
        final RelOptCluster cluster = queryRelNode.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final FlinkTypeFactory typeFactory = (FlinkTypeFactory) cluster.getTypeFactory();

        // Only fields that may be persisted are included in the sink.
        final RowType sinkRowType =
                (RowType) sinkTable.getResolvedSchema().toSinkRowDataType().getLogicalType();

        final List<String> sourceColumns = origQueryOperation.getResolvedSchema().getColumnNames();
        final Map<String, Integer> sourceFields =
                IntStream.range(0, sourceColumns.size())
                        .boxed()
                        .collect(Collectors.toMap(sourceColumns::get, Function.identity()));

        final List<RexNode> projects = new ArrayList<>();
        final List<String> fieldNames = new ArrayList<>();
        // The projection is a no-op when the query already produces the sink columns 1:1 in order.
        boolean rewriteNeeded = sinkRowType.getFieldCount() != sourceColumns.size();

        // The loop cannot stop once a rewrite is detected: the projection must cover every sink
        // field, and every missing NOT NULL column must still be validated.
        int pos = -1;
        for (RowType.RowField targetField : sinkRowType.getFields()) {
            pos++;
            fieldNames.add(targetField.getName());

            final Integer sourcePos = sourceFields.get(targetField.getName());
            if (sourcePos == null) {
                if (!targetField.getType().isNullable()) {
                    throw new ValidationException(
                            "Column '"
                                    + targetField.getName()
                                    + "' has no default value and does not allow NULLs.");
                }
                projects.add(
                        rexBuilder.makeNullLiteral(
                                typeFactory.createFieldTypeFromLogicalType(targetField.getType())));
                rewriteNeeded = true;
            } else {
                projects.add(rexBuilder.makeInputRef(queryRelNode, sourcePos));
                if (sourcePos != pos) {
                    rewriteNeeded = true;
                }
            }
        }

        if (!rewriteNeeded) {
            return origQueryOperation;
        }

        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
        final RelNode projected =
                relBuilder.push(queryRelNode).project(projects, fieldNames, true).build();
        return new PlannerQueryOperation(projected, () -> escapeExpression.apply(origQueryNode));
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

        primaryKey.ifPresent(schemaBuilder::setPrimaryKey);

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
        private final Map<String, RelDataType> regularAndMetadataFieldNamesToTypes =
                new LinkedHashMap<>();
        private final Map<String, RelDataType> computeFieldNamesToTypes = new LinkedHashMap<>();

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
                final SqlTableColumn column = toSqlTableColumn(sinkColumn);
                String name = column.getName().getSimple();
                sinkColumnPos++;

                if (sinkSchemaCols.containsKey(name)) {
                    throw new ValidationException(
                            String.format(
                                    "A column named '%s' already exists in the schema. ", name));
                }

                final UnresolvedColumn unresolvedSinkColumn;
                final RelDataType relDataType;
                if (column instanceof SqlRegularColumn) {
                    unresolvedSinkColumn = toUnresolvedPhysicalColumn((SqlRegularColumn) column);
                    relDataType = toRelDataType(((SqlRegularColumn) column).getType());
                } else if (column instanceof SqlMetadataColumn) {
                    unresolvedSinkColumn = toUnresolvedMetadataColumn((SqlMetadataColumn) column);
                    relDataType = toRelDataType(((SqlMetadataColumn) column).getType());
                } else if (column instanceof SqlComputedColumn) {
                    final SqlComputedColumn computedColumn = (SqlComputedColumn) column;
                    final SqlNode validatedExpr =
                            sqlValidator.validateParameterizedExpression(
                                    computedColumn.getExpr(), regularAndMetadataFieldNamesToTypes);

                    unresolvedSinkColumn =
                            toUnresolvedComputedColumn(computedColumn, validatedExpr);
                    relDataType = sqlValidator.getValidatedNodeType(validatedExpr);
                } else {
                    throw new ValidationException("Unsupported column type: " + column);
                }

                regularAndMetadataFieldNamesToTypes.put(name, relDataType);

                if (sourceSchemaCols.containsKey(name)) {
                    // If the column is already defined in the source schema, then check if
                    // the types are compatible.
                    validateImplicitCastCompatibility(
                            dataTypeFactory,
                            name,
                            sinkColumnPos,
                            sourceSchemaCols.get(name),
                            unresolvedSinkColumn);

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

        private SqlTableColumn toSqlTableColumn(SqlNode sinkColumn) {
            if (sinkColumn instanceof SqlTableColumn) {
                return (SqlTableColumn) sinkColumn;
            } else {
                return ((SqlTableColumnPosition) sinkColumn).getColumn();
            }
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
                            String.format("Column '%s' not found in the source schema.", name));
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
                LogicalType sourceColumnType =
                        getLogicalType(dataTypeFactory, ((UnresolvedPhysicalColumn) column));

                schemaCols.put(column.getName(), column);
                regularAndMetadataFieldNamesToTypes.put(
                        name, typeFactory.createFieldTypeFromLogicalType(sourceColumnType));
            }
        }

        private void setWatermark(SqlWatermark sqlWatermark) {
            final Map<String, RelDataType> accessibleFieldNamesToTypes =
                    new LinkedHashMap<>(regularAndMetadataFieldNamesToTypes);
            accessibleFieldNamesToTypes.putAll(computeFieldNamesToTypes);

            addWatermarks(
                    Collections.singletonList(sqlWatermark), accessibleFieldNamesToTypes, false);
        }
    }
}
