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

import org.apache.flink.sql.parser.SqlConstraintValidator;
import org.apache.flink.sql.parser.ddl.SqlCreateOrAlterMaterializedTable;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.Builder;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableChange.MaterializedTableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** A converter for {@link SqlCreateOrAlterMaterializedTable}. */
public class SqlCreateOrAlterMaterializedTableConverter
        extends AbstractCreateMaterializedTableConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                this.getIdentifier(sqlCreateOrAlterMaterializedTable, context);
        return sqlCreateOrAlterMaterializedTable.isOrAlter() && tableExists(context, identifier)
                ? handleAlter(sqlCreateOrAlterMaterializedTable, context)
                : handleCreate(sqlCreateOrAlterMaterializedTable, context, identifier);
    }

    private Operation handleAlter(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                this.getIdentifier(sqlCreateOrAlterMaterializedTable, context);
        final ResolvedCatalogMaterializedTable oldTable =
                getExistingResolvedMaterializedTable(context, identifier);

        final CatalogMaterializedTable newTable =
                buildNewCatalogMaterializedTableFromOldTable(
                        oldTable, sqlCreateOrAlterMaterializedTable, context);

        List<MaterializedTableChange> tableChanges =
                buildTableChanges(sqlCreateOrAlterMaterializedTable, oldTable, context);

        return new AlterMaterializedTableAsQueryOperation(identifier, tableChanges, newTable);
    }

    private Operation handleCreate(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context,
            ObjectIdentifier identifier) {
        final ResolvedCatalogMaterializedTable resolvedCatalogMaterializedTable =
                this.getResolvedCatalogMaterializedTable(
                        sqlCreateOrAlterMaterializedTable, context);

        return new CreateMaterializedTableOperation(identifier, resolvedCatalogMaterializedTable);
    }

    private List<MaterializedTableChange> buildTableChanges(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ResolvedCatalogMaterializedTable oldTable,
            ConvertContext context) {
        List<MaterializedTableChange> changes = new ArrayList<>();
        final MergeContext mergeContext =
                this.getMergeContext(sqlCreateOrAlterMaterializedTable, context);

        // Extract new columns
        ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        List<Column> newColumns =
                MaterializedTableUtils.validateAndExtractNewColumns(
                        oldSchema, mergeContext.getMergedQuerySchema());

        newColumns.forEach(column -> changes.add(TableChange.add(column)));
        changes.add(TableChange.modifyDefinitionQuery(mergeContext.getMergedExpandedQuery()));

        return changes;
    }

    private ResolvedCatalogMaterializedTable getExistingResolvedMaterializedTable(
            ConvertContext context, ObjectIdentifier identifier) {
        ResolvedCatalogBaseTable<?> baseTable =
                context.getCatalogManager().getTableOrError(identifier).getResolvedTable();
        if (TableKind.MATERIALIZED_TABLE != baseTable.getTableKind()) {
            throw new ValidationException(
                    "Only materialized table support modify definition query.");
        }
        return (ResolvedCatalogMaterializedTable) baseTable;
    }

    private boolean tableExists(ConvertContext context, ObjectIdentifier identifier) {
        return context.getCatalogManager().getTable(identifier).isPresent();
    }

    private CatalogMaterializedTable buildNewCatalogMaterializedTableFromOldTable(
            ResolvedCatalogMaterializedTable oldTable,
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        Schema.Builder schemaBuilder =
                Schema.newBuilder().fromResolvedSchema(oldTable.getResolvedSchema());

        // Add new columns if this is an alter operation
        ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final MergeContext mergeContext =
                this.getMergeContext(sqlCreateOrAlterMaterializedTable, context);
        List<Column> newColumns =
                MaterializedTableUtils.validateAndExtractNewColumns(
                        oldSchema, mergeContext.getMergedQuerySchema());
        newColumns.forEach(col -> schemaBuilder.column(col.getName(), col.getDataType()));

        final String comment = this.getComment(sqlCreateOrAlterMaterializedTable);
        final IntervalFreshness freshness =
                this.getDerivedFreshness(sqlCreateOrAlterMaterializedTable);
        final LogicalRefreshMode logicalRefreshMode =
                this.getDerivedLogicalRefreshMode(sqlCreateOrAlterMaterializedTable);
        final RefreshMode refreshMode = this.getDerivedRefreshMode(logicalRefreshMode);

        CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(schemaBuilder.build())
                        .comment(comment)
                        .distribution(mergeContext.getMergedTableDistribution().orElse(null))
                        .partitionKeys(mergeContext.getMergedPartitionKeys())
                        .options(mergeContext.getMergedTableOptions())
                        .originalQuery(mergeContext.getMergedOriginalQuery())
                        .expandedQuery(mergeContext.getMergedExpandedQuery())
                        .freshness(freshness)
                        .logicalRefreshMode(logicalRefreshMode)
                        .refreshMode(refreshMode)
                        .refreshStatus(RefreshStatus.INITIALIZING);

        // Preserve refresh handler from old table
        oldTable.getRefreshHandlerDescription().ifPresent(builder::refreshHandlerDescription);
        builder.serializedRefreshHandler(oldTable.getSerializedRefreshHandler());

        return builder.build();
    }

    @Override
    protected MergeContext getMergeContext(
            final SqlCreateOrAlterMaterializedTable sqlCreateMaterializedTable,
            final ConvertContext context) {
        return new MergeContext() {

            // Cache original query. If we call getDerivedExpandedQuery() first, without storing the
            // original query, it the SqlNode will be changed and the getAsQuery() will always
            // return the expanded query.
            private final String originalQuery =
                    SqlCreateOrAlterMaterializedTableConverter.this.getDerivedOriginalQuery(
                            sqlCreateMaterializedTable, context);

            private final ResolvedSchema querySchema =
                    SqlCreateOrAlterMaterializedTableConverter.this.getQueryResolvedSchema(
                            sqlCreateMaterializedTable, context);

            @Override
            public Schema getMergedSchema() {
                final Builder schemaBuilder = Schema.newBuilder().fromResolvedSchema(querySchema);
                sqlCreateMaterializedTable
                        .getTableConstraint()
                        .ifPresent(
                                constraint ->
                                        verifyAndBuildPrimaryKey(
                                                schemaBuilder, querySchema, constraint));
                return schemaBuilder.build();
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedTableOptions(
                        sqlCreateMaterializedTable);
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedPartitionKeys(
                        sqlCreateMaterializedTable);
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedTableDistribution(
                        sqlCreateMaterializedTable);
            }

            @Override
            public String getMergedOriginalQuery() {
                return originalQuery;
            }

            @Override
            public String getMergedExpandedQuery() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedExpandedQuery(
                        sqlCreateMaterializedTable, context);
            }

            @Override
            public ResolvedSchema getMergedQuerySchema() {
                return querySchema;
            }
        };
    }

    private void verifyAndBuildPrimaryKey(
            Schema.Builder schemaBuilder,
            ResolvedSchema resolvedSchema,
            SqlTableConstraint sqlTableConstraint) {
        // Validate constraint type
        try {
            SqlConstraintValidator.validate(sqlTableConstraint);
        } catch (SqlValidateException e) {
            throw new ValidationException(
                    String.format("Primary key validation failed: %s.", e.getMessage()), e);
        }

        List<String> primaryKeyColumns = Arrays.asList(sqlTableConstraint.getColumnNames());

        // Verify primary key columns exist and are not nullable
        for (String columnName : primaryKeyColumns) {
            Optional<Column> columnOptional = resolvedSchema.getColumn(columnName);
            if (columnOptional.isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "Primary key column '%s' not defined in the query schema. Available columns: [%s].",
                                columnName,
                                resolvedSchema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            if (columnOptional.get().getDataType().getLogicalType().isNullable()) {
                throw new ValidationException(
                        String.format(
                                "Could not create a PRIMARY KEY with nullable column '%s'.\n"
                                        + "A PRIMARY KEY column must be declared on non-nullable physical columns.",
                                columnName));
            }
        }

        // Build primary key constraint
        String constraintName =
                sqlTableConstraint
                        .getConstraintName()
                        .orElseGet(
                                () ->
                                        primaryKeyColumns.stream()
                                                .collect(Collectors.joining("_", "PK_", "")));
        schemaBuilder.primaryKeyNamed(constraintName, primaryKeyColumns);
    }
}
