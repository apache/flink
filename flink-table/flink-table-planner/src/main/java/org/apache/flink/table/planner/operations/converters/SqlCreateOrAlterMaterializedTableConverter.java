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

import org.apache.flink.sql.parser.ddl.SqlCreateOrAlterMaterializedTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.table.api.Schema;
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
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.operations.converters.table.MergeTableAsUtil;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** A converter for {@link SqlCreateOrAlterMaterializedTable}. */
public class SqlCreateOrAlterMaterializedTableConverter
        extends AbstractCreateMaterializedTableConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                this.getIdentifier(sqlCreateOrAlterMaterializedTable, context);

        if (createOrAlterOperation(sqlCreateOrAlterMaterializedTable)) {
            return handleCreateOrAlter(sqlCreateOrAlterMaterializedTable, context, identifier);
        }
        return handleCreate(sqlCreateOrAlterMaterializedTable, context, identifier);
    }

    private Operation handleCreateOrAlter(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final Optional<ResolvedCatalogBaseTable<?>> resolvedBaseTable =
                context.getCatalogManager().getCatalogBaseTable(identifier);
        return resolvedBaseTable
                .map(
                        oldBaseTable -> {
                            if (oldBaseTable.getTableKind() != TableKind.MATERIALIZED_TABLE) {
                                throw new ValidationException(
                                        String.format(
                                                "Table %s is not a materialized table. Only materialized table support create or alter operation.",
                                                identifier.asSummaryString()));
                            }
                            return handleAlter(
                                    sqlCreateOrAlterMaterializedTable,
                                    (ResolvedCatalogMaterializedTable) oldBaseTable,
                                    context,
                                    identifier);
                        })
                .orElseGet(
                        () -> handleCreate(sqlCreateOrAlterMaterializedTable, context, identifier));
    }

    private static boolean createOrAlterOperation(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable) {
        return sqlCreateOrAlterMaterializedTable.getOperator()
                == SqlCreateOrAlterMaterializedTable.CREATE_OR_ALTER_OPERATOR;
    }

    private Operation handleAlter(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ResolvedCatalogMaterializedTable oldMaterializedTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final CatalogMaterializedTable newTable =
                buildNewCatalogMaterializedTableFromOldTable(
                        oldMaterializedTable, sqlCreateOrAlterMaterializedTable, context);

        List<TableChange> tableChanges =
                buildTableChanges(sqlCreateOrAlterMaterializedTable, oldMaterializedTable, context);

        return new AlterMaterializedTableAsQueryOperation(identifier, tableChanges, newTable);
    }

    private Operation handleCreate(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final ResolvedCatalogMaterializedTable resolvedCatalogMaterializedTable =
                this.getResolvedCatalogMaterializedTable(
                        sqlCreateOrAlterMaterializedTable, context);

        return new CreateMaterializedTableOperation(identifier, resolvedCatalogMaterializedTable);
    }

    private List<TableChange> buildTableChanges(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ResolvedCatalogMaterializedTable oldTable,
            final ConvertContext context) {
        List<TableChange> changes = new ArrayList<>();
        final MergeContext mergeContext =
                this.getMergeContext(sqlCreateOrAlterMaterializedTable, context);

        final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final List<Column> newColumns =
                MaterializedTableUtils.validateAndExtractNewColumns(
                        oldSchema, mergeContext.getMergedQuerySchema());

        newColumns.forEach(column -> changes.add(TableChange.add(column)));
        changes.add(TableChange.modifyDefinitionQuery(mergeContext.getMergedExpandedQuery()));

        return changes;
    }

    private CatalogMaterializedTable buildNewCatalogMaterializedTableFromOldTable(
            final ResolvedCatalogMaterializedTable oldMaterializedTable,
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ConvertContext context) {
        final Schema.Builder schemaBuilder =
                Schema.newBuilder().fromResolvedSchema(oldMaterializedTable.getResolvedSchema());

        // Add new columns if this is an alter operation
        final ResolvedSchema oldSchema = oldMaterializedTable.getResolvedSchema();
        final MergeContext mergeContext =
                this.getMergeContext(sqlCreateOrAlterMaterializedTable, context);
        final List<Column> newColumns =
                MaterializedTableUtils.validateAndExtractNewColumns(
                        oldSchema, mergeContext.getMergedQuerySchema());
        newColumns.forEach(col -> schemaBuilder.column(col.getName(), col.getDataType()));

        final String comment = sqlCreateOrAlterMaterializedTable.getComment();
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

        // Preserve refresh handler from old materialized table
        oldMaterializedTable
                .getRefreshHandlerDescription()
                .ifPresent(builder::refreshHandlerDescription);
        builder.serializedRefreshHandler(oldMaterializedTable.getSerializedRefreshHandler());

        return builder.build();
    }

    @Override
    protected MergeContext getMergeContext(
            final SqlCreateOrAlterMaterializedTable sqlCreateMaterializedTable,
            final ConvertContext context) {
        return new MergeContext() {
            private final MergeTableAsUtil mergeTableAsUtil = new MergeTableAsUtil(context);

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
                final Set<String> querySchemaColumnNames =
                        new HashSet<>(querySchema.getColumnNames());
                final SqlNodeList sqlNodeList = sqlCreateMaterializedTable.getColumnList();
                for (SqlNode column : sqlNodeList) {
                    if (!(column instanceof SqlRegularColumn)) {
                        continue;
                    }

                    SqlRegularColumn physicalColumn = (SqlRegularColumn) column;
                    if (!querySchemaColumnNames.contains(physicalColumn.getName().getSimple())) {
                        throw new ValidationException(
                                String.format(
                                        "Invalid as physical column '%s' is defined in the DDL, but is not used in a query column.",
                                        physicalColumn.getName().getSimple()));
                    }
                }
                if (sqlCreateMaterializedTable.isSchemaWithColumnsIdentifiersOnly()) {
                    // If only column identifiers are provided, then these are used to
                    // order the columns in the schema.
                    return mergeTableAsUtil.reorderSchema(sqlNodeList, querySchema);
                } else {
                    return mergeTableAsUtil.mergeSchemas(
                            sqlNodeList,
                            sqlCreateMaterializedTable.getWatermark().orElse(null),
                            sqlCreateMaterializedTable.getFullConstraints(),
                            querySchema);
                }
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                return sqlCreateMaterializedTable.getProperties();
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return sqlCreateMaterializedTable.getPartitionKeyList();
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedTableDistribution(
                        sqlCreateMaterializedTable);
            }

            @Override
            public String getMergedOriginalQuery() {
                return this.originalQuery;
            }

            @Override
            public String getMergedExpandedQuery() {
                return SqlCreateOrAlterMaterializedTableConverter.this.getDerivedExpandedQuery(
                        sqlCreateMaterializedTable, context);
            }

            @Override
            public ResolvedSchema getMergedQuerySchema() {
                return this.querySchema;
            }
        };
    }
}
