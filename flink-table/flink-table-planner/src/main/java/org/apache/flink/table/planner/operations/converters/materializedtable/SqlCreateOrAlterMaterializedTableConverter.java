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

package org.apache.flink.table.planner.operations.converters.materializedtable;

import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateOrAlterMaterializedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.planner.operations.converters.MergeTableAsUtil;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/** A converter for {@link SqlCreateOrAlterMaterializedTable}. */
public class SqlCreateOrAlterMaterializedTableConverter
        extends AbstractCreateMaterializedTableConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            ConvertContext context) {
        final ObjectIdentifier identifier =
                getIdentifier(sqlCreateOrAlterMaterializedTable, context);

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
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterTable,
            final ResolvedCatalogMaterializedTable oldTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final MergeContext mergeContext = getMergeContext(sqlCreateOrAlterTable, context);
        return new FullAlterMaterializedTableOperation(
                identifier,
                buildTableChanges(mergeContext, context.getCatalogManager().getSchemaResolver()),
                oldTable);
    }

    private Operation handleCreate(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final ResolvedCatalogMaterializedTable resolvedTable =
                getResolvedCatalogMaterializedTable(sqlCreateOrAlterTable, context);

        return new CreateMaterializedTableOperation(identifier, resolvedTable);
    }

    private Function<ResolvedCatalogMaterializedTable, List<TableChange>> buildTableChanges(
            final MergeContext mergeContext, final SchemaResolver schemaResolver) {
        return oldTable -> {
            final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
            final ResolvedSchema newSchema = schemaResolver.resolve(mergeContext.getMergedSchema());
            final List<TableChange> changes =
                    new ArrayList<>(
                            MaterializedTableUtils.validateAndExtractColumnChanges(
                                    oldSchema, newSchema, mergeContext.hasSchemaDefinition()));

            final UniqueConstraint oldConstraint = oldSchema.getPrimaryKey().orElse(null);
            final UniqueConstraint newConstraint = newSchema.getPrimaryKey().orElse(null);
            if (!Objects.equals(oldConstraint, newConstraint)) {
                if (newConstraint == null) {
                    changes.add(TableChange.dropConstraint(oldConstraint.getName()));
                } else if (oldConstraint == null) {
                    changes.add(TableChange.add(newConstraint));
                } else {
                    changes.add(TableChange.modify(newConstraint));
                }
            }

            changes.add(
                    TableChange.modifyDefinitionQuery(
                            mergeContext.getMergedOriginalQuery(),
                            mergeContext.getMergedExpandedQuery()));

            final Map<String, String> oldOptions = oldTable.getOptions();
            final Map<String, String> newOptions = mergeContext.getMergedTableOptions();

            for (Map.Entry<String, String> newOptionEntry : newOptions.entrySet()) {
                if (!newOptionEntry.getValue().equals(oldOptions.get(newOptionEntry.getKey()))) {
                    changes.add(
                            TableChange.set(newOptionEntry.getKey(), newOptionEntry.getValue()));
                }
            }

            for (Map.Entry<String, String> oldOptionEntry : oldOptions.entrySet()) {
                if (newOptions.get(oldOptionEntry.getKey()) == null) {
                    changes.add(TableChange.reset(oldOptionEntry.getKey()));
                }
            }

            final RefreshMode oldRefreshMode = oldTable.getRefreshMode();
            final RefreshMode newRefreshMode = mergeContext.getMergedRefreshMode();
            if (oldRefreshMode != newRefreshMode && newRefreshMode != null) {
                throw new ValidationException("Changing of REFRESH MODE is unsupported");
            }

            final TableDistribution oldDistribution = oldTable.getDistribution().orElse(null);
            final TableDistribution newDistribution =
                    mergeContext.getMergedTableDistribution().orElse(null);
            if (!Objects.equals(oldDistribution, newDistribution)) {
                if (oldDistribution == null) {
                    changes.add(TableChange.add(newDistribution));
                } else if (newDistribution == null) {
                    changes.add(TableChange.dropDistribution());
                } else {
                    changes.add(TableChange.modify(newDistribution));
                }
            }

            return changes;
        };
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
            public boolean hasSchemaDefinition() {
                final SqlNodeList sqlNodeList = sqlCreateMaterializedTable.getColumnList();
                return !sqlNodeList.getList().isEmpty()
                        && sqlNodeList.getList().get(0) instanceof SqlRegularColumn;
            }

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

            @Override
            public RefreshMode getMergedRefreshMode() {
                return getDerivedRefreshMode(
                        getDerivedLogicalRefreshMode(sqlCreateMaterializedTable));
            }
        };
    }
}
