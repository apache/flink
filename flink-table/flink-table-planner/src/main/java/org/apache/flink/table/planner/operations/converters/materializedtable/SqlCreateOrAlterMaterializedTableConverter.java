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
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableChangeHandler;
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
        final SchemaResolver schemaResolver = context.getCatalogManager().getSchemaResolver();
        final MergeContext mergeContext = getMergeContext(sqlCreateOrAlterTable, context);
        return new FullAlterMaterializedTableOperation(
                identifier,
                currentTable -> buildTableChanges(currentTable, mergeContext, schemaResolver),
                oldTable,
                currentTable -> buildNewTable(currentTable, mergeContext, schemaResolver));
    }

    private CatalogMaterializedTable buildNewTable(
            final ResolvedCatalogMaterializedTable currentTable,
            final MergeContext mergeContext,
            final SchemaResolver schemaResolver) {
        return CatalogMaterializedTable.newBuilder()
                .schema(
                        MaterializedTableChangeHandler.getHandlerWithChanges(
                                        currentTable,
                                        getSchemaTableChanges(
                                                mergeContext, schemaResolver, currentTable))
                                .retrieveSchema())
                .comment(mergeContext.getMergedComment())
                .partitionKeys(mergeContext.getMergedPartitionKeys())
                .options(mergeContext.getMergedTableOptions())
                .originalQuery(mergeContext.getMergedOriginalQuery())
                .expandedQuery(mergeContext.getMergedExpandedQuery())
                .distribution(mergeContext.getMergedTableDistribution().orElse(null))
                .freshness(mergeContext.getMergedFreshness())
                .logicalRefreshMode(mergeContext.getMergedLogicalRefreshMode())
                .refreshMode(mergeContext.getMergedRefreshMode())
                .refreshStatus(currentTable.getRefreshStatus())
                .refreshHandlerDescription(currentTable.getRefreshHandlerDescription().orElse(null))
                .serializedRefreshHandler(currentTable.getSerializedRefreshHandler())
                .startMode(mergeContext.getMergedStartMode())
                .build();
    }

    private Operation handleCreate(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterTable,
            final ConvertContext context,
            final ObjectIdentifier identifier) {
        final ResolvedCatalogMaterializedTable resolvedTable =
                getResolvedCatalogMaterializedTable(sqlCreateOrAlterTable, context);

        return new CreateMaterializedTableOperation(identifier, resolvedTable);
    }

    private List<TableChange> buildTableChanges(
            final ResolvedCatalogMaterializedTable oldTable,
            final MergeContext mergeContext,
            final SchemaResolver schemaResolver) {
        final List<TableChange> changes =
                getSchemaTableChanges(mergeContext, schemaResolver, oldTable);

        changes.addAll(getQueryTableChanges(mergeContext, oldTable));
        changes.addAll(getOptionsTableChanges(mergeContext, oldTable));
        changes.addAll(getDistributionTableChanges(mergeContext, oldTable));

        final RefreshMode oldRefreshMode = oldTable.getRefreshMode();
        final RefreshMode newRefreshMode = mergeContext.getMergedRefreshMode();
        if (oldRefreshMode != newRefreshMode && newRefreshMode != null) {
            throw new ValidationException("Changing of REFRESH MODE is unsupported");
        }

        final StartMode newStartMode = mergeContext.getMergedStartMode();
        if (newStartMode != null) {
            final StartMode oldStartMode =
                    oldTable.getStartMode()
                            .orElseThrow(
                                    () ->
                                            new ValidationException(
                                                    "Start mode must be set on materialized table."));
            if (!Objects.equals(oldStartMode, newStartMode)) {
                changes.add(TableChange.modifyStartMode(newStartMode));
            }
        }

        return changes;
    }

    private List<TableChange> getDistributionTableChanges(
            final MergeContext mergeContext, final ResolvedCatalogMaterializedTable oldTable) {
        final TableDistribution oldDistribution = oldTable.getDistribution().orElse(null);
        final TableDistribution newDistribution =
                mergeContext.getMergedTableDistribution().orElse(null);
        if (!Objects.equals(oldDistribution, newDistribution)) {
            if (oldDistribution == null) {
                return List.of(TableChange.add(newDistribution));
            } else if (newDistribution == null) {
                return List.of(TableChange.dropDistribution());
            } else {
                return List.of(TableChange.modify(newDistribution));
            }
        }
        return List.of();
    }

    private List<TableChange> getOptionsTableChanges(
            final MergeContext mergeContext, final ResolvedCatalogMaterializedTable oldTable) {
        final List<TableChange> changes = new ArrayList<>();
        final Map<String, String> oldOptions = oldTable.getOptions();
        final Map<String, String> newOptions = mergeContext.getMergedTableOptions();

        for (Map.Entry<String, String> newOptionEntry : newOptions.entrySet()) {
            if (!newOptionEntry.getValue().equals(oldOptions.get(newOptionEntry.getKey()))) {
                changes.add(TableChange.set(newOptionEntry.getKey(), newOptionEntry.getValue()));
            }
        }

        for (Map.Entry<String, String> oldOptionEntry : oldOptions.entrySet()) {
            if (newOptions.get(oldOptionEntry.getKey()) == null) {
                changes.add(TableChange.reset(oldOptionEntry.getKey()));
            }
        }
        return changes;
    }

    private List<TableChange> getQueryTableChanges(
            final MergeContext mergeContext, final ResolvedCatalogMaterializedTable oldTable) {
        final String originalQuery = oldTable.getOriginalQuery();
        final String expandedQuery = oldTable.getExpandedQuery();
        if (!originalQuery.equals(mergeContext.getMergedOriginalQuery())
                || !expandedQuery.equals(mergeContext.getMergedExpandedQuery())) {
            return List.of(
                    TableChange.modifyDefinitionQuery(
                            mergeContext.getMergedOriginalQuery(),
                            mergeContext.getMergedExpandedQuery()));
        }
        return List.of();
    }

    private List<TableChange> getSchemaTableChanges(
            final MergeContext mergeContext,
            final SchemaResolver schemaResolver,
            final ResolvedCatalogMaterializedTable oldTable) {
        final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final ResolvedSchema newSchema = schemaResolver.resolve(mergeContext.getMergedSchema());
        final boolean hasSchemaDefinition = mergeContext.hasSchemaDefinition();
        final List<TableChange> changes =
                new ArrayList<>(
                        MaterializedTableUtils.validateAndExtractColumnChanges(
                                oldSchema, newSchema, hasSchemaDefinition));

        getConstraintChange(oldSchema, newSchema, mergeContext.hasConstraintDefinition())
                .ifPresent(changes::add);
        getWatermarkChange(oldSchema, newSchema, hasSchemaDefinition).ifPresent(changes::add);
        return changes;
    }

    private Optional<TableChange> getConstraintChange(
            final ResolvedSchema oldSchema,
            final ResolvedSchema newSchema,
            final boolean hasConstraintDefinition) {
        final UniqueConstraint oldConstraint = oldSchema.getPrimaryKey().orElse(null);
        final UniqueConstraint newConstraint = newSchema.getPrimaryKey().orElse(null);
        if (hasConstraintDefinition && !Objects.equals(oldConstraint, newConstraint)) {
            if (newConstraint == null) {
                return Optional.of(TableChange.dropConstraint(oldConstraint.getName()));
            } else if (oldConstraint == null) {
                return Optional.of(TableChange.add(newConstraint));
            } else {
                return Optional.of(TableChange.modify(newConstraint));
            }
        }
        return Optional.empty();
    }

    private Optional<TableChange> getWatermarkChange(
            final ResolvedSchema oldSchema,
            final ResolvedSchema newSchema,
            boolean hasSchemaDefinition) {
        final WatermarkSpec oldWatermarkSpec =
                oldSchema.getWatermarkSpecs().isEmpty()
                        ? null
                        : oldSchema.getWatermarkSpecs().get(0);
        final WatermarkSpec newWatermarkSpec =
                newSchema.getWatermarkSpecs().isEmpty()
                        ? null
                        : newSchema.getWatermarkSpecs().get(0);
        if (hasSchemaDefinition && !Objects.equals(oldWatermarkSpec, newWatermarkSpec)) {
            if (newWatermarkSpec == null) {
                return Optional.of(TableChange.dropWatermark());
            } else if (oldWatermarkSpec == null) {
                return Optional.of(TableChange.add(newWatermarkSpec));
            } else {
                return Optional.of(TableChange.modify(newWatermarkSpec));
            }
        }
        return Optional.empty();
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
            public boolean hasConstraintDefinition() {
                return !sqlCreateMaterializedTable.getTableConstraints().isEmpty()
                        || hasSchemaDefinition();
            }

            @Override
            public Schema getMergedSchema() {
                final SqlNodeList sqlNodeList = sqlCreateMaterializedTable.getColumnList();
                if (createOrAlterOperation(sqlCreateMaterializedTable)) {
                    MaterializedTableUtils.validatePersistedColumnsUsedByQuery(
                            sqlNodeList, querySchema);
                } else {
                    validatePhysicalColumnsUsedByQuery(sqlNodeList, querySchema);
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
                return getDerivedRefreshMode(getMergedLogicalRefreshMode());
            }

            @Override
            public LogicalRefreshMode getMergedLogicalRefreshMode() {
                return getDerivedLogicalRefreshMode(sqlCreateMaterializedTable);
            }

            @Override
            public StartMode getMergedStartMode() {
                return getStartMode(sqlCreateMaterializedTable, context);
            }

            @Override
            public String getMergedComment() {
                return getComment(sqlCreateMaterializedTable);
            }

            @Override
            public IntervalFreshness getMergedFreshness() {
                return getDerivedFreshness(sqlCreateMaterializedTable);
            }
        };
    }

    private static void validatePhysicalColumnsUsedByQuery(
            SqlNodeList sqlNodeList, ResolvedSchema querySchema) {
        final Set<String> querySchemaColumnNames = new HashSet<>(querySchema.getColumnNames());
        for (SqlNode column : sqlNodeList) {
            if (!(column instanceof SqlRegularColumn)) {
                continue;
            }
            final SqlRegularColumn physicalColumn = (SqlRegularColumn) column;
            if (!querySchemaColumnNames.contains(physicalColumn.getName().getSimple())) {
                throw new ValidationException(
                        String.format(
                                "Invalid as physical column '%s' is defined in the DDL, but is not used in a query column.",
                                physicalColumn.getName().getSimple()));
            }
        }
    }
}
