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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.StartMode;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.ConvertTableToMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.FullAlterMaterializedTableOperation;
import org.apache.flink.table.operations.materializedtable.MaterializedTableChangeHandler;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
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
        if (resolvedBaseTable.isEmpty()) {
            return handleCreate(sqlCreateOrAlterMaterializedTable, context, identifier);
        }
        final ResolvedCatalogBaseTable<?> oldBaseTable = resolvedBaseTable.get();
        final TableKind oldBaseTableKind = oldBaseTable.getTableKind();
        switch (oldBaseTableKind) {
            case MATERIALIZED_TABLE:
                return handleAlter(
                        sqlCreateOrAlterMaterializedTable,
                        (ResolvedCatalogMaterializedTable) oldBaseTable,
                        context,
                        identifier);
            case TABLE:
                return handleConvert(
                        sqlCreateOrAlterMaterializedTable,
                        context,
                        identifier,
                        (ResolvedCatalogTable) oldBaseTable);
            default:
                throw new ValidationException(
                        String.format(
                                "Catalog object %s of kind %s does not support the CREATE OR ALTER MATERIALIZED TABLE operation.",
                                identifier.asSummaryString(), oldBaseTableKind));
        }
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
        final PlannerQueryOperation asQuery = mergeContext.getAsQueryOperation();

        return new FullAlterMaterializedTableOperation(
                identifier,
                currentTable -> buildTableChanges(currentTable, mergeContext, schemaResolver),
                oldTable,
                currentTable -> buildNewTable(currentTable, mergeContext, schemaResolver),
                asQuery);
    }

    private Operation handleConvert(
            final SqlCreateOrAlterMaterializedTable sqlCreateOrAlterMaterializedTable,
            final ConvertContext context,
            final ObjectIdentifier identifier,
            final ResolvedCatalogTable oldBaseTable) {
        final boolean conversionEnabled =
                context.getTableConfig()
                        .getRootConfiguration()
                        .get(TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED);
        if (!conversionEnabled) {
            throw new ValidationException(
                    "Regular table does not support create or alter operation.");
        }
        final MergeContext baseMergeContext =
                getMergeContext(sqlCreateOrAlterMaterializedTable, context);

        final CatalogMaterializedTable newMaterializedTable =
                CatalogMaterializedTable.newBuilder()
                        .schema(baseMergeContext.getMergedSchema())
                        .comment(baseMergeContext.getMergedComment())
                        .partitionKeys(baseMergeContext.getMergedPartitionKeys())
                        .options(baseMergeContext.getMergedTableOptions())
                        .originalQuery(baseMergeContext.getMergedOriginalQuery())
                        .expandedQuery(baseMergeContext.getMergedExpandedQuery())
                        .distribution(baseMergeContext.getMergedTableDistribution().orElse(null))
                        .freshness(baseMergeContext.getMergedFreshness())
                        .logicalRefreshMode(baseMergeContext.getMergedLogicalRefreshMode())
                        .refreshMode(baseMergeContext.getMergedRefreshMode())
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .startMode(baseMergeContext.getMergedStartMode())
                        .build();
        final ResolvedCatalogMaterializedTable resolvedNewMaterializedTable =
                context.getCatalogManager().resolveCatalogMaterializedTable(newMaterializedTable);

        final PlannerQueryOperation asQueryOperation =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                baseMergeContext.getAsQueryOperation(),
                                sqlCreateOrAlterMaterializedTable.getAsQuery(),
                                resolvedNewMaterializedTable);

        return new ConvertTableToMaterializedTableOperation(
                identifier,
                oldBaseTable,
                resolvedNewMaterializedTable,
                resolvedCatalogMaterializedTable ->
                        buildConversionTableChanges(
                                oldBaseTable,
                                resolvedCatalogMaterializedTable,
                                baseMergeContext.hasSchemaDefinition(),
                                baseMergeContext.hasConstraintDefinition()),
                asQueryOperation);
    }

    private List<TableChange> buildConversionTableChanges(
            final ResolvedCatalogTable oldTable,
            final ResolvedCatalogMaterializedTable newTable,
            final boolean hasSchemaDefinition,
            final boolean hasConstraintDefinition) {
        final ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        final ResolvedSchema newSchema = newTable.getResolvedSchema();
        final List<TableChange> changes =
                new ArrayList<>(
                        MaterializedTableUtils.validateAndExtractColumnChanges(
                                oldSchema, newSchema, hasSchemaDefinition));

        getConstraintChange(oldSchema, newSchema, hasConstraintDefinition).ifPresent(changes::add);
        getWatermarkChange(oldSchema, newSchema, hasSchemaDefinition).ifPresent(changes::add);

        changes.addAll(
                getQueryTableChanges(
                        null, null, newTable.getOriginalQuery(), newTable.getExpandedQuery()));
        changes.addAll(getOptionsTableChanges(oldTable.getOptions(), newTable.getOptions()));
        changes.addAll(
                getDistributionTableChanges(
                        oldTable.getDistribution().orElse(null),
                        newTable.getDistribution().orElse(null)));

        newTable.getStartMode()
                .ifPresent(newStartMode -> changes.add(TableChange.modifyStartMode(newStartMode)));

        return changes;
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
        final MergeContext mergeContext = getMergeContext(sqlCreateOrAlterTable, context);
        final ResolvedCatalogMaterializedTable resolvedTable =
                getResolvedCatalogMaterializedTable(mergeContext, sqlCreateOrAlterTable, context);
        final PlannerQueryOperation asQueryOperation =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                mergeContext.getAsQueryOperation(),
                                sqlCreateOrAlterTable.getAsQuery(),
                                resolvedTable);

        return new CreateMaterializedTableOperation(identifier, resolvedTable, asQueryOperation);
    }

    private List<TableChange> buildTableChanges(
            final ResolvedCatalogMaterializedTable oldTable,
            final MergeContext mergeContext,
            final SchemaResolver schemaResolver) {
        final List<TableChange> changes =
                getSchemaTableChanges(mergeContext, schemaResolver, oldTable);

        changes.addAll(
                getQueryTableChanges(
                        oldTable.getOriginalQuery(),
                        oldTable.getExpandedQuery(),
                        mergeContext.getMergedOriginalQuery(),
                        mergeContext.getMergedExpandedQuery()));
        changes.addAll(
                getOptionsTableChanges(
                        oldTable.getOptions(), mergeContext.getMergedTableOptions()));
        changes.addAll(
                getDistributionTableChanges(
                        oldTable.getDistribution().orElse(null),
                        mergeContext.getMergedTableDistribution().orElse(null)));

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
            final TableDistribution oldDistribution, final TableDistribution newDistribution) {
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
            final Map<String, String> oldOptions, final Map<String, String> newOptions) {
        final List<TableChange> changes = new ArrayList<>();

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
            final String oldOriginalQuery,
            final String oldExpandedQuery,
            final String newOriginalQuery,
            final String newExpandedQuery) {
        if (!Objects.equals(oldOriginalQuery, newOriginalQuery)
                || !Objects.equals(oldExpandedQuery, newExpandedQuery)) {
            return List.of(TableChange.modifyDefinitionQuery(newOriginalQuery, newExpandedQuery));
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

            private final PlannerQueryOperation asQueryOperation =
                    SqlCreateOrAlterMaterializedTableConverter.this.getAsQueryOperation(
                            sqlCreateMaterializedTable, context);

            private final ResolvedSchema querySchema = asQueryOperation.getResolvedSchema();

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
                return asQueryOperation.getResolvedSchema();
            }

            @Override
            public PlannerQueryOperation getAsQueryOperation() {
                return asQueryOperation;
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
