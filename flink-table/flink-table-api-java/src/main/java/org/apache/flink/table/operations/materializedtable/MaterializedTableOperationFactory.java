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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.MaterializedTableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
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
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;

/**
 * Builds the operation of a {@code CREATE [OR ALTER] MATERIALIZED TABLE ... AS <query>}
 * declaration.
 */
@Internal
public final class MaterializedTableOperationFactory {

    /** Whether an object that already exists at the identifier may be altered or converted. */
    @Internal
    public enum Mode {
        /** {@code CREATE MATERIALIZED TABLE}: creates. */
        CREATE,
        /** {@code CREATE OR ALTER MATERIALIZED TABLE}: creates, alters or converts. */
        CREATE_OR_ALTER
    }

    private final CatalogManager catalogManager;
    private final TableConfig tableConfig;

    public MaterializedTableOperationFactory(
            CatalogManager catalogManager, TableConfig tableConfig) {
        this.catalogManager = catalogManager;
        this.tableConfig = tableConfig;
    }

    /** Builds the operation for the declaration against the current catalog state. */
    public Operation build(
            ObjectIdentifier identifier, Mode mode, MaterializedTableMergeContext declaration) {
        if (mode == Mode.CREATE_OR_ALTER) {
            return handleCreateOrAlter(identifier, declaration);
        }
        return handleCreate(identifier, declaration);
    }

    private Operation handleCreateOrAlter(
            ObjectIdentifier identifier, MaterializedTableMergeContext declaration) {
        final Optional<ResolvedCatalogBaseTable<?>> resolvedBaseTable =
                catalogManager.getCatalogBaseTable(identifier);
        if (resolvedBaseTable.isEmpty()) {
            return handleCreate(identifier, declaration);
        }
        final ResolvedCatalogBaseTable<?> oldBaseTable = resolvedBaseTable.get();
        final TableKind oldBaseTableKind = oldBaseTable.getTableKind();
        switch (oldBaseTableKind) {
            case MATERIALIZED_TABLE:
                return handleAlter(
                        identifier, (ResolvedCatalogMaterializedTable) oldBaseTable, declaration);
            case TABLE:
                return handleConvert(identifier, (ResolvedCatalogTable) oldBaseTable, declaration);
            default:
                throw new ValidationException(
                        String.format(
                                "Catalog object %s of kind %s does not support the CREATE OR ALTER MATERIALIZED TABLE operation.",
                                identifier.asSummaryString(), oldBaseTableKind));
        }
    }

    private Operation handleCreate(
            ObjectIdentifier identifier, MaterializedTableMergeContext declaration) {
        final ResolvedCatalogMaterializedTable resolvedTable =
                resolveNewMaterializedTable(declaration);
        final QueryOperation asQueryOperation = declaration.alignQuery(resolvedTable);
        return new CreateMaterializedTableOperation(identifier, resolvedTable, asQueryOperation);
    }

    private Operation handleAlter(
            ObjectIdentifier identifier,
            ResolvedCatalogMaterializedTable oldTable,
            MaterializedTableMergeContext declaration) {
        final SchemaResolver schemaResolver = catalogManager.getSchemaResolver();
        return new FullAlterMaterializedTableOperation(
                identifier,
                currentTable -> buildTableChanges(currentTable, declaration, schemaResolver),
                oldTable,
                currentTable -> buildNewTable(currentTable, declaration, schemaResolver),
                declaration.getQueryOperation());
    }

    private Operation handleConvert(
            ObjectIdentifier identifier,
            ResolvedCatalogTable oldBaseTable,
            MaterializedTableMergeContext declaration) {
        final boolean conversionEnabled =
                tableConfig
                        .getRootConfiguration()
                        .get(TableConfigOptions.MATERIALIZED_TABLE_CONVERSION_FROM_TABLE_ENABLED);
        if (!conversionEnabled) {
            throw new ValidationException(
                    "Regular table does not support create or alter operation.");
        }

        final CatalogMaterializedTable newMaterializedTable =
                newTableBuilder(declaration, declaration.getMergedSchema())
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build();
        final ResolvedCatalogMaterializedTable resolvedNewMaterializedTable =
                catalogManager.resolveCatalogMaterializedTable(newMaterializedTable);

        final QueryOperation asQueryOperation =
                declaration.alignQuery(resolvedNewMaterializedTable);

        return new ConvertTableToMaterializedTableOperation(
                identifier,
                oldBaseTable,
                resolvedNewMaterializedTable,
                resolvedCatalogMaterializedTable ->
                        buildConversionTableChanges(
                                oldBaseTable,
                                resolvedCatalogMaterializedTable,
                                declaration.hasSchemaDefinition(),
                                declaration.hasConstraintDefinition()),
                asQueryOperation);
    }

    private ResolvedCatalogMaterializedTable resolveNewMaterializedTable(
            MaterializedTableMergeContext declaration) {
        final Schema schema = declaration.getMergedSchema();
        verifyPartitioningColumnsExist(
                declaration.getQuerySchema(),
                declaration.getPartitionKeys(),
                declaration.getOptions());

        return catalogManager.resolveCatalogMaterializedTable(
                newTableBuilder(declaration, schema)
                        .refreshStatus(RefreshStatus.INITIALIZING)
                        .build());
    }

    private CatalogMaterializedTable buildNewTable(
            ResolvedCatalogMaterializedTable currentTable,
            MaterializedTableMergeContext declaration,
            SchemaResolver schemaResolver) {
        final Schema schema =
                MaterializedTableChangeHandler.getHandlerWithChanges(
                                currentTable,
                                getSchemaTableChanges(declaration, schemaResolver, currentTable))
                        .retrieveSchema();
        return newTableBuilder(declaration, schema)
                .refreshStatus(currentTable.getRefreshStatus())
                .refreshHandlerDescription(currentTable.getRefreshHandlerDescription().orElse(null))
                .serializedRefreshHandler(currentTable.getSerializedRefreshHandler())
                .build();
    }

    private CatalogMaterializedTable.Builder newTableBuilder(
            MaterializedTableMergeContext declaration, Schema schema) {
        return CatalogMaterializedTable.newBuilder()
                .schema(schema)
                .comment(declaration.getComment())
                .partitionKeys(declaration.getPartitionKeys())
                .options(declaration.getOptions())
                .originalQuery(declaration.getOriginalQuery())
                .expandedQuery(declaration.getExpandedQuery())
                .distribution(declaration.getDistribution().orElse(null))
                .freshness(declaration.getFreshness())
                .logicalRefreshMode(declaration.getLogicalRefreshMode())
                .refreshMode(getRefreshMode(declaration))
                .startMode(getStartMode(declaration));
    }

    private List<TableChange> buildTableChanges(
            ResolvedCatalogMaterializedTable oldTable,
            MaterializedTableMergeContext declaration,
            SchemaResolver schemaResolver) {
        final List<TableChange> changes =
                getSchemaTableChanges(declaration, schemaResolver, oldTable);

        changes.addAll(
                getQueryTableChanges(
                        oldTable.getOriginalQuery(),
                        oldTable.getExpandedQuery(),
                        declaration.getOriginalQuery(),
                        declaration.getExpandedQuery()));
        changes.addAll(getOptionsTableChanges(oldTable.getOptions(), declaration.getOptions()));
        changes.addAll(
                getDistributionTableChanges(
                        oldTable.getDistribution().orElse(null),
                        declaration.getDistribution().orElse(null)));

        final RefreshMode oldRefreshMode = oldTable.getRefreshMode();
        final RefreshMode newRefreshMode = getRefreshMode(declaration);
        if (oldRefreshMode != newRefreshMode && newRefreshMode != null) {
            throw new ValidationException("Changing of REFRESH MODE is unsupported");
        }

        final StartMode newStartMode = getStartMode(declaration);
        final StartMode oldStartMode =
                oldTable.getStartMode()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Start mode must be set on materialized table."));
        if (!Objects.equals(oldStartMode, newStartMode)) {
            changes.add(TableChange.modifyStartMode(newStartMode));
        }

        return changes;
    }

    private static List<TableChange> buildConversionTableChanges(
            ResolvedCatalogTable oldTable,
            ResolvedCatalogMaterializedTable newTable,
            boolean hasSchemaDefinition,
            boolean hasConstraintDefinition) {
        final List<TableChange> changes =
                getSchemaTableChanges(
                        oldTable.getResolvedSchema(),
                        newTable.getResolvedSchema(),
                        hasSchemaDefinition,
                        hasConstraintDefinition);

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

    private static List<TableChange> getSchemaTableChanges(
            MaterializedTableMergeContext declaration,
            SchemaResolver schemaResolver,
            ResolvedCatalogMaterializedTable oldTable) {
        return getSchemaTableChanges(
                oldTable.getResolvedSchema(),
                schemaResolver.resolve(declaration.getMergedSchema()),
                declaration.hasSchemaDefinition(),
                declaration.hasConstraintDefinition());
    }

    private static List<TableChange> getSchemaTableChanges(
            ResolvedSchema oldSchema,
            ResolvedSchema newSchema,
            boolean hasSchemaDefinition,
            boolean hasConstraintDefinition) {
        final List<TableChange> changes =
                new ArrayList<>(
                        MaterializedTableSchemaUtils.validateAndExtractColumnChanges(
                                oldSchema, newSchema, hasSchemaDefinition));
        getConstraintChange(oldSchema, newSchema, hasConstraintDefinition).ifPresent(changes::add);
        getWatermarkChange(oldSchema, newSchema, hasSchemaDefinition).ifPresent(changes::add);
        return changes;
    }

    private static Optional<TableChange> getConstraintChange(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean hasConstraintDefinition) {
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

    private static Optional<TableChange> getWatermarkChange(
            ResolvedSchema oldSchema, ResolvedSchema newSchema, boolean hasSchemaDefinition) {
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

    private static List<TableChange> getDistributionTableChanges(
            @Nullable TableDistribution oldDistribution,
            @Nullable TableDistribution newDistribution) {
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

    private static List<TableChange> getOptionsTableChanges(
            Map<String, String> oldOptions, Map<String, String> newOptions) {
        final List<TableChange> changes = new ArrayList<>();

        for (final Map.Entry<String, String> newOptionEntry : newOptions.entrySet()) {
            if (!newOptionEntry.getValue().equals(oldOptions.get(newOptionEntry.getKey()))) {
                changes.add(TableChange.set(newOptionEntry.getKey(), newOptionEntry.getValue()));
            }
        }

        for (final Map.Entry<String, String> oldOptionEntry : oldOptions.entrySet()) {
            if (newOptions.get(oldOptionEntry.getKey()) == null) {
                changes.add(TableChange.reset(oldOptionEntry.getKey()));
            }
        }
        return changes;
    }

    private static List<TableChange> getQueryTableChanges(
            @Nullable String oldOriginalQuery,
            @Nullable String oldExpandedQuery,
            String newOriginalQuery,
            String newExpandedQuery) {
        if (!Objects.equals(oldOriginalQuery, newOriginalQuery)
                || !Objects.equals(oldExpandedQuery, newExpandedQuery)) {
            return List.of(TableChange.modifyDefinitionQuery(newOriginalQuery, newExpandedQuery));
        }
        return List.of();
    }

    private static void verifyPartitioningColumnsExist(
            ResolvedSchema schema, List<String> partitionKeys, Map<String, String> tableOptions) {
        final Set<String> partitionFieldOptions =
                tableOptions.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet());

        for (final String partitionKey : partitionKeys) {
            if (schema.getColumn(partitionKey).isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the query's schema. Available columns: [%s].",
                                partitionKey,
                                schema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }

        // verify partition key used by materialized table partition option
        // partition.fields.#.date-formatter whether exist
        for (final String partitionOption : partitionFieldOptions) {
            final String partitionKey =
                    partitionOption.substring(
                            PARTITION_FIELDS.length() + 1,
                            partitionOption.length() - (DATE_FORMATTER.length() + 1));
            // partition key used in option partition.fields.#.date-formatter must be existed
            if (!partitionKeys.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s' referenced by materialized table option '%s' isn't a partition column. Available partition columns: [%s].",
                                partitionKey,
                                partitionOption,
                                partitionKeys.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            // partition key used in option partition.fields.#.date-formatter must be string type
            final LogicalType partitionKeyType =
                    schema.getColumn(partitionKey).get().getDataType().getLogicalType();
            if (!partitionKeyType
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                throw new ValidationException(
                        String.format(
                                "Materialized table option '%s' only supports referring to char, varchar and string type partition column. Column `%s` type is %s.",
                                partitionOption, partitionKey, partitionKeyType.asSummaryString()));
            }
        }
    }

    private static @Nullable RefreshMode getRefreshMode(MaterializedTableMergeContext declaration) {
        return fromLogicalRefreshModeToRefreshMode(declaration.getLogicalRefreshMode());
    }

    /**
     * The configured default applies on every read, so an alter compares an undeclared start mode
     * against the default rather than keeping the old one.
     */
    private StartMode getStartMode(MaterializedTableMergeContext declaration) {
        final StartMode startMode = declaration.getStartMode();
        if (startMode != null) {
            return startMode;
        }
        return StartMode.of(
                tableConfig.get(
                        MaterializedTableConfigOptions.MATERIALIZED_TABLE_DEFAULT_START_MODE));
    }

    private static @Nullable RefreshMode fromLogicalRefreshModeToRefreshMode(
            LogicalRefreshMode logicalRefreshMode) {
        switch (logicalRefreshMode) {
            case AUTOMATIC:
                return null;
            case FULL:
                return RefreshMode.FULL;
            case CONTINUOUS:
                return RefreshMode.CONTINUOUS;
            default:
                throw new IllegalArgumentException(
                        "Unknown logical refresh mode: " + logicalRefreshMode);
        }
    }
}
