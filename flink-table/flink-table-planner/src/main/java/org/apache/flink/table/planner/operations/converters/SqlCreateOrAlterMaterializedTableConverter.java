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
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
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
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.AlterMaterializedTableAsQueryOperation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.MATERIALIZED_TABLE_FRESHNESS_THRESHOLD;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;
import static org.apache.flink.table.catalog.CatalogBaseTable.TableKind.MATERIALIZED_TABLE;
import static org.apache.flink.table.utils.IntervalFreshnessUtils.convertFreshnessToCron;
import static org.apache.flink.table.utils.IntervalFreshnessUtils.convertFreshnessToDuration;

/** A converter for {@link SqlCreateOrAlterMaterializedTable}. */
public class SqlCreateOrAlterMaterializedTableConverter
        implements SqlNodeConverter<SqlCreateOrAlterMaterializedTable> {

    @Override
    public Operation convertSqlNode(SqlCreateOrAlterMaterializedTable sql, ConvertContext context) {
        ParsedContext parsedContext = parseMaterializedTable(sql, context);

        return sql.isOrAlter() && tableExists(context, parsedContext.identifier)
                ? handleAlter(parsedContext, context)
                : handleCreate(parsedContext, context);
    }

    private Operation handleAlter(ParsedContext parsedContext, ConvertContext context) {
        ResolvedCatalogMaterializedTable oldTable =
                getResolvedMaterializedTable(context, parsedContext.identifier);

        CatalogMaterializedTable newTable = buildMaterializedTable(parsedContext, oldTable);

        List<MaterializedTableChange> tableChanges = buildTableChanges(parsedContext, oldTable);

        return new AlterMaterializedTableAsQueryOperation(
                parsedContext.identifier, tableChanges, newTable);
    }

    private Operation handleCreate(ParsedContext parsedContext, ConvertContext context) {
        CatalogMaterializedTable materializedTable = buildMaterializedTable(parsedContext, null);

        return new CreateMaterializedTableOperation(
                parsedContext.identifier,
                context.getCatalogManager().resolveCatalogMaterializedTable(materializedTable));
    }

    private boolean tableExists(ConvertContext context, ObjectIdentifier identifier) {
        return context.getCatalogManager().getTable(identifier).isPresent();
    }

    private List<MaterializedTableChange> buildTableChanges(
            ParsedContext parsedContext, ResolvedCatalogMaterializedTable oldTable) {
        List<MaterializedTableChange> changes = new ArrayList<>();

        // Extract new columns
        ResolvedSchema oldSchema = oldTable.getResolvedSchema();
        List<Column> newColumns =
                MaterializedTableUtils.validateAndExtractNewColumns(
                        oldSchema, parsedContext.queryOperation.getResolvedSchema());

        newColumns.forEach(column -> changes.add(TableChange.add(column)));
        changes.add(TableChange.modifyDefinitionQuery(parsedContext.expandedQuery));

        return changes;
    }

    private ResolvedCatalogMaterializedTable getResolvedMaterializedTable(
            ConvertContext context, ObjectIdentifier identifier) {
        ResolvedCatalogBaseTable<?> baseTable =
                context.getCatalogManager().getTableOrError(identifier).getResolvedTable();
        if (MATERIALIZED_TABLE != baseTable.getTableKind()) {
            throw new ValidationException(
                    "Only materialized table support modify definition query.");
        }
        return (ResolvedCatalogMaterializedTable) baseTable;
    }

    // -------------------------------------------------------------------------
    // Parsing and validation methods
    // -------------------------------------------------------------------------

    private ParsedContext parseMaterializedTable(
            SqlCreateOrAlterMaterializedTable sql, ConvertContext context) {
        ParsedContext ctx = new ParsedContext();

        // Parse identifier
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(sql.fullTableName());
        ctx.identifier = context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        // Parse comment
        ctx.comment = OperationConverterUtils.getComment(sql.getComment());

        // Parse options
        ctx.options = new HashMap<>();
        sql.getPropertyList()
                .getList()
                .forEach(
                        p ->
                                ctx.options.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        // Parse freshness
        ctx.intervalFreshness =
                MaterializedTableUtils.getMaterializedTableFreshness(sql.getFreshness());

        // Parse refresh mode
        SqlRefreshMode sqlRefreshMode = null;
        if (sql.getRefreshMode().isPresent()) {
            sqlRefreshMode = sql.getRefreshMode().get().getValueAs(SqlRefreshMode.class);
        }
        ctx.logicalRefreshMode = MaterializedTableUtils.deriveLogicalRefreshMode(sqlRefreshMode);

        // Derive refresh mode based on freshness threshold
        ctx.refreshMode =
                MaterializedTableUtils.deriveRefreshMode(
                        context.getTableConfig()
                                .getRootConfiguration()
                                .get(MATERIALIZED_TABLE_FRESHNESS_THRESHOLD),
                        convertFreshnessToDuration(ctx.intervalFreshness),
                        ctx.logicalRefreshMode);

        // Validate freshness can convert to cron for full refresh mode
        if (RefreshMode.FULL == ctx.refreshMode) {
            convertFreshnessToCron(ctx.intervalFreshness);
        }

        // Parse and validate query
        SqlNode selectQuery = sql.getAsQuery();
        ctx.originalQuery = context.toQuotedSqlString(selectQuery);
        SqlNode validateQuery = context.getSqlValidator().validate(selectQuery);
        ctx.expandedQuery = context.expandSqlIdentifiers(ctx.originalQuery);

        ctx.queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));

        // Build schema from query
        ResolvedSchema resolvedSchema = ctx.queryOperation.getResolvedSchema();
        ctx.schemaBuilder = Schema.newBuilder().fromResolvedSchema(resolvedSchema);

        // Parse partition keys
        ctx.partitionKeys =
                sql.getPartitionKeyList().getList().stream()
                        .map(p -> ((SqlIdentifier) p).getSimple())
                        .collect(Collectors.toList());

        // Verify partition columns
        verifyPartitioningColumnsExist(
                resolvedSchema,
                ctx.partitionKeys,
                ctx.options.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet()));

        // Parse and verify primary key
        sql.getTableConstraint()
                .ifPresent(
                        sqlTableConstraint ->
                                verifyAndBuildPrimaryKey(
                                        ctx.schemaBuilder, resolvedSchema, sqlTableConstraint));

        // Parse table distribution
        ctx.tableDistribution =
                Optional.ofNullable(sql.getDistribution())
                        .map(OperationConverterUtils::getDistributionFromSqlDistribution)
                        .orElse(null);

        return ctx;
    }

    private CatalogMaterializedTable buildMaterializedTable(
            ParsedContext parsedContext, @Nullable ResolvedCatalogMaterializedTable oldTable) {

        Schema.Builder schemaBuilder =
                oldTable != null
                        ? Schema.newBuilder().fromResolvedSchema(oldTable.getResolvedSchema())
                        : parsedContext.schemaBuilder;

        // Add new columns if this is an alter operation
        if (oldTable != null) {
            ResolvedSchema oldSchema = oldTable.getResolvedSchema();
            List<Column> newColumns =
                    MaterializedTableUtils.validateAndExtractNewColumns(
                            oldSchema, parsedContext.queryOperation.getResolvedSchema());
            newColumns.forEach(col -> schemaBuilder.column(col.getName(), col.getDataType()));
        }

        CatalogMaterializedTable.Builder builder =
                CatalogMaterializedTable.newBuilder()
                        .schema(schemaBuilder.build())
                        .comment(parsedContext.comment)
                        .distribution(parsedContext.tableDistribution)
                        .partitionKeys(parsedContext.partitionKeys)
                        .options(parsedContext.options)
                        .originalQuery(parsedContext.originalQuery)
                        .expandedQuery(parsedContext.expandedQuery)
                        .freshness(parsedContext.intervalFreshness)
                        .logicalRefreshMode(parsedContext.logicalRefreshMode)
                        .refreshMode(parsedContext.refreshMode)
                        .refreshStatus(RefreshStatus.INITIALIZING);

        // Preserve refresh handler from old table
        if (oldTable != null) {
            oldTable.getRefreshHandlerDescription().ifPresent(builder::refreshHandlerDescription);
            builder.serializedRefreshHandler(oldTable.getSerializedRefreshHandler());
        }

        return builder.build();
    }

    private void verifyPartitioningColumnsExist(
            ResolvedSchema resolvedSchema,
            List<String> partitionKeys,
            Set<String> partitionFieldOptions) {
        // Verify partition keys exist in schema
        for (String partitionKey : partitionKeys) {
            if (resolvedSchema.getColumn(partitionKey).isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the query schema. Available columns: [%s].",
                                partitionKey,
                                resolvedSchema.getColumnNames().stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }

        // Verify partition keys used in date-formatter options
        for (String partitionOption : partitionFieldOptions) {
            String partitionKey =
                    partitionOption.substring(
                            PARTITION_FIELDS.length() + 1,
                            partitionOption.length() - (DATE_FORMATTER.length() + 1));

            // Partition key used in option must be an actual partition column
            if (!partitionKeys.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Column '%s' referenced by materialized table option '%s' isn't a partition column. Available partition columns: [%s].",
                                partitionKey,
                                partitionOption,
                                partitionKeys.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }

            // Partition key used in date-formatter must be string type
            LogicalType partitionKeyType =
                    resolvedSchema.getColumn(partitionKey).get().getDataType().getLogicalType();
            if (!partitionKeyType
                    .getTypeRoot()
                    .getFamilies()
                    .contains(LogicalTypeFamily.CHARACTER_STRING)) {
                throw new ValidationException(
                        String.format(
                                "Materialized table option '%s' only supports referring to char, varchar and string type partition column. Column %s type is %s.",
                                partitionOption, partitionKey, partitionKeyType.asSummaryString()));
            }
        }
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

    // -------------------------------------------------------------------------
    // Helper classes
    // -------------------------------------------------------------------------

    /** Context object holding parsed materialized table information. */
    private static class ParsedContext {
        ObjectIdentifier identifier;
        @Nullable String comment;
        Map<String, String> options;
        IntervalFreshness intervalFreshness;
        LogicalRefreshMode logicalRefreshMode;
        RefreshMode refreshMode;
        String originalQuery;
        String expandedQuery;
        PlannerQueryOperation queryOperation;
        Schema.Builder schemaBuilder;
        List<String> partitionKeys;
        @Nullable TableDistribution tableDistribution;
    }
}
