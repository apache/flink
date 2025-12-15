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

import org.apache.flink.sql.parser.ddl.materializedtable.SqlCreateMaterializedTable;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.CatalogMaterializedTable.LogicalRefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshMode;
import org.apache.flink.table.catalog.CatalogMaterializedTable.RefreshStatus;
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogMaterializedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.MaterializedTableUtils;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.DATE_FORMATTER;
import static org.apache.flink.table.api.config.MaterializedTableConfigOptions.PARTITION_FIELDS;

/**
 * Abstract class for converting {@link SqlCreateMaterializedTable} and it's children to create
 * materialized table operations.
 */
public abstract class AbstractCreateMaterializedTableConverter<T extends SqlCreateMaterializedTable>
        implements SqlNodeConverter<T> {
    /** Context of create table converters while merging source and derived items. */
    protected interface MergeContext {
        Schema getMergedSchema();

        Map<String, String> getMergedTableOptions();

        List<String> getMergedPartitionKeys();

        Optional<TableDistribution> getMergedTableDistribution();

        String getMergedOriginalQuery();

        String getMergedExpandedQuery();

        ResolvedSchema getMergedQuerySchema();
    }

    protected abstract MergeContext getMergeContext(
            T sqlCreateMaterializedTable, ConvertContext context);

    protected final Optional<TableDistribution> getDerivedTableDistribution(
            T sqlCreateMaterializedTable) {
        return Optional.ofNullable(sqlCreateMaterializedTable.getDistribution())
                .map(OperationConverterUtils::getDistributionFromSqlDistribution);
    }

    protected final List<String> getDerivedPartitionKeys(T sqlCreateMaterializedTable) {
        return sqlCreateMaterializedTable.getPartitionKeyList();
    }

    protected final IntervalFreshness getDerivedFreshness(T sqlCreateMaterializedTable) {
        return Optional.ofNullable(sqlCreateMaterializedTable.getFreshness())
                .map(MaterializedTableUtils::getMaterializedTableFreshness)
                .orElse(null);
    }

    protected final ResolvedSchema getQueryResolvedSchema(
            T sqlCreateMaterializedTable, ConvertContext context) {
        SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        SqlNode validateQuery = context.getSqlValidator().validate(selectQuery);

        PlannerQueryOperation queryOperation =
                new PlannerQueryOperation(
                        context.toRelRoot(validateQuery).project(),
                        () -> context.toQuotedSqlString(validateQuery));
        return queryOperation.getResolvedSchema();
    }

    protected final LogicalRefreshMode getDerivedLogicalRefreshMode(T sqlCreateMaterializedTable) {
        return MaterializedTableUtils.deriveLogicalRefreshMode(
                sqlCreateMaterializedTable.getRefreshMode());
    }

    protected final RefreshMode getDerivedRefreshMode(LogicalRefreshMode logicalRefreshMode) {
        return MaterializedTableUtils.fromLogicalRefreshModeToRefreshMode(logicalRefreshMode);
    }

    protected final String getDerivedOriginalQuery(
            T sqlCreateMaterializedTable, ConvertContext context) {
        SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        return context.toQuotedSqlString(selectQuery);
    }

    protected final String getDerivedExpandedQuery(
            T sqlCreateMaterializedTable, ConvertContext context) {
        SqlNode selectQuery = sqlCreateMaterializedTable.getAsQuery();
        SqlNode validatedQuery = context.getSqlValidator().validate(selectQuery);
        return context.expandSqlIdentifiers(context.toQuotedSqlString(validatedQuery));
    }

    protected final String getComment(T sqlCreateMaterializedTable) {
        return sqlCreateMaterializedTable.getComment();
    }

    protected final ResolvedCatalogMaterializedTable getResolvedCatalogMaterializedTable(
            T sqlCreateMaterializedTable, ConvertContext context) {
        final MergeContext mergeContext = getMergeContext(sqlCreateMaterializedTable, context);
        final List<String> partitionKeys = mergeContext.getMergedPartitionKeys();
        final Schema schema = mergeContext.getMergedSchema();
        final ResolvedSchema querySchema = mergeContext.getMergedQuerySchema();
        final Map<String, String> tableOptions = mergeContext.getMergedTableOptions();
        verifyPartitioningColumnsExist(querySchema, partitionKeys, tableOptions);

        final TableDistribution distribution =
                mergeContext.getMergedTableDistribution().orElse(null);
        final String comment = sqlCreateMaterializedTable.getComment();

        final String originalQuery = mergeContext.getMergedOriginalQuery();
        final String expandedQuery = mergeContext.getMergedExpandedQuery();

        final IntervalFreshness intervalFreshness = getDerivedFreshness(sqlCreateMaterializedTable);

        final LogicalRefreshMode logicalRefreshMode =
                getDerivedLogicalRefreshMode(sqlCreateMaterializedTable);

        final RefreshMode refreshMode = getDerivedRefreshMode(logicalRefreshMode);

        return context.getCatalogManager()
                .resolveCatalogMaterializedTable(
                        CatalogMaterializedTable.newBuilder()
                                .schema(schema)
                                .comment(comment)
                                .distribution(distribution)
                                .partitionKeys(partitionKeys)
                                .options(tableOptions)
                                .originalQuery(originalQuery)
                                .expandedQuery(expandedQuery)
                                .freshness(intervalFreshness)
                                .logicalRefreshMode(logicalRefreshMode)
                                .refreshMode(refreshMode)
                                .refreshStatus(RefreshStatus.INITIALIZING)
                                .build());
    }

    protected final ObjectIdentifier getIdentifier(
            SqlCreateMaterializedTable node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.getFullName());
        return context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
    }

    private void verifyPartitioningColumnsExist(
            ResolvedSchema schema, List<String> partitionKeys, Map<String, String> tableOptions) {
        final Set<String> partitionFieldOptions =
                tableOptions.keySet().stream()
                        .filter(k -> k.startsWith(PARTITION_FIELDS))
                        .collect(Collectors.toSet());

        for (String partitionKey : partitionKeys) {
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
        for (String partitionOption : partitionFieldOptions) {
            String partitionKey =
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
            LogicalType partitionKeyType =
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
}
