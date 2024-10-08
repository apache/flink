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

import org.apache.flink.sql.parser.ddl.SqlReplaceTableAs;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.MergeTableAsUtil;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** A converter for {@link SqlReplaceTableAs}. */
public class SqlReplaceTableAsConverter implements SqlNodeConverter<SqlReplaceTableAs> {

    @Override
    public Operation convertSqlNode(SqlReplaceTableAs sqlReplaceTableAs, ConvertContext context) {
        CatalogManager catalogManager = context.getCatalogManager();
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlReplaceTableAs.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();

        MergeTableAsUtil mergeTableAsUtil =
                new MergeTableAsUtil(
                        context.getSqlValidator(),
                        sqlNode -> sqlNode.toString(),
                        catalogManager.getDataTypeFactory());

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeToOperationConversion.convert(
                                        flinkPlanner,
                                        catalogManager,
                                        sqlReplaceTableAs.getAsQuery())
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "RTAS unsupported node type "
                                                                + sqlReplaceTableAs
                                                                        .getAsQuery()
                                                                        .getClass()
                                                                        .getSimpleName()));

        // get table
        ResolvedCatalogTable tableWithResolvedSchema =
                createCatalogTable(
                        context, mergeTableAsUtil, sqlReplaceTableAs, query.getResolvedSchema());

        // If needed, rewrite the query to include the new sink fields in the select list
        query =
                mergeTableAsUtil.maybeRewriteQuery(
                        context.getCatalogManager(),
                        flinkPlanner,
                        query,
                        sqlReplaceTableAs.getAsQuery(),
                        tableWithResolvedSchema);

        CreateTableOperation createTableOperation =
                new CreateTableOperation(
                        identifier,
                        tableWithResolvedSchema,
                        sqlReplaceTableAs.isIfNotExists(),
                        sqlReplaceTableAs.isTemporary());

        return new ReplaceTableAsOperation(
                createTableOperation, query, sqlReplaceTableAs.isCreateOrReplace());
    }

    private ResolvedCatalogTable createCatalogTable(
            ConvertContext context,
            MergeTableAsUtil mergeTableAsUtil,
            SqlReplaceTableAs sqlReplaceTableAs,
            ResolvedSchema querySchema) {
        CatalogManager catalogManager = context.getCatalogManager();

        // get table comment
        String tableComment =
                OperationConverterUtils.getTableComment(sqlReplaceTableAs.getComment());

        // get table properties
        Map<String, String> properties = new HashMap<>();
        sqlReplaceTableAs
                .getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));

        Schema mergedSchema;
        if (sqlReplaceTableAs.isSchemaWithColumnsIdentifiersOnly()) {
            // If only column identifiers are provided, then these are used to
            // order the columns in the schema.
            mergedSchema =
                    mergeTableAsUtil.reorderSchema(sqlReplaceTableAs.getColumnList(), querySchema);
        } else {
            // merge schemas
            mergedSchema =
                    mergeTableAsUtil.mergeSchemas(
                            sqlReplaceTableAs.getColumnList(),
                            sqlReplaceTableAs.getWatermark().orElse(null),
                            sqlReplaceTableAs.getFullConstraints(),
                            querySchema);
        }

        // get distribution
        Optional<TableDistribution> tableDistribution =
                Optional.ofNullable(sqlReplaceTableAs.getDistribution())
                        .map(OperationConverterUtils::getDistributionFromSqlDistribution);

        // get partition key
        List<String> partitionKeys =
                getPartitionKeyColumnNames(sqlReplaceTableAs.getPartitionKeyList());
        verifyPartitioningColumnsExist(mergedSchema, partitionKeys);

        CatalogTable catalogTable =
                CatalogTable.newBuilder()
                        .schema(mergedSchema)
                        .comment(tableComment)
                        .distribution(tableDistribution.orElse(null))
                        .options(properties)
                        .partitionKeys(partitionKeys)
                        .build();

        return catalogManager.resolveCatalogTable(catalogTable);
    }

    private List<String> getPartitionKeyColumnNames(SqlNodeList partitionKey) {
        return partitionKey.getList().stream()
                .map(p -> ((SqlIdentifier) p).getSimple())
                .collect(Collectors.toList());
    }

    private void verifyPartitioningColumnsExist(Schema mergedSchema, List<String> partitionKeys) {
        Set<String> columnNames =
                mergedSchema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toCollection(LinkedHashSet::new));
        for (String partitionKey : partitionKeys) {
            if (!columnNames.contains(partitionKey)) {
                throw new ValidationException(
                        String.format(
                                "Partition column '%s' not defined in the table schema. Available columns: [%s]",
                                partitionKey,
                                columnNames.stream()
                                        .collect(Collectors.joining("', '", "'", "'"))));
            }
        }
    }
}
