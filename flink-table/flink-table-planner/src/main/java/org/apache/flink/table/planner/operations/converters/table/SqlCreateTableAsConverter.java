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

package org.apache.flink.table.planner.operations.converters.table;

import org.apache.flink.sql.parser.ddl.table.SqlCreateTableAs;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;

import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Helper class for converting {@link SqlCreateTableAs} to {@link CreateTableASOperation}. */
public class SqlCreateTableAsConverter extends AbstractCreateTableConverter<SqlCreateTableAs> {

    @Override
    public Operation convertSqlNode(SqlCreateTableAs sqlCreateTableAs, ConvertContext context) {
        final FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();
        final CatalogManager catalogManager = context.getCatalogManager();
        SqlNode asQuerySqlNode = sqlCreateTableAs.getAsQuery();
        SqlNode validatedAsQuery = flinkPlanner.validate(asQuerySqlNode);

        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeToOperationConversion.convert(
                                        flinkPlanner, catalogManager, validatedAsQuery)
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "CTAS unsupported node type "
                                                                + validatedAsQuery
                                                                        .getClass()
                                                                        .getSimpleName()));
        ResolvedCatalogTable tableWithResolvedSchema =
                getResolvedCatalogTable(sqlCreateTableAs, context, query.getResolvedSchema());

        // If needed, rewrite the query to include the new sink fields in the select list
        query =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                catalogManager,
                                flinkPlanner,
                                query,
                                validatedAsQuery,
                                tableWithResolvedSchema);

        ObjectIdentifier identifier = getIdentifier(sqlCreateTableAs, context);
        CreateTableOperation createTableOperation =
                getCreateTableOperation(identifier, tableWithResolvedSchema, sqlCreateTableAs);

        return new CreateTableASOperation(createTableOperation, Map.of(), query, false);
    }

    @Override
    protected MergeContext getMergeContext(
            SqlCreateTableAs sqlCreateTableAs, ConvertContext context) {
        return new MergeContext() {
            private final MergeTableAsUtil mergeTableAsUtil = new MergeTableAsUtil(context);

            @Override
            public Schema getMergedSchema(ResolvedSchema schemaToMerge) {
                if (sqlCreateTableAs.isSchemaWithColumnsIdentifiersOnly()) {
                    // If only column identifiers are provided, then these are used to
                    // order the columns in the schema.
                    return mergeTableAsUtil.reorderSchema(
                            sqlCreateTableAs.getColumnList(), schemaToMerge);
                } else {
                    return mergeTableAsUtil.mergeSchemas(
                            sqlCreateTableAs.getColumnList(),
                            sqlCreateTableAs.getWatermark().orElse(null),
                            sqlCreateTableAs.getFullConstraints(),
                            schemaToMerge);
                }
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                return sqlCreateTableAs.getProperties();
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return sqlCreateTableAs.getPartitionKeyList();
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlCreateTableAsConverter.this.getDerivedTableDistribution(sqlCreateTableAs);
            }
        };
    }
}
