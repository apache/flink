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

import org.apache.flink.sql.parser.ddl.SqlReplaceTableAs;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/** A converter for {@link SqlReplaceTableAs}. */
public class SqlReplaceTableAsConverter extends AbstractCreateTableConverter<SqlReplaceTableAs> {

    @Override
    public Operation convertSqlNode(SqlReplaceTableAs sqlReplaceTableAs, ConvertContext context) {
        CatalogManager catalogManager = context.getCatalogManager();
        FlinkPlannerImpl flinkPlanner = context.getFlinkPlanner();

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
                getResolvedCatalogTable(sqlReplaceTableAs, context, query.getResolvedSchema());

        // If needed, rewrite the query to include the new sink fields in the select list
        query =
                new MergeTableAsUtil(context)
                        .maybeRewriteQuery(
                                context.getCatalogManager(),
                                flinkPlanner,
                                query,
                                sqlReplaceTableAs.getAsQuery(),
                                tableWithResolvedSchema);

        ObjectIdentifier identifier = getIdentifier(sqlReplaceTableAs, context);
        CreateTableOperation createTableOperation =
                getCreateTableOperation(identifier, tableWithResolvedSchema, sqlReplaceTableAs);

        return new ReplaceTableAsOperation(
                createTableOperation, query, sqlReplaceTableAs.isCreateOrReplace());
    }

    @Override
    protected MergeContext getMergeContext(
            SqlReplaceTableAs sqlReplaceTableAs, ConvertContext context) {
        return new MergeContext() {
            private final MergeTableAsUtil mergeTableAsUtil = new MergeTableAsUtil(context);

            @Override
            public Schema getMergedSchema(ResolvedSchema querySchema) {
                if (sqlReplaceTableAs.isSchemaWithColumnsIdentifiersOnly()) {
                    // If only column identifiers are provided, then these are used to
                    // order the columns in the schema.
                    return mergeTableAsUtil.reorderSchema(
                            sqlReplaceTableAs.getColumnList(), querySchema);
                } else {
                    return mergeTableAsUtil.mergeSchemas(
                            sqlReplaceTableAs.getColumnList(),
                            sqlReplaceTableAs.getWatermark().orElse(null),
                            sqlReplaceTableAs.getFullConstraints(),
                            querySchema);
                }
            }

            @Override
            public Map<String, String> getMergedTableOptions() {
                return sqlReplaceTableAs.getProperties();
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return sqlReplaceTableAs.getPartitionKeyList();
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlReplaceTableAsConverter.this.getDerivedTableDistribution(
                        sqlReplaceTableAs);
            }
        };
    }
}
