
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

import org.apache.flink.sql.parser.ddl.SqlCreateMaterializedTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.materializedtable.CreateMaterializedTableOperation;
import org.apache.flink.table.planner.operations.converters.table.MergeTableAsUtil;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A converter for {@link SqlCreateMaterializedTable} to {@link CreateMaterializedTableOperation}.
 */
public class SqlCreateMaterializedTableConverter
        extends AbstractCreateMaterializedTableConverter<SqlCreateMaterializedTable> {

    @Override
    public Operation convertSqlNode(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        return new CreateMaterializedTableOperation(
                getIdentifier(sqlCreateMaterializedTable, context),
                getResolvedCatalogMaterializedTable(sqlCreateMaterializedTable, context));
    }

    @Override
    protected MergeContext getMergeContext(
            SqlCreateMaterializedTable sqlCreateMaterializedTable, ConvertContext context) {
        return new MergeContext() {
            private final MergeTableAsUtil mergeTableAsUtil = new MergeTableAsUtil(context);
            private final String definitionQuery =
                    SqlCreateMaterializedTableConverter.this.getDerivedDefinitionQuery(
                            sqlCreateMaterializedTable, context);
            private final ResolvedSchema querySchema =
                    SqlCreateMaterializedTableConverter.this.getQueryResolvedSchema(
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
                return SqlCreateMaterializedTableConverter.this.getDerivedTableOptions(
                        sqlCreateMaterializedTable);
            }

            @Override
            public List<String> getMergedPartitionKeys() {
                return SqlCreateMaterializedTableConverter.this.getDerivedPartitionKeys(
                        sqlCreateMaterializedTable);
            }

            @Override
            public Optional<TableDistribution> getMergedTableDistribution() {
                return SqlCreateMaterializedTableConverter.this.getDerivedTableDistribution(
                        sqlCreateMaterializedTable);
            }

            @Override
            public String getMergedDefinitionQuery() {
                return definitionQuery;
            }

            @Override
            public ResolvedSchema getMergedQuerySchema() {
                return querySchema;
            }
        };
    }
}
