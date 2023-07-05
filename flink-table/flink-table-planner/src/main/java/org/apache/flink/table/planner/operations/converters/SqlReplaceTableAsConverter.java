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
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.ReplaceTableAsOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.utils.OperationConverterUtils;

import org.apache.calcite.sql.SqlNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A converter for {@link SqlReplaceTableAs}. */
public class SqlReplaceTableAsConverter implements SqlNodeConverter<SqlReplaceTableAs> {

    @Override
    public Operation convertSqlNode(SqlReplaceTableAs sqlReplaceTableAs, ConvertContext context) {
        CatalogManager catalogManager = context.getCatalogManager();
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlReplaceTableAs.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        SqlNode asQuerySqlNode = sqlReplaceTableAs.getAsQuery();
        context.getSqlValidator().validate(asQuerySqlNode);
        QueryOperation query =
                new PlannerQueryOperation(context.toRelRoot(asQuerySqlNode).project());

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

        // get table
        CatalogTable catalogTable =
                CatalogTable.of(
                        Schema.newBuilder().fromResolvedSchema(query.getResolvedSchema()).build(),
                        tableComment,
                        Collections.emptyList(),
                        properties);

        CreateTableOperation createTableOperation =
                new CreateTableOperation(
                        identifier,
                        catalogTable,
                        sqlReplaceTableAs.isIfNotExists(),
                        sqlReplaceTableAs.isTemporary());

        return new ReplaceTableAsOperation(
                createTableOperation, query, sqlReplaceTableAs.isCreateOrReplace());
    }
}
