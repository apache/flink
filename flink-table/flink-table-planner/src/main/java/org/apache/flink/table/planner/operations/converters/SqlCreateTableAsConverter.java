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

import org.apache.flink.sql.parser.ddl.SqlCreateTableAs;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.CreateTableASOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.sql.SqlNode;

import java.util.Collections;

import static org.apache.flink.table.planner.operations.CreateTableConverterUtils.createCatalogTable;

/** A converter for {@link SqlCreateTableAs}. */
public class SqlCreateTableAsConverter implements SqlNodeConverter<SqlCreateTableAs> {
    @Override
    public Operation convertSqlNode(SqlCreateTableAs node, ConvertContext context) {
        return convertCreateTableAs(context, node);
    }

    public Operation convertCreateTableAs(
            SqlNodeConverter.ConvertContext context, SqlCreateTableAs sqlCreateTableAs) {
        CatalogManager catalogManager = context.getCatalogManager();
        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTableAs.fullTableName());
        ObjectIdentifier identifier = catalogManager.qualifyIdentifier(unresolvedIdentifier);

        SqlNode asQuery = sqlCreateTableAs.getAsQuery();
        SqlNode validate = context.getSqlValidator().validate(asQuery);
        PlannerQueryOperation query =
                (PlannerQueryOperation)
                        SqlNodeConverters.convertSqlNode(validate, context)
                                .orElseThrow(
                                        () ->
                                                new TableException(
                                                        "CTAS unsupported node type "
                                                                + asQuery.getClass()
                                                                        .getSimpleName()));
        CatalogTable catalogTable = createCatalogTable(context, sqlCreateTableAs);

        CreateTableOperation createTableOperation =
                new CreateTableOperation(
                        identifier,
                        CatalogTable.of(
                                Schema.newBuilder()
                                        .fromResolvedSchema(query.getResolvedSchema())
                                        .build(),
                                catalogTable.getComment(),
                                catalogTable.getPartitionKeys(),
                                catalogTable.getOptions()),
                        sqlCreateTableAs.isIfNotExists(),
                        sqlCreateTableAs.isTemporary());

        return new CreateTableASOperation(
                createTableOperation, Collections.emptyMap(), query, false);
    }
}
