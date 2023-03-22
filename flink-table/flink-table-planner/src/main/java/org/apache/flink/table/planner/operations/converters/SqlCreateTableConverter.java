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

import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateTableLike;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;

import org.apache.calcite.sql.SqlKind;

import static org.apache.flink.table.planner.operations.CreateTableConverterUtils.createCatalogTable;
import java.util.EnumSet;
import java.util.Optional;


/** A converter for {@link SqlCreateTable}. */
public class SqlCreateTableConverter implements SqlNodeConverter<SqlCreateTable> {

    @Override
    public Operation convertSqlNode(SqlCreateTable node, ConvertContext context) {
        return convertCreateTable(context, node);
    }


    @Override
    public Optional<EnumSet<SqlKind>> supportedSqlKinds() {
        return Optional.of(EnumSet.of(SqlKind.CREATE_TABLE));
    }

    /** Convert {@link SqlCreateTable} or {@link SqlCreateTableLike} node. */
    public Operation convertCreateTable(
            SqlNodeConverter.ConvertContext context, SqlCreateTable sqlCreateTable) {
        CatalogTable catalogTable = createCatalogTable(context, sqlCreateTable);

        UnresolvedIdentifier unresolvedIdentifier =
                UnresolvedIdentifier.of(sqlCreateTable.fullTableName());
        ObjectIdentifier identifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);

        return new CreateTableOperation(
                identifier,
                catalogTable,
                sqlCreateTable.isIfNotExists(),
                sqlCreateTable.isTemporary());
    }
}
