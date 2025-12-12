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

import org.apache.flink.sql.parser.ddl.table.SqlAlterTableRename;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableRenameOperation;

/**
 * Convert ALTER TABLE [IF EXISTS] [[catalogName.] dataBasesName].tableName RENAME TO
 * [[catalogName.] dataBasesName].newTableName statemen.
 */
public class SqlAlterTableRenameConverter extends AbstractAlterTableConverter<SqlAlterTableRename> {
    @Override
    protected Operation convertToOperation(
            SqlAlterTableRename sqlAlterTable,
            ResolvedCatalogTable oldTable,
            ConvertContext context) {
        final ObjectIdentifier tableIdentifier = resolveIdentifier(sqlAlterTable, context);
        UnresolvedIdentifier newUnresolvedIdentifier =
                UnresolvedIdentifier.of(sqlAlterTable.fullNewTableName());
        ObjectIdentifier newTableIdentifier =
                context.getCatalogManager().qualifyIdentifier(newUnresolvedIdentifier);
        return new AlterTableRenameOperation(
                tableIdentifier, newTableIdentifier, sqlAlterTable.ifTableExists());
    }
}
