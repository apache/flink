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

import org.apache.flink.sql.parser.ddl.SqlUseDatabase;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.UseDatabaseOperation;

public class SqlUseDatabaseConverter implements SqlNodeConverter<SqlUseDatabase> {
    @Override
    public Operation convertSqlNode(SqlUseDatabase node, ConvertContext context) {
        String[] fullDatabaseName = node.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("use database identifier format error");
        }
        String catalogName =
                fullDatabaseName.length == 2
                        ? fullDatabaseName[0]
                        : context.getCatalogManager().getCurrentCatalog();
        String databaseName =
                fullDatabaseName.length == 2 ? fullDatabaseName[1] : fullDatabaseName[0];
        return new UseDatabaseOperation(catalogName, databaseName);
    }
}
