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

import org.apache.flink.sql.parser.ddl.SqlAlterDatabase;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterDatabaseOperation;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SqlAlterDatabaseConverter implements SqlNodeConverter<SqlAlterDatabase> {
    @Override
    public Operation convertSqlNode(SqlAlterDatabase node, ConvertContext context) {
        String[] fullDatabaseName = node.fullDatabaseName();
        if (fullDatabaseName.length > 2) {
            throw new ValidationException("alter database identifier format error");
        }
        String catalogName =
                (fullDatabaseName.length == 1)
                        ? context.getCatalogManager().getCurrentCatalog()
                        : fullDatabaseName[0];
        String databaseName =
                (fullDatabaseName.length == 1) ? fullDatabaseName[0] : fullDatabaseName[1];
        final Map<String, String> properties;
        CatalogDatabase originCatalogDatabase;
        Optional<Catalog> catalog = context.getCatalogManager().getCatalog(catalogName);
        if (catalog.isPresent()) {
            try {
                originCatalogDatabase = catalog.get().getDatabase(databaseName);
                properties = new HashMap<>(originCatalogDatabase.getProperties());
            } catch (DatabaseNotExistException e) {
                throw new ValidationException(
                        String.format("Database %s not exists", databaseName), e);
            }
        } else {
            throw new ValidationException(String.format("Catalog %s not exists", catalogName));
        }
        // set with properties
        node.getPropertyList()
                .getList()
                .forEach(
                        p ->
                                properties.put(
                                        ((SqlTableOption) p).getKeyString(),
                                        ((SqlTableOption) p).getValueString()));
        CatalogDatabase catalogDatabase =
                new CatalogDatabaseImpl(properties, originCatalogDatabase.getComment());
        return new AlterDatabaseOperation(catalogName, databaseName, catalogDatabase);
    }
}
