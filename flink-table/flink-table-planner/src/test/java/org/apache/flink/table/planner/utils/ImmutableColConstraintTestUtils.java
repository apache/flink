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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ImmutableColumnsConstraint;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.List;

/**
 * Utils about {@link ImmutableColumnsConstraint} for tests.
 *
 * <p>This utils can be removed after we support syntax to define immutable columns constraint in
 * DDL.
 */
public class ImmutableColConstraintTestUtils {

    public static void addImmutableColConstraint(
            Catalog catalog, String databaseName, String tableName, String... immutableCols)
            throws Exception {
        ObjectPath tablePath = new ObjectPath(databaseName, tableName);
        CatalogTable originalTable = (CatalogTable) catalog.getTable(tablePath);
        catalog.dropTable(tablePath, false);

        Schema.UnresolvedImmutableColumns immutableColumns =
                new Schema.UnresolvedImmutableColumns("imt", List.of(immutableCols));

        Schema schema = originalTable.getUnresolvedSchema();
        schema =
                new Schema(
                        schema.getColumns(),
                        schema.getWatermarkSpecs(),
                        schema.getPrimaryKey().orElse(null),
                        schema.getIndexes(),
                        immutableColumns);

        CatalogTable newTable =
                CatalogTable.newBuilder()
                        .schema(schema)
                        .comment(originalTable.getComment())
                        .partitionKeys(originalTable.getPartitionKeys())
                        .options(originalTable.getOptions())
                        .build();

        catalog.createTable(tablePath, newTable, false);
    }
}
