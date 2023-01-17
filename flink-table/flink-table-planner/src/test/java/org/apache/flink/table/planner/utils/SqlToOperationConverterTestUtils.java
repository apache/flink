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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Utils for {@link org.apache.flink.table.planner.operations.SqlToOperationConverterTest} . */
public class SqlToOperationConverterTestUtils {

    private final CatalogManager catalogManager;

    public SqlToOperationConverterTestUtils(CatalogManager catalogManager) {
        this.catalogManager = catalogManager;
    }

    public void prepareTable(
            String tableName,
            boolean managedTable,
            boolean hasPartition,
            boolean hasWatermark,
            int numOfPkFields)
            throws Exception {
        Catalog catalog = new GenericInMemoryCatalog("default", "default");
        if (!catalogManager.getCatalog("cat1").isPresent()) {
            catalogManager.registerCatalog("cat1", catalog);
        }
        catalog.createDatabase("db1", new CatalogDatabaseImpl(new HashMap<>(), null), true);
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.BIGINT().notNull())
                        .column("c", DataTypes.STRING().notNull())
                        .withComment("column comment")
                        .columnByExpression("d", "a*(b+2 + a*b)")
                        .column(
                                "e",
                                DataTypes.ROW(
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.DOUBLE(),
                                                DataTypes.ARRAY(DataTypes.FLOAT()))))
                        .columnByExpression("f", "e.f1 + e.f2.f0")
                        .columnByMetadata("g", DataTypes.STRING(), null, true)
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .withComment("just a comment");
        Map<String, String> options = new HashMap<>();
        options.put("k", "v");
        if (!managedTable) {
            options.put("connector", "dummy");
        }
        if (numOfPkFields == 0) {
            // do nothing
        } else if (numOfPkFields == 1) {
            builder.primaryKeyNamed("ct1", "a");
        } else if (numOfPkFields == 2) {
            builder.primaryKeyNamed("ct1", "a", "b");
        } else if (numOfPkFields == 3) {
            builder.primaryKeyNamed("ct1", "a", "b", "c");
        } else {
            throw new IllegalArgumentException(
                    String.format("Don't support to set pk with %s fields.", numOfPkFields));
        }

        if (hasWatermark) {
            builder.watermark("ts", "ts - interval '5' seconds");
        }
        CatalogTable catalogTable =
                CatalogTable.of(
                        builder.build(),
                        "a table",
                        hasPartition ? Arrays.asList("b", "c") : Collections.emptyList(),
                        Collections.unmodifiableMap(options));
        catalogManager.setCurrentCatalog("cat1");
        catalogManager.setCurrentDatabase("db1");
        ObjectIdentifier tableIdentifier = ObjectIdentifier.of("cat1", "db1", tableName);
        catalogManager.createTable(catalogTable, tableIdentifier, true);
    }
}
