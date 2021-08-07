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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.table.utils.ExpressionResolverMocks;

import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Tests for {@link DatabaseCalciteSchema}. */
public class DatabaseCalciteSchemaTest {

    private static final String catalogName = "cat";
    private static final String databaseName = "db";
    private static final String tableName = "tab";

    @Test
    public void testGetTableWithPrimaryKey() {
        GenericInMemoryCatalog catalog = new GenericInMemoryCatalog(catalogName, databaseName);
        CatalogManager catalogManager =
                CatalogManagerMocks.preparedCatalogManager()
                        .defaultCatalog(catalogName, catalog)
                        .build();
        catalogManager.initSchemaResolver(true, ExpressionResolverMocks.dummyResolver());

        DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema(databaseName, catalogName, catalogManager, true);

        catalogManager.createTable(
                createTable(), ObjectIdentifier.of(catalogName, databaseName, tableName), false);

        Table table = calciteSchema.getTable(tableName);
        assertThat(table, instanceOf(CatalogSchemaTable.class));
        assertThat(
                ((CatalogSchemaTable) table).getStatistic().getUniqueKeys().iterator().next(),
                containsInAnyOrder("a", "b"));
    }

    private CatalogBaseTable createTable() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();

        return CatalogTable.of(schema, null, new ArrayList<>(), new HashMap<>());
    }
}
