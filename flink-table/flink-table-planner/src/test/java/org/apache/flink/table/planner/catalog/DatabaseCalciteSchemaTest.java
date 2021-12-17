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
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.utils.CatalogManagerMocks;

import org.apache.calcite.schema.Table;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_CATALOG;
import static org.apache.flink.table.utils.CatalogManagerMocks.DEFAULT_DATABASE;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** Tests for {@link DatabaseCalciteSchema}. */
public class DatabaseCalciteSchemaTest {

    private static final String TABLE_NAME = "tab";

    @Test
    public void testPermanentTableWithPrimaryKey() {
        final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();

        final DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema(DEFAULT_CATALOG, DEFAULT_DATABASE, catalogManager, true);

        catalogManager.createTable(
                createTable(),
                ObjectIdentifier.of(DEFAULT_CATALOG, DEFAULT_DATABASE, TABLE_NAME),
                false);

        final Table table = calciteSchema.getTable(TABLE_NAME);
        assertThat(table, instanceOf(CatalogSchemaTable.class));
        assertThat(
                ((CatalogSchemaTable) table).getStatistic().getUniqueKeys().iterator().next(),
                containsInAnyOrder("a", "b"));
    }

    @Test
    public void testTemporaryTableWithPrimaryKey() {
        final CatalogManager catalogManager = CatalogManagerMocks.createEmptyCatalogManager();

        final DatabaseCalciteSchema calciteSchema =
                new DatabaseCalciteSchema("other_catalog", "other_database", catalogManager, true);

        catalogManager.createTemporaryTable(
                createTable(),
                ObjectIdentifier.of("other_catalog", "other_database", TABLE_NAME),
                false);

        final Table table = calciteSchema.getTable(TABLE_NAME);
        assertThat(table, instanceOf(CatalogSchemaTable.class));
        assertThat(
                ((CatalogSchemaTable) table).getStatistic().getUniqueKeys().iterator().next(),
                containsInAnyOrder("a", "b"));
    }

    private CatalogBaseTable createTable() {
        final Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT().notNull())
                        .column("b", DataTypes.STRING().notNull())
                        .column("c", DataTypes.STRING())
                        .primaryKey("a", "b")
                        .build();

        return CatalogTable.of(schema, null, new ArrayList<>(), new HashMap<>());
    }
}
