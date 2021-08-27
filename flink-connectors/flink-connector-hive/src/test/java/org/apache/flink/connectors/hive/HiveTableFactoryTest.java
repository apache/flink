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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

/** Test for {@link HiveTableFactory}. */
public class HiveTableFactoryTest {
    private static HiveCatalog catalog;

    @BeforeClass
    public static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    @AfterClass
    public static void close() {
        catalog.close();
    }

    @Test
    public void testGenericTable() throws Exception {
        TableSchema schema =
                TableSchema.builder()
                        .field("name", DataTypes.STRING())
                        .field("age", DataTypes.INT())
                        .build();

        Map<String, String> properties = new HashMap<>();
        properties.put(FactoryUtil.CONNECTOR.key(), "COLLECTION");

        catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
        ObjectPath path = new ObjectPath("mydb", "mytable");
        CatalogTable table = new CatalogTableImpl(schema, properties, "csv table");
        catalog.createTable(path, table, true);
        Optional<TableFactory> opt = catalog.getTableFactory();
        assertTrue(opt.isPresent());
        HiveTableFactory tableFactory = (HiveTableFactory) opt.get();
        TableSource tableSource =
                tableFactory.createTableSource(
                        new TableSourceFactoryContextImpl(
                                ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                                table,
                                new Configuration(),
                                false));
        assertTrue(tableSource instanceof StreamTableSource);
        TableSink tableSink =
                tableFactory.createTableSink(
                        new TableSinkFactoryContextImpl(
                                ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                                table,
                                new Configuration(),
                                true,
                                false));
        assertTrue(tableSink instanceof StreamTableSink);
    }

    @Test
    public void testHiveTable() throws Exception {
        ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT()));

        Map<String, String> properties = new HashMap<>();
        properties.put(FactoryUtil.CONNECTOR.key(), SqlCreateHiveTable.IDENTIFIER);

        catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
        ObjectPath path = new ObjectPath("mydb", "mytable");
        CatalogTable table =
                new CatalogTableImpl(
                        TableSchema.fromResolvedSchema(schema), properties, "hive table");
        catalog.createTable(path, table, true);

        DynamicTableSource tableSource =
                FactoryUtil.createTableSource(
                        catalog,
                        ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                        new ResolvedCatalogTable(table, schema),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        assertTrue(tableSource instanceof HiveTableSource);

        DynamicTableSink tableSink =
                FactoryUtil.createTableSink(
                        catalog,
                        ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                        new ResolvedCatalogTable(table, schema),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        assertTrue(tableSink instanceof HiveTableSink);
    }
}
