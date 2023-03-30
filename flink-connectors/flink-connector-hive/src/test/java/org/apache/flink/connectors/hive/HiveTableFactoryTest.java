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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactoryContextImpl;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HiveTableFactory}. */
@ExtendWith(TestLoggerExtension.class)
class HiveTableFactoryTest {
    private static HiveCatalog catalog;

    @BeforeAll
    static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    @AfterAll
    static void close() {
        catalog.close();
    }

    @Test
    void testGenericTable() throws Exception {
        final ResolvedSchema resolvedSchema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT()));
        final Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();

        catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);

        final Map<String, String> options =
                Collections.singletonMap(FactoryUtil.CONNECTOR.key(), "COLLECTION");
        final CatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.of(schema, "csv table", new ArrayList<>(), options),
                        resolvedSchema);
        catalog.createTable(new ObjectPath("mydb", "mytable"), table, true);

        final Optional<TableFactory> tableFactoryOpt = catalog.getTableFactory();
        assertThat(tableFactoryOpt).isPresent();
        final HiveTableFactory tableFactory = (HiveTableFactory) tableFactoryOpt.get();

        final TableSource tableSource =
                tableFactory.createTableSource(
                        new TableSourceFactoryContextImpl(
                                ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                                table,
                                new Configuration(),
                                false));
        assertThat(tableSource).isInstanceOf(StreamTableSource.class);

        final TableSink tableSink =
                tableFactory.createTableSink(
                        new TableSinkFactoryContextImpl(
                                ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                                table,
                                new Configuration(),
                                true,
                                false));
        assertThat(tableSink).isInstanceOf(StreamTableSink.class);
    }

    @Test
    void testHiveTable() throws Exception {
        final ResolvedSchema schema =
                ResolvedSchema.of(
                        Column.physical("name", DataTypes.STRING()),
                        Column.physical("age", DataTypes.INT()));

        catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);

        final Map<String, String> options =
                Collections.singletonMap(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
        final CatalogTable table =
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "hive table",
                                new ArrayList<>(),
                                options),
                        schema);
        catalog.createTable(new ObjectPath("mydb", "mytable"), table, true);

        final DynamicTableSource tableSource =
                FactoryUtil.createDynamicTableSource(
                        (DynamicTableSourceFactory)
                                catalog.getFactory().orElseThrow(IllegalStateException::new),
                        ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                        new ResolvedCatalogTable(table, schema),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        assertThat(tableSource).isInstanceOf(HiveTableSource.class);

        final DynamicTableSink tableSink =
                FactoryUtil.createDynamicTableSink(
                        (DynamicTableSinkFactory)
                                catalog.getFactory().orElseThrow(IllegalStateException::new),
                        ObjectIdentifier.of("mycatalog", "mydb", "mytable"),
                        new ResolvedCatalogTable(table, schema),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader(),
                        false);
        assertThat(tableSink).isInstanceOf(HiveTableSink.class);
    }
}
