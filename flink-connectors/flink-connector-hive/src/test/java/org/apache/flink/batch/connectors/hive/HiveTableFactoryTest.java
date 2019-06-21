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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.factories.TableFactory;
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

/**
 * Test for {@link org.apache.flink.batch.connectors.hive.HiveTableFactory}.
 */
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
	public void testCsvTable() throws Exception {
		TableSchema schema = TableSchema.builder()
			.field("name", DataTypes.STRING())
			.field("age", DataTypes.INT())
			.build();

		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogConfig.IS_GENERIC, String.valueOf(true));
		properties.put("connector.type", "filesystem");
		properties.put("connector.path", "/tmp");
		properties.put("connector.property-version", "1");
		properties.put("update-mode", "append");

		properties.put("format.type", "csv");
		properties.put("format.property-version", "1");
		properties.put("format.fields.0.name", "name");
		properties.put("format.fields.0.type", "STRING");
		properties.put("format.fields.1.name", "age");
		properties.put("format.fields.1.type", "INT");

		CatalogTable table = new CatalogTableImpl(schema, properties, "csv table");

		catalog.createDatabase("mydb", new CatalogDatabaseImpl(new HashMap<>(), ""), true);
		ObjectPath path = new ObjectPath("mydb", "mytable");
		catalog.createTable(path, table, true);
		Optional<TableFactory> opt = catalog.getTableFactory();
		assertTrue(opt.isPresent());
		HiveTableFactory tableFactory = (HiveTableFactory) opt.get();
		TableSource tableSource = tableFactory.createTableSource(table);
		assertTrue(tableSource instanceof StreamTableSource);
		TableSink tableSink = tableFactory.createTableSink(table);
		assertTrue(tableSink instanceof StreamTableSink);
	}

}
