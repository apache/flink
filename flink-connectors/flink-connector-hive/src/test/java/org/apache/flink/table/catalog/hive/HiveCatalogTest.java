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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.config.CatalogConfig;
import org.apache.flink.table.descriptors.FileSystem;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for HiveCatalog.
 */
public class HiveCatalogTest {

	TableSchema schema = TableSchema.builder()
		.field("name", DataTypes.STRING())
		.field("age", DataTypes.INT())
		.build();

	@Test
	public void testCreateGenericTable() {
		Table hiveTable = HiveCatalog.instantiateHiveTable(
			new ObjectPath("test", "test"),
			new CatalogTableImpl(
				schema,
				new FileSystem().path("/test_path").toProperties(),
				null
			));

		Map<String, String> prop = hiveTable.getParameters();
		assertEquals(prop.remove(CatalogConfig.IS_GENERIC), String.valueOf("true"));
		assertTrue(prop.keySet().stream().allMatch(k -> k.startsWith(CatalogConfig.FLINK_PROPERTY_PREFIX)));
	}

	@Test
	public void testCreateHiveTable() {
		Map<String, String> map = new HashMap<>(new FileSystem().path("/test_path").toProperties());

		map.put(CatalogConfig.IS_GENERIC, String.valueOf(false));

		Table hiveTable = HiveCatalog.instantiateHiveTable(
			new ObjectPath("test", "test"),
			new CatalogTableImpl(
				schema,
				map,
				null
			));

		Map<String, String> prop = hiveTable.getParameters();
		assertEquals(prop.remove(CatalogConfig.IS_GENERIC), String.valueOf(false));
		assertTrue(prop.keySet().stream().noneMatch(k -> k.startsWith(CatalogConfig.FLINK_PROPERTY_PREFIX)));
	}
}
