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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.GenericCatalogDatabase;
import org.apache.flink.table.catalog.GenericCatalogTable;
import org.apache.flink.table.catalog.GenericCatalogView;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * Test for HiveCatalog on generic metadata.
 */
public class HiveCatalogGenericMetadataTest extends CatalogTestBase {

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createHiveCatalog();
		catalog.open();
	}

	// ------ data types ------

	@Test
	public void testDataTypes() throws Exception {
		// TODO: the following Hive types are not supported in Flink yet, including CHAR, VARCHAR, DECIMAL, MAP, STRUCT
		//	  [FLINK-12386] Support complete mapping between Flink and Hive data types
		TypeInformation[] types = new TypeInformation[] {
			BasicTypeInfo.BYTE_TYPE_INFO,
			BasicTypeInfo.SHORT_TYPE_INFO,
			BasicTypeInfo.INT_TYPE_INFO,
			BasicTypeInfo.LONG_TYPE_INFO,
			BasicTypeInfo.FLOAT_TYPE_INFO,
			BasicTypeInfo.DOUBLE_TYPE_INFO,
			BasicTypeInfo.BOOLEAN_TYPE_INFO,
			BasicTypeInfo.STRING_TYPE_INFO,
			BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO,
			BasicTypeInfo.DATE_TYPE_INFO,
			SqlTimeTypeInfo.TIMESTAMP
		};

		verifyDataTypes(types);
	}

	private void verifyDataTypes(TypeInformation[] types) throws Exception {
		String[] colNames = new String[types.length];

		for (int i = 0; i < types.length; i++) {
			colNames[i] = types[i].toString().toLowerCase() + "_col";
		}

		CatalogTable table = new GenericCatalogTable(
			new TableSchema(colNames, types),
			getBatchTableProperties(),
			TEST_COMMENT
		);

		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, table, false);

		checkEquals(table, (CatalogTable) catalog.getTable(path1));
	}

	@Override
	public CatalogDatabase createDb() {
		return new GenericCatalogDatabase(
			new HashMap<String, String>() {{
				put("k1", "v1");
			}},
			TEST_COMMENT);
	}

	@Override
	public CatalogDatabase createAnotherDb() {
		return new GenericCatalogDatabase(
			new HashMap<String, String>() {{
				put("k2", "v2");
			}},
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createStreamingTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			getStreamingTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createPartitionedTable() {
		return new GenericCatalogTable(
			createTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherPartitionedTable() {
		return new GenericCatalogTable(
			createAnotherTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	@Override
	public CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createAnotherTableSchema(),
			new HashMap<>(),
			"This is another view");
	}
}
