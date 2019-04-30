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

import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTestBase;
import org.apache.flink.table.catalog.GenericCatalogDatabase;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

/**
 * Test for GenericHiveMetastoreCatalog.
 */
public class GenericHiveMetastoreCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() throws IOException {
		catalog = HiveTestUtils.createGenericHiveMetastoreCatalog();
		catalog.open();
	}

	// =====================
	// GenericHiveMetastoreCatalog doesn't support table operation yet
	// Thus, overriding the following tests which involve table operation in CatalogTestBase so they won't run against GenericHiveMetastoreCatalog
	// =====================

	// TODO: re-enable this test once GenericHiveMetastoreCatalog support table operations
	@Test
	public void testDropDb_DatabaseNotEmptyException() throws Exception {
	}

	// ------ utils ------

	@Override
	public String getBuiltInDefaultDatabase() {
		return GenericHiveMetastoreCatalog.DEFAULT_DB;
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
		// TODO: implement this once GenericHiveMetastoreCatalog support table operations
		return null;
	}

	@Override
	public CatalogTable createAnotherTable() {
		// TODO: implement this once GenericHiveMetastoreCatalog support table operations
		return null;
	}
}
